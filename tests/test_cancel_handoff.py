"""取消 / 发布 / 共享扫描收尾交接的专项测试。

覆盖任务书要求的关键交错：
* 成品改名前暂停发布、取消先受理：不得发布，报 cancelled 且暂存已清；
* 成功先提交：保留成功，取消返回“不可取消”说明，结果仍可下载；
* 发布/清理缓慢：取消及时返回 cancelling，查询与重复取消如实反映进度；
* 最后一个任务取消：报 cancelled 前本代扫描已停止、暂存目录已清空；
* 取消与扫描完成竞速：结果只能是 cancelled(无结果) 或 completed(有结果)。
"""
from __future__ import annotations

import pathlib
import threading
import time

import pytest
from helpers import HEADER, create_task, make_csv, upload, wait_terminal


def test_cancel_accepted_before_publish_never_publishes(client, monkeypatch):
    """扫描已完成、发布尚未进入时取消：取消先受理，绝不发布。"""
    from app.scan import Subscriber

    publish_entered = threading.Event()
    release_publish = threading.Event()
    real_publish = Subscriber._publish

    def blocking_publish(self):
        publish_entered.set()
        release_publish.wait(timeout=10)
        return real_publish(self)

    monkeypatch.setattr(Subscriber, "_publish", blocking_publish)

    sid = upload(client, make_csv(50)).json()["source_id"]
    tid = create_task(client, sid).json()["task_id"]
    assert publish_entered.wait(timeout=10), "扫描应已完成并进入发布"

    holder: dict = {}

    def do_cancel():
        holder["resp"] = client.post(f"/tasks/{tid}/cancel")

    t = threading.Thread(target=do_cancel)
    t.start()
    # 等取消先受理（状态变为 cancelling）后再放行发布
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if client.get(f"/tasks/{tid}").json()["status"] == "cancelling":
            break
        time.sleep(0.02)
    else:
        raise AssertionError("取消未被受理为 cancelling")
    release_publish.set()
    t.join(timeout=10)

    resp = holder["resp"].json()
    assert resp["status"] == "cancelled"
    assert client.get(f"/tasks/{tid}").json()["status"] == "cancelled"
    # 不发布：结果不可下载，成品与暂存都不存在
    assert client.get(f"/tasks/{tid}/result").status_code == 409
    from app import config as cfg

    assert not (cfg.RESULTS_DIR / f"{tid}.csv").exists()
    # 末任务取消：返回前本代扫描已停止、暂存目录已清空
    assert not list(cfg.SCANS_DIR.glob("**/*"))


def test_publish_committed_first_cancel_keeps_completed(client, monkeypatch):
    """成品改名前暂停发布时取消：成功先提交则保留成功并说明不可取消。"""
    real_replace = pathlib.Path.replace
    rename_entered = threading.Event()
    release_rename = threading.Event()

    def blocking_replace(self, target):
        if self.name.endswith(".staging.csv"):
            rename_entered.set()
            release_rename.wait(timeout=10)
        return real_replace(self, target)

    monkeypatch.setattr(pathlib.Path, "replace", blocking_replace)

    sid = upload(client, make_csv(50)).json()["source_id"]
    tid = create_task(client, sid).json()["task_id"]
    assert rename_entered.wait(timeout=10), "发布应已进入成品改名"

    holder: dict = {}

    def do_cancel():
        holder["resp"] = client.post(f"/tasks/{tid}/cancel")

    t = threading.Thread(target=do_cancel)
    t.start()
    time.sleep(0.3)  # 取消在改名暂停期间到达
    # 取消不得插队进入发布：状态不允许出现 cancelling
    assert client.get(f"/tasks/{tid}").json()["status"] != "cancelling"
    release_rename.set()
    t.join(timeout=10)

    resp = holder["resp"].json()
    assert resp["status"] == "completed"
    assert "不可取消" in resp.get("message", "")
    # 保留成功：状态与结果都可查可下
    assert client.get(f"/tasks/{tid}").json()["status"] == "completed"
    result = client.get(f"/tasks/{tid}/result")
    assert result.status_code == 200
    assert result.content.decode().splitlines()[0] == HEADER


def test_slow_cleanup_returns_cancelling_and_reflects_progress(client, monkeypatch):
    """清理缓慢：取消及时返回 cancelling，查询/重复取消如实反映进度。"""
    from app import config as cfg

    real_unlink = pathlib.Path.unlink

    def slow_unlink(self, *args, **kwargs):
        if self.name.endswith(".staging.csv"):
            time.sleep(3.0)  # 模拟缓慢清理
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "unlink", slow_unlink)

    sid = upload(client, make_csv(300), delay_ms=5).json()["source_id"]
    tid = create_task(client, sid).json()["task_id"]
    time.sleep(0.2)

    start = time.monotonic()
    resp = client.post(f"/tasks/{tid}/cancel").json()
    elapsed = time.monotonic() - start
    # 及时返回处理状态，不等待 3s 的缓慢清理
    assert resp["status"] == "cancelling"
    assert elapsed < cfg.CANCEL_WAIT_TIMEOUT_S + 2.0
    # 查询如实反映进度：暂存未清完前不是 cancelled
    assert client.get(f"/tasks/{tid}").json()["status"] == "cancelling"
    # 重复取消如实反映进度（清理完成后为 cancelled）
    again = client.post(f"/tasks/{tid}/cancel").json()
    assert again["status"] in ("cancelling", "cancelled")

    final = wait_terminal(client, tid, timeout=15)
    assert final["status"] == "cancelled"
    assert client.get(f"/tasks/{tid}/result").status_code == 409
    # 收尾完成后暂存与本代目录清空
    for _ in range(100):
        if not list(cfg.SCANS_DIR.glob("**/*")):
            break
        time.sleep(0.05)
    assert not list(cfg.SCANS_DIR.glob("**/*"))


def test_cancel_completed_task_keeps_result_and_explains(client):
    """成功先提交：取消保留成功并说明不可取消，结果仍可下载。"""
    sid = upload(client, make_csv(30)).json()["source_id"]
    tid = create_task(client, sid).json()["task_id"]
    assert wait_terminal(client, tid)["status"] == "completed"

    resp = client.post(f"/tasks/{tid}/cancel").json()
    assert resp["status"] == "completed"
    assert resp["result_available"] is True
    assert "不可取消" in resp.get("message", "")
    # 结果保留可下载，状态不受取消影响
    assert client.get(f"/tasks/{tid}/result").status_code == 200
    assert client.get(f"/tasks/{tid}").json()["status"] == "completed"


@pytest.mark.parametrize("round_no", range(6))
def test_cancel_at_scan_completion_is_always_consistent(client, round_no):
    """取消与扫描完成竞速：只能 cancelled(无结果) 或 completed(有结果)。

    不允许出现 cancelling 之后又 completed、或报 cancelled 却能下载结果。
    """
    sid = upload(client, make_csv(120), delay_ms=1).json()["source_id"]
    tid = create_task(client, sid).json()["task_id"]
    # 在扫描接近尾声时取消，覆盖取消/发布的竞态窗口
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        info = client.get(f"/tasks/{tid}").json()
        if info["scanned_rows"] >= 100 or info["status"] in ("completed", "failed"):
            break
        time.sleep(0.005)
    resp = client.post(f"/tasks/{tid}/cancel").json()
    final = wait_terminal(client, tid)

    assert final["status"] in ("cancelled", "completed")
    if final["status"] == "cancelled":
        assert client.get(f"/tasks/{tid}/result").status_code == 409
    else:
        assert client.get(f"/tasks/{tid}/result").status_code == 200
    # 取消响应与最终状态一致：报 cancelling 的最终必须落定 cancelled
    if resp["status"] == "cancelling":
        assert final["status"] == "cancelled"
    else:
        assert resp["status"] == final["status"]
