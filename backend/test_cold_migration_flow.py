"""冷存迁移状态流转与预热回迁读取资格专项测试。

覆盖需求：
  1. 先确认后发起：未发起迁移时确认完成必须被拒绝，不能写出“校验通过”记录；
  2. 发起后多次确认与迟到发起：状态只能 迁移中→已完成 一次，凭据唯一且幂等；
  3. 预热回迁闸门：凭据与校验结果未收齐时不能读取冷存对象，
     同一冷存位置只能有一条回迁记录获得读取资格；
  4. 未完成（迁移中/已失败）的冷存目标不能被当成可恢复数据；
  5. 篡改发起数据记录后校验必须失败，不签发凭据；
  6. 切换目标存储后旧凭据作废，重新校验后只保留一份可用凭据；
  7. 重复确认与迟到发起并发到达时不产生两份可用完成结果。
"""
from __future__ import annotations

import json
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(__file__))

from app.services.cold_storage import ColdStorageService
from app.services.hot_prefetch import HotPrefetchService
from app.store import store

DATA_DIR = os.path.join(os.path.dirname(__file__), "app", "data", "assets")
RED = "\033[91m"
GREEN = "\033[92m"
RESET = "\033[0m"

passed = 0
failed = 0


def check(desc: str, condition: bool, detail: str = "") -> bool:
    global passed, failed
    if condition:
        print(f"  {GREEN}PASS{RESET} {desc}")
        passed += 1
        return True
    print(f"  {RED}FAIL{RESET} {desc} {detail}")
    failed += 1
    return False


cold_svc = ColdStorageService()
hot_svc = HotPrefetchService()


def _path(prefix: str, key: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in key)
    return os.path.join(DATA_DIR, f"{prefix}_{safe}.json")


def start_path(task_id: str) -> str:
    return _path("cold_migration_start", task_id)


def verify_path(task_id: str) -> str:
    return _path("cold_migration_verify", task_id)


def credential_path(task_id: str) -> str:
    return _path("cold_completion_credential", task_id)


def grant_path(task_id: str) -> str:
    return _path("prefetch_read_grant", task_id)


def load(path: str):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def cleanup(task_ids: list[str], asset_ids: list[str] | None = None,
            prefetch_ids: list[str] | None = None) -> None:
    for tid in task_ids:
        for p in (start_path(tid), verify_path(tid), credential_path(tid), grant_path(tid)):
            if os.path.exists(p):
                os.remove(p)
    for asset in asset_ids or []:
        p = os.path.join(DATA_DIR, f"cold_storage_{asset}.json")
        if os.path.exists(p):
            os.remove(p)
    for ptid in prefetch_ids or []:
        p = os.path.join(DATA_DIR, f"hot_prefetch_{ptid}.json")
        if os.path.exists(p):
            os.remove(p)


def create_cold(task_id: str, asset: str, target: str = "S3-GLACIER") -> int:
    entry, missing = cold_svc.create_entry({
        "任务编号": task_id,
        "源资产": asset,
        "目标存储": target,
        "压缩算法": "ZSTD",
        "迁移量": "10GB",
        "预计耗时": "30min",
        "迁移状态": "待调度",
        "校验摘要": "",
    })
    assert entry is not None, missing
    return int(entry["id"])


def create_prefetch(task_id: str, asset: str, partition: str) -> int:
    rows = store.rows("hot_prefetch")
    next_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    rows.append({
        "id": next_id,
        "status": "排队中",
        "pending": True,
        "abnormal": False,
        "任务编号": task_id,
        "冷存位置": f"S3://glacier/{asset}",
        "目标分区": partition,
        "回迁量": "10GB",
        "预计耗时": "15min",
        "优先级": "高",
        "过期策略": "T+30",
        "任务状态": "排队中",
    })
    return next_id


def prefetch_seed_count(asset: str) -> int:
    return sum(
        1 for r in store.rows("hot_prefetch")
        if str(r.get("冷存位置", "")).find(asset) >= 0
    )


# ---------------------------------------------------------------------------

def test_confirm_before_start():
    print("\n=== 场景1: 同一任务先确认后发起 ===")
    tid, asset = "COLD-TA", "ASSE-TA"
    cleanup([tid], [asset])
    cid = create_cold(tid, asset)

    # 1.1 未发起直接确认完成 → 拒绝，且不产生任何证据产物
    entry, msg = cold_svc.complete_migration(cid)
    check("未发起迁移时确认完成被拒绝", entry is None, msg)
    check("任务保持待调度", store.find("cold_storage", cid)["status"] == "待调度")
    check("无发起数据记录", not os.path.exists(start_path(tid)))
    check("无校验结果文件", not os.path.exists(verify_path(tid)))
    check("无完成凭据", not os.path.exists(credential_path(tid)))
    check("拒绝信息指向缺少发起记录", "发起" in msg)

    # 1.2 发起迁移 → 迁移中，落盘发起数据记录
    entry, msg = cold_svc.start_migration(cid)
    check("发起迁移成功", entry is not None and entry["status"] == "迁移中", msg)
    start = load(start_path(tid))
    check("发起数据记录已写入", start is not None and start["epoch"] == 1)
    check("发起记录含校验基线", bool(start["manifest"]["objects"][0]["checksum"]))

    # 1.3 确认完成 → 校验通过并签发凭据
    entry, msg = cold_svc.complete_migration(cid)
    check("确认完成成功", entry is not None and entry["status"] == "已完成", msg)
    verify = load(verify_path(tid))
    check("校验结果文件存在且 verified=true", verify is not None and verify["verified"] is True)
    cred = load(credential_path(tid))
    check("完成凭据已签发且 valid", cred is not None and cred["status"] == "valid")
    check("凭据绑定任务编号", cred["task_id"] == tid)
    check("凭据绑定目标存储", cred["target_storage"] == "S3-GLACIER")
    check("凭据引用实际校验值", cred["verification"]["checksum"] == verify["actual_checksum"])
    check("列表校验摘要来自真实校验", entry["校验摘要"].startswith("SHA256:"))


def test_repeated_confirm_and_late_start():
    print("\n=== 场景2: 发起后多次确认 + 迟到的发起请求 ===")
    tid, asset = "COLD-TB", "ASSE-TB"
    cleanup([tid], [asset])
    cid = create_cold(tid, asset)
    seeds_before = prefetch_seed_count(asset)

    cold_svc.start_migration(cid)

    # 2.1 连续三次确认：第一次成功，后两次幂等拒绝
    ok_msgs, fail_msgs = [], []
    for _ in range(3):
        entry, msg = cold_svc.complete_migration(cid)
        (ok_msgs if entry is not None else fail_msgs).append(msg)
    check("三次确认仅一次成功", len(ok_msgs) == 1 and len(fail_msgs) == 2, str(fail_msgs))
    check("重复确认被明确拒绝", all("重复确认" in m for m in fail_msgs))

    cred = load(credential_path(tid))
    check("磁盘上只有一份完成凭据", cred is not None and cred["status"] == "valid")
    check("凭据代数为 1", cred["epoch"] == 1)
    check("预热种子只创建一次", prefetch_seed_count(asset) == seeds_before + 1)

    # 2.2 完成后迟到的发起请求 → 拒绝，状态与凭据均不受影响
    entry, msg = cold_svc.start_migration(cid)
    check("迟到发起被拒绝", entry is None and "迟到" in msg, msg)
    check("状态保持已完成", store.find("cold_storage", cid)["status"] == "已完成")
    check("凭据仍然有效", load(credential_path(tid))["status"] == "valid")

    # 2.3 再次确认仍被拒绝
    entry, msg = cold_svc.complete_migration(cid)
    check("完成后再次确认仍被拒绝", entry is None and "重复确认" in msg, msg)


def test_prefetch_gate_and_grant_exclusivity():
    print("\n=== 场景3: 预热回迁读取资格闸门与排他性 ===")
    tid, asset = "COLD-TC", "ASSE-TC"
    cleanup([tid], [asset], ["HOT-TC1", "HOT-TC2"])
    cid = create_cold(tid, asset)
    p1 = create_prefetch("HOT-TC1", asset, f"{asset}_hot_a")
    p2 = create_prefetch("HOT-TC2", asset, f"{asset}_hot_b")

    # 3.1 冷存尚未发起 → 回迁被拒
    entry, msg = hot_svc.start_prefetch(p1)
    check("无凭据时回迁被拒绝", entry is None, msg)
    check("回迁任务保持排队中", store.find("hot_prefetch", p1)["status"] == "排队中")
    check("未签发读取资格", not os.path.exists(grant_path(tid)))

    # 3.2 冷存迁移中（凭据未收齐）→ 回迁仍被拒
    cold_svc.start_migration(cid)
    entry, msg = hot_svc.start_prefetch(p1)
    check("迁移中回迁被拒绝", entry is None and "凭据" in msg, msg)

    # 3.3 冷存完成 → 第一条回迁记录获得唯一资格
    cold_svc.complete_migration(cid)
    entry, msg = hot_svc.start_prefetch(p1)
    check("凭据收齐后回迁启动成功", entry is not None and entry["status"] == "回迁中", msg)
    grant = load(grant_path(tid))
    check("读取资格已签发且 active", grant is not None and grant["status"] == "active")
    check("资格绑定冷存任务", grant["cold_task_id"] == tid)
    check("资格绑定回迁任务", grant["prefetch_task_id"] == "HOT-TC1")
    check("资格记录冷存凭据编号", bool(grant["credential_id"]))

    # 3.4 第二条回迁记录无法再取得同一冷存位置的资格
    entry, msg = hot_svc.start_prefetch(p2)
    check("第二条回迁记录被拒绝", entry is None and "读取资格" in msg, msg)
    check("第二条保持排队中", store.find("hot_prefetch", p2)["status"] == "排队中")

    # 3.5 第一条重复发起也被拒绝
    entry, msg = hot_svc.start_prefetch(p1)
    check("回迁中重复发起被拒绝", entry is None, msg)

    # 3.6 第一条完成后资格被消费，第二条依旧无法读取
    entry, msg = hot_svc.complete_prefetch(p1)
    check("第一条回迁确认完成", entry is not None and entry["status"] == "已完成", msg)
    check("资格变为 consumed", load(grant_path(tid))["status"] == "consumed")
    entry, msg = hot_svc.start_prefetch(p2)
    check("资格消费后第二条仍被拒绝", entry is None, msg)

    # 3.7 取消释放资格后，其他回迁记录才能取得资格
    tid_d, asset_d = "COLD-TD", "ASSE-TD"
    cleanup([tid_d], [asset_d], ["HOT-TD1", "HOT-TD2"])
    cd = create_cold(tid_d, asset_d)
    px = create_prefetch("HOT-TD1", asset_d, f"{asset_d}_a")
    py = create_prefetch("HOT-TD2", asset_d, f"{asset_d}_b")
    cold_svc.start_migration(cd)
    cold_svc.complete_migration(cd)
    hot_svc.start_prefetch(px)
    check("PX 持有资格", load(grant_path(tid_d))["prefetch_task_id"] == "HOT-TD1")
    entry, msg = hot_svc.cancel_prefetch(px)
    check("PX 取消成功", entry is not None and entry["status"] == "已取消", msg)
    check("取消后资格被释放", load(grant_path(tid_d))["status"] == "revoked")
    entry, msg = hot_svc.start_prefetch(py)
    check("释放后 PY 可取得资格", entry is not None and entry["status"] == "回迁中", msg)
    check("资格转授 PY", load(grant_path(tid_d))["prefetch_task_id"] == "HOT-TD2")


def test_unfinished_targets_not_recoverable():
    print("\n=== 场景4: 未完成的冷存目标不能被当成可恢复数据 ===")
    # 迁移中
    tid_e, asset_e = "COLD-TE", "ASSE-TE"
    cleanup([tid_e], [asset_e], ["HOT-TE1"])
    ce = create_cold(tid_e, asset_e)
    pe = create_prefetch("HOT-TE1", asset_e, f"{asset_e}_hot")
    cold_svc.start_migration(ce)
    entry, msg = hot_svc.start_prefetch(pe)
    check("迁移中目标不可回迁", entry is None, msg)
    check("迁移中目标无资格文件", not os.path.exists(grant_path(tid_e)))

    # 已失败
    tid_f, asset_f = "COLD-TF", "ASSE-TF"
    cleanup([tid_f], [asset_f], ["HOT-TF1"])
    cf = create_cold(tid_f, asset_f)
    pf = create_prefetch("HOT-TF1", asset_f, f"{asset_f}_hot")
    cold_svc.start_migration(cf)
    entry, msg = cold_svc.mark_failed(cf)
    check("冷存任务可标记失败", entry is not None and entry["status"] == "已失败", msg)
    entry, msg = hot_svc.start_prefetch(pf)
    check("已失败目标不可回迁", entry is None, msg)
    check("失败目标无凭据", not os.path.exists(credential_path(tid_f)))
    check("失败目标无资格文件", not os.path.exists(grant_path(tid_f)))

    # 待调度
    tid_g, asset_g = "COLD-TG0", "ASSE-TG0"
    cleanup([tid_g], [asset_g], ["HOT-TG0"])
    cg = create_cold(tid_g, asset_g)
    pg = create_prefetch("HOT-TG0", asset_g, f"{asset_g}_hot")
    entry, msg = hot_svc.start_prefetch(pg)
    check("待调度目标不可回迁", entry is None, msg)


def test_tampered_initiation_fails_verification():
    print("\n=== 场景5: 发起数据记录被篡改 → 校验失败，不签发凭据 ===")
    tid, asset = "COLD-TG", "ASSE-TG"
    cleanup([tid], [asset])
    cid = create_cold(tid, asset)
    cold_svc.start_migration(cid)

    # 模拟冷存对象被破坏：篡改发起记录中的对象校验基线
    start = load(start_path(tid))
    original = start["manifest"]["objects"][0]["checksum"]
    start["manifest"]["objects"][0]["checksum"] = "0" * 64
    with open(start_path(tid), "w", encoding="utf-8") as f:
        json.dump(start, f, ensure_ascii=False, indent=2)

    entry, msg = cold_svc.complete_migration(cid)
    check("篡改后确认完成被拒绝", entry is None, msg)
    verify = load(verify_path(tid))
    check("校验结果落盘且 verified=false", verify is not None and verify["verified"] is False)
    check("校验结果保留期望/实际值", verify["objects"][0]["expected_checksum"] == "0" * 64
          and verify["objects"][0]["match"] is False)
    check("不签发完成凭据", not os.path.exists(credential_path(tid)))
    check("任务保留迁移中状态", store.find("cold_storage", cid)["status"] == "迁移中")
    check("任务被标记异常", store.find("cold_storage", cid)["abnormal"] is True)

    # 恢复基线后重新确认 → 校验通过，凭据签发
    start["manifest"]["objects"][0]["checksum"] = original
    start["manifest"]["manifest_checksum"] = cold_svc._manifest_checksum(start["manifest"]["objects"])
    with open(start_path(tid), "w", encoding="utf-8") as f:
        json.dump(start, f, ensure_ascii=False, indent=2)
    entry, msg = cold_svc.complete_migration(cid)
    check("恢复后确认完成成功", entry is not None and entry["status"] == "已完成", msg)
    check("恢复后凭据签发", load(credential_path(tid)) is not None)


def test_target_switch_supersedes_credential():
    print("\n=== 场景6: 切换目标存储 → 旧凭据作废，重新校验后仅一份可用凭据 ===")
    tid, asset = "COLD-TH", "ASSE-TH"
    cleanup([tid], [asset], ["HOT-TH1"])
    cid = create_cold(tid, asset, target="S3-GLACIER")
    cold_svc.start_migration(cid)
    cold_svc.complete_migration(cid)
    first = load(credential_path(tid))
    check("首代凭据有效", first["status"] == "valid" and first["epoch"] == 1)

    # 切换目标存储并重新发起
    entry_row = store.find("cold_storage", cid)
    entry_row["目标存储"] = "S3-DEEP-ARCHIVE"
    entry, msg = cold_svc.start_migration(cid)
    check("切换目标后可重新发起", entry is not None and entry["status"] == "迁移中", msg)
    check("任务进入第二代", entry["epoch"] == 2)
    old = load(credential_path(tid))
    check("旧凭据被标记 superseded", old["status"] == "superseded")
    check("作废原因记录目标切换", "切换" in old["supersede_reason"])

    # 旧凭据未被新证据替换前，回迁闸门仍然关闭
    ph = create_prefetch("HOT-TH1", asset, f"{asset}_hot")
    entry, msg = hot_svc.start_prefetch(ph)
    check("重新迁移期间回迁被拒绝", entry is None, msg)

    # 重新校验完成 → 新凭据，旧凭据仍作废，可用凭据只有一份
    entry, msg = cold_svc.complete_migration(cid)
    check("第二代确认完成成功", entry is not None and entry["status"] == "已完成", msg)
    new = load(credential_path(tid))
    check("新凭据 valid 且绑定新目标",
          new["status"] == "valid" and new["target_storage"] == "S3-DEEP-ARCHIVE"
          and new["epoch"] == 2)
    check("磁盘上有效凭据只有一份", old["status"] == "superseded" and new["status"] == "valid")
    verify = load(verify_path(tid))
    check("校验结果属于第二代", verify["epoch"] == 2 and verify["verified"] is True)

    # 新凭据满足后回迁可启动
    entry, msg = hot_svc.start_prefetch(ph)
    check("新凭据满足后回迁启动", entry is not None and entry["status"] == "回迁中", msg)
    check("资格引用新凭据", load(grant_path(tid))["credential_id"] == new["credential_id"])


def test_concurrent_confirm_and_late_start():
    print("\n=== 场景7: 重复确认与迟到发起并发到达 ===")
    tid, asset = "COLD-TI", "ASSE-TI"
    cleanup([tid], [asset])
    cid = create_cold(tid, asset)
    cold_svc.start_migration(cid)

    results: list[tuple[str, object]] = []

    def do_confirm():
        results.append(("confirm",) + cold_svc.complete_migration(cid))

    def do_start():
        results.append(("start",) + cold_svc.start_migration(cid))

    threads = [threading.Thread(target=do_confirm) for _ in range(4)]
    threads += [threading.Thread(target=do_start) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    confirms_ok = [r for r in results if r[0] == "confirm" and r[1] is not None]
    starts_ok = [r for r in results if r[0] == "start" and r[1] is not None]
    check("并发下仅一次确认生效", len(confirms_ok) == 1, f"成功确认 {len(confirms_ok)} 次")
    check("迟到发起无一生效", len(starts_ok) == 0, f"成功发起 {len(starts_ok)} 次")
    check("最终状态为已完成", store.find("cold_storage", cid)["status"] == "已完成")
    cred = load(credential_path(tid))
    check("仅一份有效凭据", cred is not None and cred["status"] == "valid")


if __name__ == "__main__":
    import atexit

    def _sweep_test_artifacts() -> None:
        """兜底回收：任何场景中途失败也不留下测试产物。"""
        markers = ("COLD-T", "ASSE-T", "HOT-T")
        for name in os.listdir(DATA_DIR):
            if name.endswith(".json") and any(m in name for m in markers):
                os.remove(os.path.join(DATA_DIR, name))

    atexit.register(_sweep_test_artifacts)

    print("=" * 60)
    print("冷存迁移状态流转 × 预热回迁读取资格 专项测试")
    print("=" * 60)

    test_confirm_before_start()
    test_repeated_confirm_and_late_start()
    test_prefetch_gate_and_grant_exclusivity()
    test_unfinished_targets_not_recoverable()
    test_tampered_initiation_fails_verification()
    test_target_switch_supersedes_credential()
    test_concurrent_confirm_and_late_start()

    print(f"\n{'=' * 60}")
    print(f"结果: {GREEN}{passed} 通过{RESET} / {RED}{failed} 失败{RESET} / "
          f"{passed + failed} 总计")
    print("=" * 60)
    sys.exit(1 if failed else 0)
