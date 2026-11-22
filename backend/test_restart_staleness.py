"""进程重启场景：内存仓库重置后，旧落盘凭据/资格不得被新生命周期信任。

模拟方式：同一进程内重建 store（等价于服务重启：内存清空、JSON 证据保留），
验证：
  1. 重启后冷存任务回到种子状态，旧凭据仍在磁盘但 epoch 对不上，不能用于确认；
  2. 预热回迁闸门拒绝读取旧冷存对象（未完成的目标不能被当成可恢复数据）；
  3. 重新走完发起→校验→完成后，新凭据生效，回迁才能拿到资格。
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from app import store as store_module
from app.services.cold_storage import ColdStorageService
from app.services.hot_prefetch import HotPrefetchService

DATA_DIR = os.path.join(os.path.dirname(__file__), "app", "data", "assets")
RED, GREEN, RESET = "\033[91m", "\033[92m", "\033[0m"
passed = failed = 0


def check(desc, condition, detail=""):
    global passed, failed
    if condition:
        print(f"  {GREEN}PASS{RESET} {desc}")
        passed += 1
    else:
        print(f"  {RED}FAIL{RESET} {desc} {detail}")
        failed += 1


def safe(v):
    return "".join(c if c.isalnum() or c in "._-" else "_" for c in v)


TID, ASSET = "COLD-RST", "ASSE-RST"
PID = "HOT-RST"
files = [
    os.path.join(DATA_DIR, f"cold_migration_start_{safe(TID)}.json"),
    os.path.join(DATA_DIR, f"cold_migration_verify_{safe(TID)}.json"),
    os.path.join(DATA_DIR, f"cold_completion_credential_{safe(TID)}.json"),
    os.path.join(DATA_DIR, f"prefetch_read_grant_{safe(TID)}.json"),
    os.path.join(DATA_DIR, f"cold_storage_{safe(ASSET)}.json"),
    os.path.join(DATA_DIR, f"hot_prefetch_{safe(PID)}.json"),
    os.path.join(DATA_DIR, f"hot_prefetch_{safe(PID)}B.json"),
]
for p in files:
    if os.path.exists(p):
        os.remove(p)

cold_svc = ColdStorageService()
hot_svc = HotPrefetchService()

# --- 第一个生命周期：完成一次迁移并回迁 ---
cold_id = cold_svc.create_entry({
    "任务编号": TID, "源资产": ASSET, "目标存储": "S3-GLACIER",
    "压缩算法": "ZSTD", "迁移量": "1GB", "预计耗时": "5min",
    "迁移状态": "待调度", "校验摘要": "",
})[0]["id"]
cold_svc.start_migration(cold_id)
cold_svc.complete_migration(cold_id)
cred_before = json.load(open(files[2], encoding="utf-8"))
check("重启前凭据有效", cred_before["status"] == "valid" and cred_before["epoch"] == 1)

hot_rows = store_module.store.rows("hot_prefetch")
hot_id = max((int(r.get("id", 0)) for r in hot_rows), default=0) + 1
hot_rows.append({
    "id": hot_id, "status": "排队中", "pending": True, "abnormal": False,
    "任务编号": PID, "冷存位置": f"S3://glacier/{ASSET}",
    "目标分区": f"{ASSET}_hot", "回迁量": "1GB", "预计耗时": "5min",
    "优先级": "高", "过期策略": "T+30", "任务状态": "排队中",
})
entry, _ = hot_svc.start_prefetch(hot_id)
check("重启前回迁可启动", entry is not None and entry["status"] == "回迁中")
grant_before = json.load(open(files[3], encoding="utf-8"))
check("重启前资格 active", grant_before["status"] == "active")

# --- 模拟进程重启：重建内存仓库（种子数据），落盘证据原样保留 ---
store_module.store = store_module.Store()
import app.services.base as base_mod
base_mod.store = store_module.store
import app.services.cold_storage as cs_mod
import app.services.hot_prefetch as hp_mod
cs_mod.store = store_module.store
hp_mod.store = store_module.store

# 重建服务实例（__init__ 不依赖旧内存）
cold_svc = ColdStorageService()
hot_svc = HotPrefetchService()

check("旧凭据文件仍在磁盘", os.path.exists(files[2]))
check("旧资格文件仍在磁盘", os.path.exists(files[3]))

# 种子里没有本任务，重新登记一个“重启后”的同编号任务（epoch 从 0 开始）
cold_id2 = cold_svc.create_entry({
    "任务编号": TID, "源资产": ASSET, "目标存储": "S3-GLACIER",
    "压缩算法": "ZSTD", "迁移量": "1GB", "预计耗时": "5min",
    "迁移状态": "待调度", "校验摘要": "",
})[0]["id"]
fresh = store_module.store.find("cold_storage", cold_id2)
check("重启后任务回到待调度且无 epoch", fresh["status"] == "待调度"
      and int(fresh.get("epoch", 0)) == 0)

# 旧凭据 epoch=1 与新生命周期 epoch=0 不匹配 → 不能凭旧证据确认
entry, msg = cold_svc.complete_migration(cold_id2)
check("重启后不能凭旧凭据确认完成", entry is None, msg)
check("任务仍待调度", fresh["status"] == "待调度")

# 重启后回迁闸门：内存任务未完成 → 拒绝读取冷存对象
hot_rows2 = store_module.store.rows("hot_prefetch")
hot_id2 = max((int(r.get("id", 0)) for r in hot_rows2), default=0) + 1
hot_rows2.append({
    "id": hot_id2, "status": "排队中", "pending": True, "abnormal": False,
    "任务编号": PID + "B", "冷存位置": f"S3://glacier/{ASSET}",
    "目标分区": f"{ASSET}_hot2", "回迁量": "1GB", "预计耗时": "5min",
    "优先级": "高", "过期策略": "T+30", "任务状态": "排队中",
})
entry, msg = hot_svc.start_prefetch(hot_id2)
check("重启后回迁被闸门拒绝", entry is None and "凭据" in msg, msg)

# 重新走完整生命周期
cold_svc.start_migration(cold_id2)
check("重启后发起进入 epoch 1", int(fresh["epoch"]) == 1)
entry, msg = cold_svc.complete_migration(cold_id2)
check("重新迁移后确认完成", entry is not None and entry["status"] == "已完成", msg)
cred_after = json.load(open(files[2], encoding="utf-8"))
check("新凭据 valid 且 epoch 匹配",
      cred_after["status"] == "valid" and cred_after["epoch"] == fresh["epoch"])

entry, msg = hot_svc.start_prefetch(hot_id2)
check("新凭据满足后回迁启动", entry is not None and entry["status"] == "回迁中", msg)
grant_after = json.load(open(files[3], encoding="utf-8"))
check("资格重新签发给新回迁任务",
      grant_after["prefetch_task_id"] == PID + "B" and grant_after["status"] == "active")

for p in files:
    if os.path.exists(p):
        os.remove(p)

print(f"\n重启场景结果: {GREEN}{passed} 通过{RESET} / {RED}{failed} 失败{RESET}")
sys.exit(1 if failed else 0)
