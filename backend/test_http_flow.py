"""通过真实 HTTP 服务验证冷存/预热动作的对外行为（前端只消费 ok/message）。

前置：uvicorn app.main:app --host 127.0.0.1 --port ${PORT:-8011} 已启动。
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request

BASE = f"http://127.0.0.1:{os.environ.get('PORT', '8011')}/api"
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


def action(module, entry_id, act):
    body = json.dumps({"action": act}).encode("utf-8")
    req = urllib.request.Request(
        f"{BASE}/{module}/{entry_id}/actions", data=body,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        return json.loads(resp.read().decode("utf-8"))


def get_entry(module, entry_id):
    with urllib.request.urlopen(f"{BASE}/{module}/{entry_id}") as resp:
        return json.loads(resp.read().decode("utf-8"))


def create_entry(module, values):
    body = json.dumps({"values": values}).encode("utf-8")
    req = urllib.request.Request(
        f"{BASE}/{module}", data=body,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    assert payload["ok"], payload
    return int(payload["entry"]["id"])


def create_cold(task_id, asset, target="S3-GLACIER"):
    return create_entry("cold_storage", {
        "任务编号": task_id, "源资产": asset, "目标存储": target,
        "压缩算法": "ZSTD", "迁移量": "1GB", "预计耗时": "5min",
        "迁移状态": "待调度", "校验摘要": "",
    })


def create_prefetch(task_id, asset):
    return create_entry("hot_prefetch", {
        "任务编号": task_id, "冷存位置": f"S3://glacier/{asset}",
        "目标分区": f"{asset}_hot", "回迁量": "1GB", "预计耗时": "5min",
        "优先级": "高", "过期策略": "T+30", "任务状态": "排队中",
    })


def safe(v):
    return "".join(c if c.isalnum() or c in "._-" else "_" for c in v)


print("=== HTTP 端到端：确认/发起/回迁的对外契约 ===")
tid, asset = "COLD-HTTP", "ASSE-HTTP"
prefetch_ids = ["HOT-HTTP", "HOT-HTTP2"]


def wipe_artifacts():
    for name in ("cold_migration_start", "cold_migration_verify",
                 "cold_completion_credential", "prefetch_read_grant"):
        p = os.path.join(DATA_DIR, f"{name}_{safe(tid)}.json")
        if os.path.exists(p):
            os.remove(p)
    for p in (os.path.join(DATA_DIR, f"cold_storage_{asset}.json"),
              *(os.path.join(DATA_DIR, f"hot_prefetch_{ptid}.json")
                for ptid in prefetch_ids)):
        if os.path.exists(p):
            os.remove(p)


wipe_artifacts()

cid = create_cold(tid, asset)
pid = create_prefetch("HOT-HTTP", asset)

try:
    # 1. 未发起直接确认 → ok=false，列表状态仍待调度
    r = action("cold_storage", cid, "确认完成")
    check("未发起确认返回 ok=false", r["ok"] is False and r["entry"] is None, r)
    listed = get_entry("cold_storage", cid)
    check("列表任务仍待调度，无伪造校验摘要",
          listed["status"] == "待调度" and not listed.get("校验摘要"))

    # 2. 迁移中回迁 → ok=false
    r = action("cold_storage", cid, "发起迁移")
    check("发起迁移 ok=true", r["ok"] and r["entry"]["status"] == "迁移中", r)
    r = action("hot_prefetch", pid, "发起回迁")
    check("凭据未齐时回迁 ok=false", r["ok"] is False, r)

    # 3. 完成迁移后回迁 → ok=true，资格落盘
    r = action("cold_storage", cid, "确认完成")
    check("确认完成 ok=true", r["ok"] and r["entry"]["status"] == "已完成", r)
    check("列表校验摘要是真实 SHA256", r["entry"]["校验摘要"].startswith("SHA256:"))

    # 4. 重复确认与迟到发起均 ok=false
    r1 = action("cold_storage", cid, "确认完成")
    r2 = action("cold_storage", cid, "发起迁移")
    check("重复确认 ok=false", r1["ok"] is False and "重复确认" in r1["message"], r1)
    check("迟到发起 ok=false", r2["ok"] is False and "迟到" in r2["message"], r2)

    # 5. 回迁启动成功，第二条回迁拿不到资格
    r = action("hot_prefetch", pid, "发起回迁")
    check("回迁启动 ok=true 并带资格信息",
          r["ok"] and r["entry"]["status"] == "回迁中" and r["entry"]["读取资格"], r)
    pid2 = create_prefetch("HOT-HTTP2", asset)
    r = action("hot_prefetch", pid2, "发起回迁")
    check("第二条回迁 ok=false", r["ok"] is False and "读取资格" in r["message"], r)

    # 6. 标记失败动作在已完成任务上被拒绝
    r = action("cold_storage", cid, "标记失败")
    check("已完成任务标记失败 ok=false", r["ok"] is False, r)
finally:
    wipe_artifacts()

print(f"\nHTTP 结果: {GREEN}{passed} 通过{RESET} / {RED}{failed} 失败{RESET}")
sys.exit(1 if failed else 0)
