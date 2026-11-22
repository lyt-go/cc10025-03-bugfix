"""hot_prefetch 领域服务：预热回迁管理，接收 tier_policy 和 cold_storage 上游事件。

读取资格闸门（start_prefetch）：
    1. 从回迁任务的冷存位置解析出数据来源（冷存任务的源资产）；
    2. 读取该冷存任务的冷存完成凭据 cold_completion_credential_*.json，
       凭据必须 valid 且任务编号、目标存储与凭据完全匹配；
    3. 读取凭据引用的校验结果 cold_migration_verify_*.json，
       确认与核验证据已收齐（verified=True），否则禁止读取冷存对象；
    4. 读取资格按冷存任务签发 prefetch_read_grant_*.json，
       同一冷存位置只能有一条回迁记录获得资格，重复申请直接拒绝；
    5. 取消任务会释放（revoke）其持有的读取资格。

跨模块数据流：
    tier_policy → hot_prefetch: COLD→HOT策略变更触发预热回迁
    cold_storage → hot_prefetch: 冷存完成凭据 + 校验结果是回迁读取的前置条件
    hot_prefetch → cost_tracker: 回迁任务更新成本追踪
"""
from __future__ import annotations

import json
import os
import re
import threading
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.cold_storage import ColdStorageService
from app.store import store

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "assets")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_name(value: str) -> str:
    # 仅中和路径分隔符等危险字符，保留中文等 Unicode 标识，兼容既有落盘命名
    return re.sub(r"[\\/\x00:]+", "_", str(value)).strip(" _") or "unknown"


class HotPrefetchService(BaseModuleService):
    MODULE = "hot_prefetch"
    ENTITY = "回迁任务"
    OPERATION_TERM = "回填"
    LIST_FIELDS = ["任务编号", "冷存位置", "目标分区", "回迁量", "预计耗时", "优先级", "过期策略", "任务状态"]
    REQUIRED_FIELDS = ["任务编号", "冷存位置", "目标分区"]
    STATUS_ORDER = ["排队中", "回迁中", "已完成", "已取消"]
    ACTION_RULES = {"发起回迁": "回迁中", "确认完成": "已完成", "取消任务": "已取消"}
    NEGATIVE_ACTIONS = ["取消任务"]

    def __init__(self):
        super().__init__()
        os.makedirs(DATA_DIR, exist_ok=True)
        self._lock = threading.Lock()

    # ==================== 动作入口 ====================

    def start_prefetch(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        cold_location = str(entry.get("冷存位置", ""))
        target_partition = str(entry.get("目标分区", ""))
        prefetch_task_id = str(entry.get("任务编号", ""))

        with self._lock:
            if entry.get("status") == "回迁中":
                return None, f"{self.ENTITY} {entry_id} 已在回迁中，不能重复发起"
            if entry.get("status") in ("已完成", "已取消"):
                return None, f"{self.ENTITY} {entry_id} 当前状态 {entry.get('status')}，不能发起回迁"

            # 读取资格闸门：凭据 + 校验证据 + 排他资格
            resolved = self._resolve_readable_cold_task(cold_location)
            if resolved is None:
                return None, (
                    f"{self.ENTITY} {entry_id} 不能读取冷存对象："
                    "冷存完成凭据或校验结果尚未收齐"
                )
            cold_task = resolved["cold_task"]
            credential = resolved["credential"]
            verify_record = resolved["verify_record"]
            cold_task_id = str(cold_task.get("任务编号", ""))

            grant = self.read_read_grant(cold_task_id)
            if grant is not None and grant.get("status") != "revoked":
                holder_live = any(
                    str(r.get("任务编号", "")) == str(grant.get("prefetch_task_id"))
                    and r.get("status") != "已取消"
                    for r in store.rows(self.MODULE)
                )
                if not holder_live:
                    # 资格持有者已不存在（如进程重启）：旧资格失效，可重新签发
                    grant["status"] = "revoked"
                    grant["revoked_at"] = _utc_now()
                    grant["revoke_reason"] = "原回迁任务已不存在，资格自动失效"
                    self._dump_json(self._grant_path(cold_task_id), grant)
                elif str(grant.get("prefetch_task_id")) != prefetch_task_id:
                    return None, (
                        f"{self.ENTITY} {entry_id} 不能读取冷存对象：读取资格已被回迁任务 "
                        f"{grant.get('prefetch_task_id')} 持有"
                    )
                elif grant.get("status") == "active":
                    return None, f"{self.ENTITY} {entry_id} 已持有读取资格，不能重复发起"
            if grant is None or grant.get("status") == "revoked":
                grant = self._issue_read_grant(
                    cold_task=cold_task,
                    credential=credential,
                    verify_record=verify_record,
                    prefetch_entry=entry,
                )
                self._dump_json(self._grant_path(cold_task_id), grant)

            entry["status"] = "回迁中"
            entry["pending"] = True
            entry["abnormal"] = False
            entry["任务状态"] = "回迁中"
            entry["读取资格"] = grant["grant_id"]
            entry["冷存凭据"] = credential["credential_id"]

            self._write_prefetch_record(cold_location, entry, grant)
            self._update_cost_for_prefetch(cold_location)

        return entry, (
            f"{self.ENTITY} {entry_id} 回迁中，{cold_location}→{target_partition}，"
            f"读取资格 {grant['grant_id']}"
        )

    def complete_prefetch(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        prefetch_task_id = str(entry.get("任务编号", ""))
        cold_location = str(entry.get("冷存位置", ""))

        with self._lock:
            if entry.get("status") != "回迁中":
                return None, (
                    f"{self.ENTITY} {entry_id} 当前状态 {entry.get('status')}，"
                    "只有回迁中的任务可以确认完成"
                )

            resolved = self._resolve_readable_cold_task(cold_location)
            if resolved is None:
                return None, (
                    f"{self.ENTITY} {entry_id} 失去冷存读取依据："
                    "完成凭据或校验结果缺失，不能确认完成"
                )
            cold_task_id = str(resolved["cold_task"].get("任务编号", ""))
            grant = self.read_read_grant(cold_task_id)
            if grant is None or str(grant.get("prefetch_task_id")) != prefetch_task_id \
                    or grant.get("status") not in ("active", "consumed"):
                return None, f"{self.ENTITY} {entry_id} 未持有有效读取资格，不能确认完成"

            entry["status"] = "已完成"
            entry["pending"] = False
            entry["abnormal"] = False
            entry["任务状态"] = "已完成"

            # 资格随回迁完成而消费，落盘保留，其他回迁记录仍无法再取得资格
            grant["status"] = "consumed"
            grant["consumed_at"] = _utc_now()
            self._dump_json(self._grant_path(cold_task_id), grant)

            self._write_prefetch_record(cold_location, entry, grant)

        return entry, f"{self.ENTITY} {entry_id} 已完成，数据已从冷存回迁到热层"

    def cancel_prefetch(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        prefetch_task_id = str(entry.get("任务编号", ""))
        cold_location = str(entry.get("冷存位置", ""))

        with self._lock:
            if entry.get("status") in ("已完成", "已取消"):
                return None, f"{self.ENTITY} {entry_id} 当前状态不能取消"

            grant: dict[str, Any] | None = None
            resolved = self._resolve_readable_cold_task(cold_location)
            if resolved is not None:
                cold_task_id = str(resolved["cold_task"].get("任务编号", ""))
                grant = self.read_read_grant(cold_task_id)
                if grant is not None and str(grant.get("prefetch_task_id")) == prefetch_task_id \
                        and grant.get("status") == "active":
                    grant["status"] = "revoked"
                    grant["revoked_at"] = _utc_now()
                    self._dump_json(self._grant_path(cold_task_id), grant)

            entry["status"] = "已取消"
            entry["pending"] = False
            entry["abnormal"] = True
            entry["任务状态"] = "已取消"
            self._write_prefetch_record(cold_location, entry, grant)

        return entry, f"{self.ENTITY} {entry_id} 已取消"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "发起回迁":
            return self.start_prefetch(entry_id)
        if action == "确认完成":
            return self.complete_prefetch(entry_id)
        if action == "取消任务":
            return self.cancel_prefetch(entry_id)
        return super().run_action(entry_id, action)

    # ==================== 读取资格解析 ====================

    @staticmethod
    def _asset_from_location(cold_location: str) -> str:
        """冷存位置形如 S3://glacier/{asset_id}，末段即冷存任务的源资产。"""
        return cold_location.rstrip("/").split("/")[-1].strip()

    def _resolve_readable_cold_task(
        self, cold_location: str
    ) -> dict[str, Any] | None:
        """按冷存位置找回冷存任务，并确认完成凭据与校验证据同时满足。"""
        asset_id = self._asset_from_location(cold_location)
        if not asset_id:
            return None

        cold_rows = store.rows("cold_storage")
        for cold_task in cold_rows:
            if str(cold_task.get("源资产", "")) != asset_id:
                continue
            if cold_task.get("status") != "已完成":
                # 未完成的目标不能被当成可恢复的数据
                continue
            task_id = str(cold_task.get("任务编号", ""))
            target_storage = str(cold_task.get("目标存储", ""))
            credential = ColdStorageService.read_completion_credential(task_id)
            if not ColdStorageService._credential_usable(
                credential, task_id, target_storage
            ):
                continue
            if int(credential.get("epoch", 0)) != int(cold_task.get("epoch", 0)):
                # 凭据属于已作废的旧迁移生命周期，不具备读取效力
                continue
            verify_record = ColdStorageService.read_verification_record(task_id)
            if not verify_record or not verify_record.get("verified"):
                continue
            if int(verify_record.get("epoch", 0)) != int(cold_task.get("epoch", 0)):
                continue
            return {
                "cold_task": cold_task,
                "credential": credential,
                "verify_record": verify_record,
            }
        return None

    def _issue_read_grant(
        self,
        cold_task: dict[str, Any],
        credential: dict[str, Any],
        verify_record: dict[str, Any],
        prefetch_entry: dict[str, Any],
    ) -> dict[str, Any]:
        cold_task_id = str(cold_task.get("任务编号", ""))
        prefetch_task_id = str(prefetch_entry.get("任务编号", ""))
        return {
            "grant_id": f"GRANT-{_safe_name(cold_task_id)}-{_safe_name(prefetch_task_id)}",
            "cold_task_id": cold_task_id,
            "prefetch_task_id": prefetch_task_id,
            "cold_location": str(prefetch_entry.get("冷存位置", "")),
            "target_partition": str(prefetch_entry.get("目标分区", "")),
            "credential_id": credential["credential_id"],
            "target_storage": credential["target_storage"],
            "verification_checksum": verify_record["actual_checksum"],
            "status": "active",
            "issued_at": _utc_now(),
        }

    @staticmethod
    def _grant_path(cold_task_id: str) -> str:
        return os.path.join(
            DATA_DIR, f"prefetch_read_grant_{_safe_name(cold_task_id)}.json"
        )

    @classmethod
    def read_read_grant(cls, cold_task_id: str) -> dict[str, Any] | None:
        path = cls._grant_path(cold_task_id)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # ==================== 文件 I/O ====================

    @staticmethod
    def _dump_json(path: str, payload: dict[str, Any]) -> None:
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        os.replace(tmp_path, path)

    def _write_prefetch_record(
        self,
        cold_location: str,
        entry: dict[str, Any],
        grant: dict[str, Any] | None,
    ) -> None:
        task_id = str(entry.get("任务编号", ""))
        filename = f"hot_prefetch_{_safe_name(task_id)}.json"
        path = os.path.join(DATA_DIR, filename)
        record = {
            "cold_location": cold_location,
            "task_id": task_id,
            "target_partition": str(entry.get("目标分区", "")),
            "volume": str(entry.get("回迁量", "")),
            "priority": str(entry.get("优先级", "")),
            "expiry_policy": str(entry.get("过期策略", "")),
            "status": str(entry.get("任务状态", "")),
            "read_grant": (grant or {}).get("grant_id", ""),
            "cold_credential": (grant or {}).get("credential_id", ""),
            "updated_at": _utc_now(),
        }
        self._dump_json(path, record)

    def _update_cost_for_prefetch(self, cold_location: str) -> None:
        cost_rows = store.rows("cost_tracker")
        for row in cost_rows:
            if cold_location.find(str(row.get("关联资产", ""))) >= 0:
                row["传输成本"] = "预热回迁中"
                row["status"] = "已核算"
                row["pending"] = False
                break