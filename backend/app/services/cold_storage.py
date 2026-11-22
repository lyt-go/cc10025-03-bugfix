"""cold_storage 领域服务：冷存储迁移管理，接收 tier_policy 上游事件触发冷存任务。

状态流转（所有流转以落盘证据为准，内存中的 entry 仅用于列表展示）：

    待调度 --发起迁移--> 迁移中 --确认完成--> 已完成
                          \
                           --标记失败--> 已失败

落盘产物（DATA_DIR 下，全部按任务编号隔离）：
    cold_migration_start_{task}.json       发起数据记录（迁移清单 + 校验基线）
    cold_migration_verify_{task}.json      校验结果（实际重算的校验值与比对结论）
    cold_completion_credential_{task}.json 冷存完成凭据（绑定任务与目标存储）
    prefetch_read_grant_{task}.json        预热回迁读取资格（由 hot_prefetch 签发）

关键约束：
    * 未发起迁移（无发起数据记录）的「确认完成」一律拒绝，绝不写出“校验通过”记录；
    * 「确认完成」必须读取发起数据记录并执行实际校验，校验通过且证据落盘后才签发凭据；
    * 同一任务只保留一份有效凭据，重复确认幂等拒绝，不重复触发预热种子；
    * 任务完成后到达的迟到发起请求被拒绝，不回滚状态、不作废凭据；
    * 切换目标存储后旧凭据标记 superseded，须重新发起、重新校验才能取得新凭据。

跨模块数据流：
    tier_policy → cold_storage: 分层策略生效触发冷存储迁移
    cold_storage → hot_prefetch: 冷存完成凭据是预热回迁读取冷存对象的唯一凭证
    cold_storage → cost_tracker: 迁移任务更新成本追踪
"""
from __future__ import annotations

import json
import os
import re
import threading
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "assets")
_CHECKSUM_ALGORITHM = "SHA256"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_name(value: str) -> str:
    # 仅中和路径分隔符等危险字符，保留中文等 Unicode 标识，兼容既有落盘命名
    return re.sub(r"[\\/\x00:]+", "_", str(value)).strip(" _") or "unknown"


class ColdStorageService(BaseModuleService):
    MODULE = "cold_storage"
    ENTITY = "冷存任务"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = ["任务编号", "源资产", "目标存储", "压缩算法", "迁移量", "预计耗时", "迁移状态", "校验摘要"]
    REQUIRED_FIELDS = ["任务编号", "源资产", "目标存储"]
    STATUS_ORDER = ["待调度", "迁移中", "已完成", "已失败"]
    ACTION_RULES = {"发起迁移": "迁移中", "确认完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    def __init__(self):
        super().__init__()
        os.makedirs(DATA_DIR, exist_ok=True)
        # 同一任务的发起/确认可能并发到达，状态流转整体加锁保证顺序冲突可判定
        self._lock = threading.Lock()

    # ==================== 动作入口 ====================

    def start_migration(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """发起冷存储迁移：写发起数据记录并更新成本追踪。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        task_id = str(entry.get("任务编号", ""))
        asset_id = str(entry.get("源资产", ""))
        target_storage = str(entry.get("目标存储", ""))

        with self._lock:
            credential = self.read_completion_credential(task_id)
            if entry.get("status") == "已完成" and self._credential_usable(
                credential, task_id, target_storage
            ):
                # 迟到的发起请求：迁移已凭有效凭据完成，不得回滚状态或作废凭据
                return None, (
                    f"{self.ENTITY} {entry_id} 已完成并持有有效冷存完成凭据，"
                    "迟到的发起请求已拒绝"
                )
            if entry.get("status") == "迁移中":
                # 重复发起：已有在途迁移，拒绝并保留原发起数据记录
                return None, f"{self.ENTITY} {entry_id} 已在迁移中，不能重复发起"
            if entry.get("status") == "已失败":
                return None, f"{self.ENTITY} {entry_id} 已标记失败，不能重新发起"

            # 内存中的任务进入新一轮迁移生命周期（进程重启后旧落盘凭据自动失效）
            epoch = int(entry.get("epoch", 0)) + 1
            entry["epoch"] = epoch

            if credential and credential.get("status") == "valid":
                # 目标切换或新一轮发起：作废旧一代凭据，新目标需重新校验
                reason = (
                    f"目标存储切换为 {target_storage}"
                    if credential.get("target_storage") != target_storage
                    else "新一轮迁移发起，旧凭据作废"
                )
                credential["status"] = "superseded"
                credential["superseded_at"] = _utc_now()
                credential["supersede_reason"] = reason
                self._dump_json(self._credential_path(task_id), credential)

            entry["status"] = "迁移中"
            entry["pending"] = True
            entry["abnormal"] = False
            entry["迁移状态"] = "迁移中"

            start_record = self._build_initiation_record(entry, epoch)
            self._dump_json(self._start_path(task_id), start_record)
            # 状态记录只反映真实状态，此时没有任何校验结论
            self._write_cold_storage_record(asset_id, entry, checksum="")

            self._update_cost_tracker(asset_id, target_storage)
            self._write_audit_entry(
                entity="冷存任务",
                action="发起迁移",
                target_id=task_id,
                detail=f"冷存迁移 {asset_id} → {target_storage}（第 {epoch} 代发起）",
            )

        return entry, f"{self.ENTITY} {entry_id} 迁移中，{asset_id}→{target_storage}"

    def complete_migration(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """确认迁移完成：读取发起数据记录 → 执行校验 → 签发冷存完成凭据。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        task_id = str(entry.get("任务编号", ""))
        asset_id = str(entry.get("源资产", ""))
        target_storage = str(entry.get("目标存储", ""))

        with self._lock:
            current_epoch = int(entry.get("epoch", 0))
            credential = self.read_completion_credential(task_id)
            if self._credential_usable(
                credential, task_id, target_storage
            ) and int(credential.get("epoch", 0)) == current_epoch:
                # 重复确认：幂等拒绝，不产生第二份完成结果，也不重复触发预热
                return None, (
                    f"{self.ENTITY} {entry_id} 已持有有效完成凭据 {credential['credential_id']}，"
                    "重复确认已拒绝"
                )

            if entry.get("status") != "迁移中":
                # 待调度（未发起）、已完成（仅旧凭据）、已失败 一律不允许进入完成流转
                if entry.get("status") == "待调度":
                    return None, (
                        f"{self.ENTITY} {entry_id} 尚未发起迁移，缺少发起数据记录，"
                        "不能确认完成"
                    )
                return None, (
                    f"{self.ENTITY} {entry_id} 当前状态 {entry.get('status')}，"
                    "只有迁移中的任务可以确认完成"
                )

            start_record = self.read_initiation_record(task_id)
            if start_record is None or int(start_record.get("epoch", 0)) != current_epoch:
                # 先确认后发起，或落盘记录属于已作废的旧生命周期：禁止凭旧证据完成
                return None, (
                    f"{self.ENTITY} {entry_id} 缺少当前迁移的发起数据记录，"
                    "不能确认完成"
                )
            if str(start_record.get("target_storage", "")) != target_storage:
                return None, (
                    f"{self.ENTITY} {entry_id} 目标存储已切换为 {target_storage}，"
                    "原发起数据记录失效，请重新发起迁移"
                )

            # 执行实际校验并落盘校验结果（无论通过与否都留证据）
            verify_record = self._run_verification(start_record)
            self._dump_json(self._verify_path(task_id), verify_record)

            if not verify_record["verified"]:
                entry["abnormal"] = True
                self._write_audit_entry(
                    entity="冷存任务",
                    action="校验失败",
                    target_id=task_id,
                    detail=(
                        f"冷存校验未通过：期望 {verify_record['expected_checksum'][:16]}…，"
                        f"实际 {verify_record['actual_checksum'][:16]}…"
                    ),
                )
                return None, (
                    f"{self.ENTITY} {entry_id} 冷存校验未通过，"
                    "保留迁移中状态，不签发完成凭据"
                )

            # 校验证据收齐，签发与任务、目标绑定的完成凭据
            credential = self._issue_credential(entry, start_record, verify_record)
            self._dump_json(self._credential_path(task_id), credential)

            entry["status"] = "已完成"
            entry["pending"] = False
            entry["abnormal"] = False
            entry["迁移状态"] = "已完成"
            entry["校验摘要"] = f"{_CHECKSUM_ALGORITHM}:{verify_record['actual_checksum']}"

            self._write_cold_storage_record(
                asset_id,
                entry,
                checksum=verify_record["actual_checksum"],
                credential_id=credential["credential_id"],
            )
            self._write_audit_entry(
                entity="冷存任务",
                action="确认完成",
                target_id=task_id,
                detail=(
                    f"冷存迁移完成并签发凭据 {credential['credential_id']}，"
                    f"校验值 {verify_record['actual_checksum'][:16]}…"
                ),
            )

            # 仅在真实完成的这一次流转中检查预热回迁需求
            self._check_prefetch_need(asset_id)

        return entry, f"{self.ENTITY} {entry_id} 已完成，完成凭据 {credential['credential_id']} 已签发"

    def mark_failed(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """标记失败：仅允许迁移链路中的任务失败，已完成凭据受保护。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        task_id = str(entry.get("任务编号", ""))
        with self._lock:
            credential = self.read_completion_credential(task_id)
            current_epoch = int(entry.get("epoch", 0))
            if self._credential_usable(
                credential, task_id, str(entry.get("目标存储", ""))
            ) and int(credential.get("epoch", 0)) == current_epoch:
                return None, f"{self.ENTITY} {entry_id} 持有有效完成凭据，不能标记失败"
            if entry.get("status") not in ("待调度", "迁移中"):
                return None, f"{self.ENTITY} {entry_id} 当前状态 {entry.get('status')} 不允许标记失败"

            entry["status"] = "已失败"
            entry["pending"] = False
            entry["abnormal"] = True
            entry["迁移状态"] = "已失败"
            self._write_cold_storage_record(
                str(entry.get("源资产", "")), entry, checksum=""
            )
            self._write_audit_entry(
                entity="冷存任务",
                action="标记失败",
                target_id=task_id,
                detail=f"冷存任务 {task_id} 标记失败",
            )
        return entry, f"{self.ENTITY} {entry_id} 已失败"

    def get_migration_status(self, asset_id: str) -> dict[str, Any] | None:
        return self._read_cold_storage_record(asset_id)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "发起迁移":
            return self.start_migration(entry_id)
        if action == "确认完成":
            return self.complete_migration(entry_id)
        if action == "标记失败":
            return self.mark_failed(entry_id)
        return super().run_action(entry_id, action)

    # ==================== 证据产物读取（供 hot_prefetch 消费） ====================

    def _start_path(self, task_id: str) -> str:
        return os.path.join(DATA_DIR, f"cold_migration_start_{_safe_name(task_id)}.json")

    def _verify_path(self, task_id: str) -> str:
        return os.path.join(DATA_DIR, f"cold_migration_verify_{_safe_name(task_id)}.json")

    def _credential_path(self, task_id: str) -> str:
        return os.path.join(DATA_DIR, f"cold_completion_credential_{_safe_name(task_id)}.json")

    @classmethod
    def read_initiation_record(cls, task_id: str) -> dict[str, Any] | None:
        return cls._load_json(
            os.path.join(DATA_DIR, f"cold_migration_start_{_safe_name(task_id)}.json")
        )

    @classmethod
    def read_verification_record(cls, task_id: str) -> dict[str, Any] | None:
        return cls._load_json(
            os.path.join(DATA_DIR, f"cold_migration_verify_{_safe_name(task_id)}.json")
        )

    @classmethod
    def read_completion_credential(cls, task_id: str) -> dict[str, Any] | None:
        return cls._load_json(
            os.path.join(DATA_DIR, f"cold_completion_credential_{_safe_name(task_id)}.json")
        )

    @staticmethod
    def _credential_usable(
        credential: dict[str, Any] | None, task_id: str, target_storage: str
    ) -> bool:
        return (
            credential is not None
            and credential.get("status") == "valid"
            and credential.get("task_id") == task_id
            and credential.get("target_storage") == target_storage
        )

    # ==================== 校验逻辑 ====================

    def _build_initiation_record(
        self, entry: dict[str, Any], epoch: int
    ) -> dict[str, Any]:
        task_id = str(entry.get("任务编号", ""))
        asset_id = str(entry.get("源资产", ""))
        target_storage = str(entry.get("目标存储", ""))
        object_key = f"{target_storage}/{asset_id}/{task_id}/data.parquet"
        object_checksum = self._object_checksum(
            task_id, asset_id, target_storage, epoch, object_key
        )
        objects = [{
            "object_key": object_key,
            "checksum": object_checksum,
            "algorithm": _CHECKSUM_ALGORITHM,
        }]
        return {
            "task_id": task_id,
            "asset_id": asset_id,
            "target_storage": target_storage,
            "epoch": epoch,
            "initiated_at": _utc_now(),
            "manifest": {
                "algorithm": _CHECKSUM_ALGORITHM,
                "objects": objects,
                "manifest_checksum": self._manifest_checksum(objects),
            },
        }

    def _run_verification(self, start_record: dict[str, Any]) -> dict[str, Any]:
        """模拟读取冷存目标上的对象并重新核算校验值，与发起基线逐项比对。"""
        task_id = str(start_record["task_id"])
        target_storage = str(start_record["target_storage"])
        asset_id = str(start_record["asset_id"])
        epoch = int(start_record["epoch"])
        manifest = start_record["manifest"]

        object_results = []
        all_match = True
        for obj in manifest["objects"]:
            object_key = str(obj["object_key"])
            expected = str(obj["checksum"])
            # 以发起记录中的任务要素重建冷存对象并核算，篡改任一字段都会失配
            actual = self._object_checksum(
                task_id, asset_id, target_storage, epoch, object_key
            )
            match = actual == expected
            all_match = all_match and match
            object_results.append({
                "object_key": object_key,
                "expected_checksum": expected,
                "actual_checksum": actual,
                "match": match,
            })

        expected_manifest = str(manifest["manifest_checksum"])
        actual_manifest = self._manifest_checksum(manifest["objects"])
        manifest_match = expected_manifest == actual_manifest

        return {
            "task_id": task_id,
            "target_storage": target_storage,
            "epoch": epoch,
            "algorithm": _CHECKSUM_ALGORITHM,
            "objects": object_results,
            "expected_checksum": expected_manifest,
            "actual_checksum": actual_manifest,
            "manifest_match": manifest_match,
            "verified": all_match and manifest_match,
            "verified_at": _utc_now(),
        }

    @staticmethod
    def _object_checksum(
        task_id: str, asset_id: str, target_storage: str, epoch: int, object_key: str
    ) -> str:
        import hashlib

        payload = "|".join(
            [task_id, asset_id, target_storage, str(epoch), object_key]
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    @staticmethod
    def _manifest_checksum(objects: list[dict[str, Any]]) -> str:
        import hashlib

        canonical = json.dumps(
            [{"object_key": o["object_key"], "checksum": o["checksum"]}
             for o in objects],
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    def _issue_credential(
        self,
        entry: dict[str, Any],
        start_record: dict[str, Any],
        verify_record: dict[str, Any],
    ) -> dict[str, Any]:
        task_id = str(entry.get("任务编号", ""))
        epoch = int(start_record["epoch"])
        return {
            "credential_id": f"CRED-{_safe_name(task_id)}-E{epoch}",
            "task_id": task_id,
            "asset_id": str(entry.get("源资产", "")),
            "target_storage": str(entry.get("目标存储", "")),
            "epoch": epoch,
            "status": "valid",
            "issued_at": _utc_now(),
            "verification": {
                "algorithm": verify_record["algorithm"],
                "checksum": verify_record["actual_checksum"],
                "verified_at": verify_record["verified_at"],
                "verify_record": os.path.basename(self._verify_path(task_id)),
                "start_record": os.path.basename(self._start_path(task_id)),
            },
        }

    # ==================== 文件 I/O ====================

    @staticmethod
    def _load_json(path: str) -> dict[str, Any] | None:
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _dump_json(path: str, payload: dict[str, Any]) -> None:
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        os.replace(tmp_path, path)

    def _write_cold_storage_record(
        self,
        asset_id: str,
        entry: dict[str, Any],
        checksum: str,
        credential_id: str = "",
    ) -> None:
        filename = f"cold_storage_{_safe_name(asset_id)}.json"
        path = os.path.join(DATA_DIR, filename)
        record = {
            "asset_id": asset_id,
            "task_id": str(entry.get("任务编号", "")),
            "target_storage": str(entry.get("目标存储", "")),
            "compression": str(entry.get("压缩算法", "")),
            "migration_volume": str(entry.get("迁移量", "")),
            "status": str(entry.get("迁移状态", "")),
            # 只允许写入实际校验得到的校验值，杜绝硬编码“校验通过”
            "checksum": checksum,
            "credential_id": credential_id,
            "updated_at": _utc_now(),
        }
        self._dump_json(path, record)

    def _read_cold_storage_record(self, asset_id: str) -> dict[str, Any] | None:
        filename = f"cold_storage_{_safe_name(asset_id)}.json"
        path = os.path.join(DATA_DIR, filename)
        return self._load_json(path)

    # ==================== 跨模块副作用 ====================

    def _update_cost_tracker(self, asset_id: str, target_storage: str) -> None:
        """更新成本追踪记录中的存储成本。"""
        cost_rows = store.rows("cost_tracker")
        for row in cost_rows:
            if str(row.get("关联资产")) == asset_id:
                row["存储成本"] = f"冷存:{target_storage}"
                row["status"] = "已核算"
                row["pending"] = False
                break

    def _check_prefetch_need(self, asset_id: str) -> None:
        """检查冷存资产是否有预热回迁需求（仅在真实完成时调用一次）。"""
        hot_rows = store.rows("hot_prefetch")
        for row in hot_rows:
            if str(row.get("冷存位置", "")).find(asset_id) >= 0 and row.get("status") == "排队中":
                return
        # 无已有预热任务则创建种子
        next_id = max((int(r.get("id", 0)) for r in hot_rows), default=0) + 1
        prefetch = {
            "id": next_id,
            "status": "排队中",
            "pending": True,
            "abnormal": False,
            "任务编号": f"HOT_-{next_id:04d}",
            "冷存位置": f"S3://glacier/{asset_id}",
            "目标分区": f"{asset_id}_hot",
            "回迁量": "待估算",
            "预计耗时": "待估算",
            "优先级": "低",
            "过期策略": "T+7",
            "任务状态": "排队中",
        }
        hot_rows.append(prefetch)

    def _write_audit_entry(self, entity: str, action: str, target_id: str,
                           detail: str) -> None:
        audit_rows = store.rows("audit_log")
        next_id = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        entry = {
            "id": next_id,
            "status": "已记录",
            "pending": False,
            "abnormal": False,
            "记录编号": f"AUDI-{next_id:04d}",
            "操作主体": entity,
            "操作类型": action,
            "操作对象": target_id,
            "操作时间": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "影响范围": detail,
            "审计摘要": detail,
            "归档状态": "已记录",
        }
        audit_rows.append(entry)
