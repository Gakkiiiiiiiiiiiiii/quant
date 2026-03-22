from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant_demo.core.events import AuditRecord
from quant_demo.db.models import AuditLogModel


class AuditRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add_log(self, record: AuditRecord) -> AuditLogModel:
        model = AuditLogModel(
            object_type=record.object_type,
            object_id=record.object_id,
            message=record.message,
            payload=record.payload,
        )
        self.session.add(model)
        return model

    def list_logs(self) -> list[AuditLogModel]:
        return list(self.session.scalars(select(AuditLogModel).order_by(AuditLogModel.created_at)))
