"""initial durable runtime schema

Revision ID: 001_initial
Revises:
Create Date: 2026-10-11
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "workflow_runs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("workflow_name", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("input_payload", mysql.JSON(), nullable=False),
        sa.Column("result_payload", mysql.JSON(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("lease_owner", sa.String(length=64), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("UTC_TIMESTAMP()")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("UTC_TIMESTAMP()")),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_runs_claim", "workflow_runs", ["status", "lease_expires_at", "created_at"])
    op.create_index("ix_runs_created", "workflow_runs", ["created_at"])
    op.create_index("ix_workflow_runs_status", "workflow_runs", ["status"])

    op.create_table(
        "workflow_events",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("run_id", sa.String(length=36), sa.ForeignKey("workflow_runs.id"), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("payload", mysql.JSON(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("correlation_id", sa.String(length=36), nullable=True),
        sa.Column("causation_id", sa.String(length=36), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("UTC_TIMESTAMP()")),
        sa.UniqueConstraint("run_id", "sequence", name="ux_events_run_sequence"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_events_run_created", "workflow_events", ["run_id", "created_at"])

    op.create_table(
        "tool_operations",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("idempotency_key", sa.String(length=191), nullable=False),
        sa.Column("run_id", sa.String(length=36), sa.ForeignKey("workflow_runs.id"), nullable=False),
        sa.Column("step_id", sa.String(length=64), nullable=False),
        sa.Column("tool_name", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("request_payload", mysql.JSON(), nullable=False),
        sa.Column("result_payload", mysql.JSON(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("UTC_TIMESTAMP()")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("UTC_TIMESTAMP()")),
        sa.UniqueConstraint("idempotency_key", name="ux_tool_operations_key"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )

    op.create_table(
        "tool_effects",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("idempotency_key", sa.String(length=191), nullable=False),
        sa.Column("run_id", sa.String(length=36), sa.ForeignKey("workflow_runs.id"), nullable=False),
        sa.Column("tool_name", sa.String(length=64), nullable=False),
        sa.Column("payload", mysql.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("UTC_TIMESTAMP()")),
        sa.UniqueConstraint("idempotency_key", name="ux_tool_effects_key"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )

    op.create_table(
        "workflow_snapshots",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("run_id", sa.String(length=36), sa.ForeignKey("workflow_runs.id"), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("state_payload", mysql.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("UTC_TIMESTAMP()")),
        sa.UniqueConstraint("run_id", "sequence", name="ux_snapshots_run_sequence"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )


def downgrade() -> None:
    op.drop_table("workflow_snapshots")
    op.drop_table("tool_effects")
    op.drop_table("tool_operations")
    op.drop_table("workflow_events")
    op.drop_table("workflow_runs")
