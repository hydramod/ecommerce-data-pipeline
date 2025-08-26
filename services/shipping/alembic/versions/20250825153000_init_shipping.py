from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20250825153000_init_shipping"
down_revision = None
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.create_table(
        "shipments",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("order_id", sa.Integer, nullable=False, index=True),
        sa.Column("user_email", sa.String(length=255), nullable=False),
        sa.Column("address_line1", sa.String(length=255), nullable=False),
        sa.Column("address_line2", sa.String(length=255), nullable=True),
        sa.Column("city", sa.String(length=120), nullable=False),
        sa.Column("country", sa.String(length=2), nullable=False),
        sa.Column("postcode", sa.String(length=32), nullable=False),
        sa.Column("carrier", sa.String(length=64), nullable=True),
        sa.Column("tracking_number", sa.String(length=64), nullable=True),
        sa.Column("status", sa.Enum("PENDING_PAYMENT","READY_TO_SHIP","DISPATCHED","DELIVERED","CANCELLED", name="shipmentstatus"), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )

def downgrade() -> None:
    op.drop_table("shipments")
