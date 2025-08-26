from alembic import op
import sqlalchemy as sa

revision = "20250823142909"
down_revision = None

def upgrade():
    op.create_table(
        'orders',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('user_email', sa.String(length=255), index=True, nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False, server_default='CREATED'),
        sa.Column('total_cents', sa.BigInteger(), nullable=False),
        sa.Column('currency', sa.String(length=3), nullable=False, server_default='USD'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text("(now() at time zone 'utc')")),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text("(now() at time zone 'utc')")),
    )
    op.create_table(
        'order_items',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('order_id', sa.Integer(), sa.ForeignKey('orders.id', ondelete='CASCADE'), nullable=False),
        sa.Column('product_id', sa.Integer(), nullable=False),
        sa.Column('qty', sa.Integer(), nullable=False),
        sa.Column('unit_price_cents', sa.BigInteger(), nullable=False),
        sa.Column('title_snapshot', sa.String(length=255), nullable=False),
    )

def downgrade():
    op.drop_table('order_items')
    op.drop_table('orders')
