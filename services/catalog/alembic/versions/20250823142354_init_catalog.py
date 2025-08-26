from alembic import op
import sqlalchemy as sa

revision='20250823142354'
down_revision=None

def upgrade():
    op.create_table('categories', sa.Column('id', sa.Integer(), primary_key=True), sa.Column('name', sa.String(120), nullable=False, unique=True))
    op.create_table('products', sa.Column('id', sa.Integer(), primary_key=True), sa.Column('title', sa.String(240), nullable=False), sa.Column('description', sa.Text()), sa.Column('price_cents', sa.BigInteger(), nullable=False), sa.Column('currency', sa.String(3), nullable=False, server_default='USD'), sa.Column('sku', sa.String(64), nullable=False, unique=True), sa.Column('category_id', sa.Integer(), sa.ForeignKey('categories.id')), sa.Column('active', sa.Boolean(), nullable=False, server_default=sa.text('true')))
    op.create_table('product_images', sa.Column('id', sa.Integer(), primary_key=True), sa.Column('product_id', sa.Integer(), sa.ForeignKey('products.id', ondelete='CASCADE'), nullable=False), sa.Column('object_key', sa.String(255), nullable=False), sa.Column('url', sa.String(1024), nullable=False))
    op.create_table('inventory', sa.Column('product_id', sa.Integer(), sa.ForeignKey('products.id', ondelete='CASCADE'), primary_key=True), sa.Column('in_stock', sa.Integer(), nullable=False, server_default='0'), sa.Column('reserved', sa.Integer(), nullable=False, server_default='0'))

def downgrade():
    op.drop_table('inventory'); op.drop_table('product_images'); op.drop_table('products'); op.drop_table('categories')
