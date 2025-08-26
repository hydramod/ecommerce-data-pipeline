from alembic import op
import sqlalchemy as sa

revision='20250823142355'
down_revision=None

def upgrade():
    op.create_table('users', sa.Column('id', sa.Integer(), primary_key=True), sa.Column('email', sa.String(255), nullable=False, unique=True), sa.Column('password_hash', sa.String(255), nullable=False), sa.Column('role', sa.String(32), nullable=False, server_default='customer'), sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text("(now() at time zone 'utc')")), sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text("(now() at time zone 'utc')")))
    op.create_table('refresh_tokens', sa.Column('id', sa.Integer(), primary_key=True), sa.Column('user_id', sa.Integer(), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False), sa.Column('jti', sa.String(64), nullable=False, unique=True), sa.Column('token_hash', sa.String(64), nullable=False), sa.Column('expires_at', sa.DateTime(), nullable=False), sa.Column('revoked', sa.Boolean(), nullable=False, server_default=sa.text('false')), sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text("(now() at time zone 'utc')")))

def downgrade():
    op.drop_table('refresh_tokens'); op.drop_table('users')
