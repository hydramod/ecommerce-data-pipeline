from pydantic import BaseModel, EmailStr, Field
from typing import Optional

class RegisterPayload(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8)
    role: Optional[str] = 'customer'

class LoginPayload(BaseModel):
    email: EmailStr
    password: str

class UserRead(BaseModel):
    id: int
    email: EmailStr
    role: str
    class Config: from_attributes = True

class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = 'bearer'

class RefreshRequest(BaseModel):
    refresh_token: str