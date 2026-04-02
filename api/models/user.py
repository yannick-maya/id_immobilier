from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime

class UserCreate(BaseModel):
    email: EmailStr
    password: str
    nom: Optional[str] = None
    prenom: Optional[str] = None

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class UserResponse(BaseModel):
    id: Optional[str]
    email: EmailStr
    nom: Optional[str]
    prenom: Optional[str]
    role: str
    created_at: Optional[str]

class UserInDB(UserResponse):
    hashed_password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse