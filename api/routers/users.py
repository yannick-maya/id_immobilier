from fastapi import APIRouter, HTTPException, Depends
from ..database import db
from ..models.user import UserCreate, UserLogin, UserResponse, TokenResponse
from ..auth.password import get_password_hash, verify_password
from ..auth.jwt import create_access_token
from ..auth.middleware import get_current_user
from bson import ObjectId
from datetime import datetime

router = APIRouter()

@router.post("/auth/register", response_model=TokenResponse)
async def register(user: UserCreate):
    try:
        # Vérifier email unique
        existing_user = await db.users.find_one({"email": user.email})
        if existing_user:
            raise HTTPException(status_code=400, detail="Email already registered")

        # Vérifier la longueur minimale du mot de passe
        raw_password = user.password
        if len(raw_password) < 8:
            raise HTTPException(status_code=400, detail="Le mot de passe doit contenir au moins 8 caractères")

        try:
            hashed_password = get_password_hash(raw_password)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"hash error: {type(e).__name__}: {e}")

        user_dict = user.dict()
        user_dict.pop("password")  # Ne pas stocker le mot de passe en clair
        user_dict["hashed_password"] = hashed_password
        user_dict["role"] = "user"
        user_dict["created_at"] = datetime.utcnow().isoformat() + "Z"

        result = await db.users.insert_one(user_dict)
        user_dict["id"] = str(result.inserted_id)

        # Créer token
        access_token = create_access_token(data={"sub": str(result.inserted_id)})

        user_response = UserResponse(**{k: v for k, v in user_dict.items() if k != "hashed_password"})

        return TokenResponse(access_token=access_token, user=user_response)
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/auth/login", response_model=TokenResponse)
async def login(credentials: UserLogin):
    user = await db.users.find_one({"email": credentials.email})
    if not user or not verify_password(credentials.password, user.get("hashed_password", "")):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    access_token = create_access_token(data={"sub": str(user["_id"])})

    user_response = UserResponse(**{**{k: v for k, v in user.items() if k != "hashed_password"}, "id": str(user["_id"])})

    return TokenResponse(access_token=access_token, user=user_response)

@router.get("/auth/me", response_model=UserResponse)
async def get_me(current_user: dict = Depends(get_current_user)):
    return UserResponse(**{**current_user, "id": str(current_user["_id"])})

@router.put("/auth/me", response_model=UserResponse)
async def update_me(
    nom: str = None,
    prenom: str = None,
    current_user: dict = Depends(get_current_user)
):
    update_data = {}
    if nom is not None:
        update_data["nom"] = nom
    if prenom is not None:
        update_data["prenom"] = prenom

    if update_data:
        await db.users.update_one({"_id": current_user["_id"]}, {"$set": update_data})
        current_user.update(update_data)

    return UserResponse(**{**current_user, "id": str(current_user["_id"])})