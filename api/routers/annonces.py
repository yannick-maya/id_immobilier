from fastapi import APIRouter, Query, HTTPException, Depends
from typing import Optional, List
from datetime import datetime
from ..database import db
from ..models.annonce import AnnonceResponse, AnnonceCreate
from ..auth.middleware import get_current_user
from bson import ObjectId

router = APIRouter()

@router.get("/annonces", response_model=List[AnnonceResponse])
async def get_annonces(
    zone: Optional[str] = None,
    type_bien: Optional[str] = None,
    type_offre: Optional[str] = None,
    prix_min: Optional[float] = None,
    prix_max: Optional[float] = None,
    periode: Optional[str] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100)
):
    query = {}

    if zone:
        query["zone"] = zone
    if type_bien:
        query["type_bien"] = type_bien
    if type_offre:
        query["type_offre"] = type_offre
    if periode:
        query["periode"] = periode

    if prix_min is not None or prix_max is not None:
        prix_query = {}
        if prix_min is not None:
            prix_query["$gte"] = prix_min
        if prix_max is not None:
            prix_query["$lte"] = prix_max
        query["prix"] = prix_query

    skip = (page - 1) * limit
    cursor = db.annonces.find(query).skip(skip).limit(limit)
    annonces = await cursor.to_list(length=limit)

    return [AnnonceResponse(**{**annonce, "id": str(annonce["_id"])}) for annonce in annonces]

@router.get("/annonces/{annonce_id}", response_model=AnnonceResponse)
async def get_annonce(annonce_id: str):
    try:
        annonce = await db.annonces.find_one({"_id": ObjectId(annonce_id)})
    except:
        raise HTTPException(status_code=404, detail="Annonce not found")

    if annonce is None:
        raise HTTPException(status_code=404, detail="Annonce not found")

    return AnnonceResponse(**{**annonce, "id": str(annonce["_id"])})

@router.get("/annonces/search", response_model=List[AnnonceResponse])
async def search_annonces(q: str, limit: int = Query(20, ge=1, le=100)):
    # Recherche full-text sur le titre
    query = {"titre": {"$regex": q, "$options": "i"}}
    cursor = db.annonces.find(query).limit(limit)
    annonces = await cursor.to_list(length=limit)

    return [AnnonceResponse(**{**annonce, "id": str(annonce["_id"])}) for annonce in annonces]

@router.post("/annonces", response_model=AnnonceResponse)
async def create_annonce(annonce: AnnonceCreate, current_user: dict = Depends(get_current_user)):
    annonce_dict = annonce.dict()
    annonce_dict["user_id"] = current_user["_id"]
    annonce_dict["statut"] = "en_attente"
    annonce_dict["created_at"] = datetime.utcnow().isoformat() + "Z"

    result = await db.annonces.insert_one(annonce_dict)
    annonce_dict["id"] = str(result.inserted_id)

    return AnnonceResponse(**annonce_dict)