from fastapi import APIRouter, HTTPException, Depends
from ..database import db
from ..auth.middleware import get_current_user
from bson import ObjectId

router = APIRouter()

@router.get("/favoris")
async def get_favoris(current_user: dict = Depends(get_current_user)):
    user = await db.users.find_one({"_id": current_user["_id"]})
    favoris_ids = user.get("favoris", [])

    if not favoris_ids:
        return []

    # Récupérer les annonces favorites
    object_ids = [ObjectId(fid) for fid in favoris_ids]
    cursor = db.annonces.find({"_id": {"$in": object_ids}})
    annonces = await cursor.to_list(length=None)

    return [{"id": str(a["_id"]), **{k: v for k, v in a.items() if k != "_id"}} for a in annonces]

@router.post("/favoris/{annonce_id}")
async def add_favori(annonce_id: str, current_user: dict = Depends(get_current_user)):
    try:
        annonce = await db.annonces.find_one({"_id": ObjectId(annonce_id)})
    except:
        raise HTTPException(status_code=404, detail="Annonce not found")

    if annonce is None:
        raise HTTPException(status_code=404, detail="Annonce not found")

    # Ajouter aux favoris de l'utilisateur
    await db.users.update_one(
        {"_id": current_user["_id"]},
        {"$addToSet": {"favoris": annonce_id}}
    )

    return {"message": "Annonce ajoutée aux favoris"}

@router.delete("/favoris/{annonce_id}")
async def remove_favori(annonce_id: str, current_user: dict = Depends(get_current_user)):
    # Retirer des favoris
    await db.users.update_one(
        {"_id": current_user["_id"]},
        {"$pull": {"favoris": annonce_id}}
    )

    return {"message": "Annonce retirée des favoris"}