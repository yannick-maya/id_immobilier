from fastapi import APIRouter, HTTPException, Depends
from ..database import db
from ..auth.middleware import get_current_admin
from bson import ObjectId

router = APIRouter()

@router.get("/admin/users")
async def get_users(current_admin: dict = Depends(get_current_admin)):
    cursor = db.users.find({}, {"hashed_password": 0})
    users = await cursor.to_list(length=None)
    return [{"id": str(u["_id"]), **{k: v for k, v in u.items() if k != "_id"}} for u in users]

@router.put("/admin/users/{user_id}")
async def update_user(user_id: str, role: str = None, current_admin: dict = Depends(get_current_admin)):
    update_data = {}
    if role:
        update_data["role"] = role

    try:
        result = await db.users.update_one({"_id": ObjectId(user_id)}, {"$set": update_data})
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="User not found")
    except:
        raise HTTPException(status_code=404, detail="User not found")

    return {"message": "User updated"}

@router.delete("/admin/users/{user_id}")
async def delete_user(user_id: str, current_admin: dict = Depends(get_current_admin)):
    try:
        result = await db.users.delete_one({"_id": ObjectId(user_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="User not found")
    except:
        raise HTTPException(status_code=404, detail="User not found")

    return {"message": "User deleted"}

@router.get("/admin/annonces")
async def get_annonces_admin(current_admin: dict = Depends(get_current_admin)):
    cursor = db.annonces.find({})
    annonces = await cursor.to_list(length=None)
    return [{"id": str(a["_id"]), **{k: v for k, v in a.items() if k != "_id"}} for a in annonces]

@router.put("/admin/annonces/{annonce_id}/valider")
async def valider_annonce(annonce_id: str, current_admin: dict = Depends(get_current_admin)):
    try:
        result = await db.annonces.update_one(
            {"_id": ObjectId(annonce_id)},
            {"$set": {"statut": "valide"}}
        )
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Annonce not found")
    except:
        raise HTTPException(status_code=404, detail="Annonce not found")

    return {"message": "Annonce validée"}

@router.put("/admin/annonces/{annonce_id}/refuser")
async def refuser_annonce(annonce_id: str, current_admin: dict = Depends(get_current_admin)):
    try:
        result = await db.annonces.update_one(
            {"_id": ObjectId(annonce_id)},
            {"$set": {"statut": "refuse"}}
        )
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Annonce not found")
    except:
        raise HTTPException(status_code=404, detail="Annonce not found")

    return {"message": "Annonce refusée"}

@router.get("/admin/stats")
async def get_admin_stats(current_admin: dict = Depends(get_current_admin)):
    # Compter les utilisateurs
    nb_users = await db.users.count_documents({})

    # Compter les annonces par statut
    pipeline_annonces = [
        {"$group": {"_id": "$statut", "count": {"$sum": 1}}}
    ]
    cursor = db.annonces.aggregate(pipeline_annonces)
    stats_annonces = await cursor.to_list(length=None)
    annonces_par_statut = {item["_id"]: item["count"] for item in stats_annonces}

    # Taux de rejet
    total_annonces = sum(annonces_par_statut.values())
    rejetees = annonces_par_statut.get("refuse", 0)
    taux_rejet = (rejetees / total_annonces * 100) if total_annonces > 0 else 0

    return {
        "nb_users": nb_users,
        "annonces_par_statut": annonces_par_statut,
        "taux_rejet": taux_rejet
    }

@router.get("/admin/okr")
async def get_okr_metrics(current_admin: dict = Depends(get_current_admin)):
    # Métriques OKR depuis MongoDB
    # À adapter selon les vraies métriques OKR du projet
    nb_annonces_total = await db.annonces.count_documents({})
    nb_users_actifs = await db.users.count_documents({"role": "user"})
    nb_indices = await db.indices.count_documents({})

    return {
        "annonces_totales": nb_annonces_total,
        "utilisateurs_actifs": nb_users_actifs,
        "indices_calcules": nb_indices
    }