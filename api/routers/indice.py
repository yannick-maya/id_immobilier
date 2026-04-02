from fastapi import APIRouter, Query
from typing import Optional, List
from ..database import db

router = APIRouter()

@router.get("/indice")
async def get_indices(
    zone: Optional[str] = None,
    periode: Optional[str] = None,
    tendance: Optional[str] = None
):
    query = {}

    if zone:
        query["zone"] = zone
    if periode:
        query["periode"] = periode
    if tendance:
        query["tendance"] = tendance

    cursor = db.indices.find(query)
    indices = await cursor.to_list(length=None)

    return indices

@router.get("/indice/{zone}")
async def get_indice_zone(zone: str):
    # Évolution historique triée par période
    pipeline = [
        {"$match": {"zone": zone}},
        {"$sort": {"periode": 1}}
    ]

    cursor = db.indices.aggregate(pipeline)
    indices = await cursor.to_list(length=None)

    return indices

@router.get("/indice/tendances")
async def get_tendances():
    # Résumé HAUSSE/STABLE/BAISSE avec listes de zones
    pipeline = [
        {"$group": {
            "_id": "$tendance",
            "zones": {"$push": "$zone"},
            "count": {"$sum": 1}
        }}
    ]

    cursor = db.indices.aggregate(pipeline)
    tendances = await cursor.to_list(length=None)

    return {item["_id"]: {"zones": item["zones"], "count": item["count"]} for item in tendances}