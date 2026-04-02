from fastapi import APIRouter, Query
from typing import Optional, List
from ..database import db

router = APIRouter()

@router.get("/statistiques")
async def get_statistiques(
    zone: Optional[str] = None,
    type_bien: Optional[str] = None,
    periode: Optional[str] = None
):
    query = {}

    if zone:
        query["zone"] = zone
    if type_bien:
        query["type_bien"] = type_bien
    if periode:
        query["periode"] = periode

    cursor = db.statistiques.find(query)
    stats = await cursor.to_list(length=None)

    return stats

@router.get("/statistiques/{zone}")
async def get_statistiques_zone(zone: str):
    # Stats détaillées + historique par période
    pipeline = [
        {"$match": {"zone": zone}},
        {"$sort": {"periode": 1}}
    ]

    cursor = db.statistiques.aggregate(pipeline)
    stats = await cursor.to_list(length=None)

    return stats