from pydantic import BaseModel, EmailStr
from typing import Optional, List
from datetime import datetime

class AnnonceResponse(BaseModel):
    id: Optional[str]
    titre: str
    type_offre: str
    type_bien: str
    zone: str
    prix: float
    pieces: Optional[float]
    surface_m2: Optional[float]
    source: str
    annee: Optional[int]
    trimestre: Optional[int]
    periode: Optional[str]
    created_at: Optional[str]

class AnnonceCreate(BaseModel):
    titre: str
    type_offre: str
    type_bien: str
    zone: str
    prix: float
    pieces: Optional[float] = None
    surface_m2: Optional[float] = None