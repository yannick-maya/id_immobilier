from pydantic import BaseModel
from typing import Optional

class IndiceResponse(BaseModel):
    id: Optional[str]
    zone: str
    periode: str
    indice_valeur: float
    tendance: str
    created_at: Optional[str]