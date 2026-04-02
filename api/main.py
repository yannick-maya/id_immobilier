from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routers.users import router as auth_router
from .routers.annonces import router as annonces_router
from .routers.statistiques import router as statistiques_router
from .routers.indice import router as indice_router
from .routers.favoris import router as favoris_router
from .routers.admin import router as admin_router

app = FastAPI(title="ID Immobilier API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inclure tous les routers avec leurs préfixes
app.include_router(auth_router, tags=["auth"])
app.include_router(annonces_router, tags=["annonces"])
app.include_router(statistiques_router, tags=["statistiques"])
app.include_router(indice_router, tags=["indice"])
app.include_router(favoris_router, tags=["favoris"])
app.include_router(admin_router, tags=["admin"])

@app.get("/")
def read_root():
    return {"message": "Welcome to ID Immobilier API v2.0.0"}