from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routers import auth, annonces, statistiques, indice, favoris, admin

app = FastAPI(title="ID Immobilier API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inclure tous les routers avec leurs préfixes
app.include_router(auth.router, tags=["auth"])
app.include_router(annonces.router, tags=["annonces"])
app.include_router(statistiques.router, tags=["statistiques"])
app.include_router(indice.router, tags=["indice"])
app.include_router(favoris.router, tags=["favoris"])
app.include_router(admin.router, tags=["admin"])

@app.get("/")
def read_root():
    return {"message": "Welcome to ID Immobilier API v2.0.0"}