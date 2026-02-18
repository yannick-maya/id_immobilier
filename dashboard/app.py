"""
Dashboard Streamlit — ID Immobilier
Indice Intelligent du Marché Immobilier au Togo
Connexion : MySQL
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

# ─── Config page ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ID Immobilier — Togo",
    page_icon="🏠",
    layout="wide"
)

# ─── Connexion MySQL ───────────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DB", "id_immobilier")
    )

@st.cache_data(ttl=3600)
def load_statistiques():
    conn = get_connection()
    query = """
        SELECT s.*, z.nom AS zone_nom
        FROM statistiques_zone s
        JOIN zone_geographique z ON s.id_zone = z.id
    """
    return pd.read_sql(query, conn)

@st.cache_data(ttl=3600)
def load_indice():
    conn = get_connection()
    query = """
        SELECT i.*, z.nom AS zone_nom
        FROM indice_immobilier i
        JOIN zone_geographique z ON i.id_zone = z.id
    """
    return pd.read_sql(query, conn)

# ─── Header ────────────────────────────────────────────────────────────────────
st.title("🏠 ID Immobilier — Indice Intelligent du Marché Immobilier au Togo")
st.markdown("**Données combinées : ImmoAsk · Facebook · CoinAfrique · Valeurs Vénales OTR**")
st.divider()

# ─── Chargement des données ────────────────────────────────────────────────────
try:
    df_stats = load_statistiques()
    df_indice = load_indice()
except Exception as e:
    st.error(f"❌ Erreur de connexion MySQL : {e}")
    st.info("Lance d'abord le pipeline : `python pipeline/modeling.py`")
    st.stop()

# ─── Sidebar filtres ───────────────────────────────────────────────────────────
st.sidebar.header("🔎 Filtres")
zones_dispo = sorted(df_stats["zone_nom"].unique().tolist())
zone_selectionnee = st.sidebar.selectbox("📍 Zone géographique", ["Toutes"] + zones_dispo)

types_bien = sorted(df_stats["type_bien"].unique().tolist())
type_selectionnee = st.sidebar.multiselect("🏗️ Type de bien", types_bien, default=types_bien[:3])

type_offre = st.sidebar.radio("💼 Type d'offre", ["Tous", "VENTE", "LOCATION"])

# ─── Filtrage ──────────────────────────────────────────────────────────────────
df_filtered = df_stats.copy()
if zone_selectionnee != "Toutes":
    df_filtered = df_filtered[df_filtered["zone_nom"] == zone_selectionnee]
if type_selectionnee:
    df_filtered = df_filtered[df_filtered["type_bien"].isin(type_selectionnee)]
if type_offre != "Tous":
    df_filtered = df_filtered[df_filtered["type_offre"] == type_offre]

# ─── KPIs ──────────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("📊 Prix moyen / m²", f"{df_filtered['prix_moyen_m2'].mean():,.0f} FCFA")
col2.metric("📐 Prix médian / m²", f"{df_filtered['prix_median_m2'].median():,.0f} FCFA")
col3.metric("📋 Annonces analysées", f"{df_filtered['nombre_annonces'].sum():,}")
col4.metric("📍 Zones couvertes", f"{df_filtered['zone_nom'].nunique()}")

st.divider()

# ─── Graphique 1 : Prix moyen au m² par zone ───────────────────────────────────
st.subheader("📊 Prix moyen au m² par zone géographique")
df_bar = df_filtered.groupby("zone_nom")["prix_moyen_m2"].mean().reset_index()
df_bar = df_bar.sort_values("prix_moyen_m2", ascending=True)
fig_bar = px.bar(
    df_bar, x="prix_moyen_m2", y="zone_nom",
    orientation="h",
    labels={"prix_moyen_m2": "Prix moyen / m² (FCFA)", "zone_nom": "Zone"},
    color="prix_moyen_m2",
    color_continuous_scale="Oranges"
)
st.plotly_chart(fig_bar, use_container_width=True)

# ─── Graphique 2 : Indice immobilier par zone ──────────────────────────────────
st.subheader("📈 Indice immobilier ID Immobilier par zone")
df_indice_filtered = df_indice.copy()
if zone_selectionnee != "Toutes":
    df_indice_filtered = df_indice_filtered[df_indice_filtered["zone_nom"] == zone_selectionnee]

fig_indice = px.bar(
    df_indice_filtered,
    x="zone_nom", y="indice_valeur",
    color="tendance",
    color_discrete_map={"HAUSSE": "#27AE60", "STABLE": "#F39C12", "BAISSE": "#E74C3C"},
    labels={"indice_valeur": "Indice (Base 100)", "zone_nom": "Zone"},
    title="Indice immobilier par zone (Base 100 = période référence)"
)
fig_indice.add_hline(y=100, line_dash="dash", line_color="gray", annotation_text="Base 100")
st.plotly_chart(fig_indice, use_container_width=True)

# ─── Tableau comparatif ────────────────────────────────────────────────────────
st.subheader("📋 Tableau comparatif des zones")
cols_affichage = ["zone_nom", "type_bien", "type_offre", "prix_moyen_m2",
                  "prix_median_m2", "nombre_annonces", "ecart_valeur_venale"]
cols_dispo = [c for c in cols_affichage if c in df_filtered.columns]
st.dataframe(
    df_filtered[cols_dispo].sort_values("prix_moyen_m2", ascending=False),
    use_container_width=True,
    hide_index=True
)

# ─── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption("🎓 Projet ID Immobilier — Cours Introduction Big Data | Données : ImmoAsk, Facebook, CoinAfrique, OTR Togo")
