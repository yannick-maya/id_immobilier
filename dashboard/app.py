"""
Dashboard Streamlit - ID Immobilier
Indice Intelligent du Marche Immobilier au Togo
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import folium
from streamlit_folium import st_folium
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="ID Immobilier - Togo",
    page_icon="",
    layout="wide"
)

# ── Connexion MySQL ────────────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DB", "id_immobilier"),
        buffered=True
    )

def run_query(query):
    conn = get_connection()
    return pd.read_sql(query, conn)

@st.cache_data(ttl=3600)
def load_statistiques():
    return run_query("""
        SELECT s.*, z.nom AS zone_nom
        FROM statistiques_zone s
        JOIN zone_geographique z ON s.id_zone = z.id
    """)

@st.cache_data(ttl=3600)
def load_annonces():
    return run_query("""
        SELECT a.prix, a.prix_m2, a.titre,
               b.type_bien, b.type_offre, b.surface_m2,
               z.nom AS zone,
               s.nom AS source
        FROM annonce a
        JOIN bien_immobilier b ON a.id_bien = b.id
        JOIN zone_geographique z ON b.id_zone = z.id
        JOIN source_donnees s ON a.id_source = s.id
        WHERE a.prix_m2 IS NOT NULL AND a.prix_m2 > 0
    """)

@st.cache_data(ttl=3600)
def load_venales():
    return run_query("""
        SELECT vv.prix_m2_officiel, vv.surface_m2, vv.valeur_totale,
               z.nom AS zone
        FROM valeur_venale vv
        JOIN zone_geographique z ON vv.id_zone = z.id
        WHERE vv.prix_m2_officiel IS NOT NULL
    """)

@st.cache_data(ttl=3600)
def load_sources():
    return run_query("""
        SELECT s.nom AS source, COUNT(a.id) AS nombre_annonces
        FROM source_donnees s
        LEFT JOIN annonce a ON a.id_source = s.id
        GROUP BY s.id, s.nom
        ORDER BY nombre_annonces DESC
    """)

@st.cache_data(ttl=3600)
def load_indice_par_type():
    """Calcule l indice directement depuis les annonces par type de bien"""
    return run_query("""
        SELECT
            b.type_bien,
            b.type_offre,
            z.nom AS zone_nom,
            AVG(a.prix_m2) AS prix_moyen_m2,
            COUNT(a.id) AS nb_annonces
        FROM annonce a
        JOIN bien_immobilier b ON a.id_bien = b.id
        JOIN zone_geographique z ON b.id_zone = z.id
        WHERE a.prix_m2 IS NOT NULL AND a.prix_m2 > 0
        GROUP BY b.type_bien, b.type_offre, z.nom
        HAVING COUNT(a.id) >= 2
        ORDER BY prix_moyen_m2 DESC
    """)

# ── Header ─────────────────────────────────────────────────────────────────────
st.title("ID Immobilier - Indice Intelligent du Marche Immobilier au Togo")
st.markdown("**Donnees combinees : ImmoAsk - Facebook - CoinAfrique - Valeurs Venales OTR**")
st.divider()

# ── Chargement ─────────────────────────────────────────────────────────────────
try:
    df_stats    = load_statistiques()
    df_annonces = load_annonces()
    df_venales  = load_venales()
    df_sources  = load_sources()
    df_indice   = load_indice_par_type()
except Exception as e:
    st.error(f"Erreur de connexion MySQL : {e}")
    st.stop()

# ── Sidebar filtres ────────────────────────────────────────────────────────────
st.sidebar.header("Filtres")

zones_dispo = sorted(df_stats["zone_nom"].unique().tolist())
zone_selectionnee = st.sidebar.selectbox("Zone geographique", ["Toutes"] + zones_dispo)

types_bien = sorted(df_stats["type_bien"].unique().tolist())
type_selectionnee = st.sidebar.multiselect("Type de bien", types_bien, default=types_bien[:3])

type_offre = st.sidebar.radio("Type d offre", ["Tous", "VENTE", "LOCATION"])

st.sidebar.divider()
st.sidebar.markdown("### Export")

# ── Filtrage ───────────────────────────────────────────────────────────────────
df_filtered = df_stats.copy()
df_ann_filtered = df_annonces.copy()
df_ind_filtered = df_indice.copy()

if zone_selectionnee != "Toutes":
    df_filtered     = df_filtered[df_filtered["zone_nom"] == zone_selectionnee]
    df_ann_filtered = df_ann_filtered[df_ann_filtered["zone"] == zone_selectionnee]
    df_ind_filtered = df_ind_filtered[df_ind_filtered["zone_nom"] == zone_selectionnee]

if type_selectionnee:
    df_filtered     = df_filtered[df_filtered["type_bien"].isin(type_selectionnee)]
    df_ann_filtered = df_ann_filtered[df_ann_filtered["type_bien"].isin(type_selectionnee)]
    df_ind_filtered = df_ind_filtered[df_ind_filtered["type_bien"].isin(type_selectionnee)]

if type_offre != "Tous":
    df_filtered     = df_filtered[df_filtered["type_offre"] == type_offre]
    df_ann_filtered = df_ann_filtered[df_ann_filtered["type_offre"] == type_offre]
    df_ind_filtered = df_ind_filtered[df_ind_filtered["type_offre"] == type_offre]

# ── KPIs ───────────────────────────────────────────────────────────────────────
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Prix moyen / m2",    f"{df_filtered['prix_moyen_m2'].mean():,.0f} FCFA")
col2.metric("Prix median / m2",   f"{df_filtered['prix_median_m2'].median():,.0f} FCFA")
col3.metric("Annonces analysees", f"{df_filtered['nombre_annonces'].sum():,}")
col4.metric("Zones couvertes",    f"{df_filtered['zone_nom'].nunique()}")
col5.metric("Biens uniques",      f"{len(df_ann_filtered):,}")

st.divider()

# ── Section 1 : Annonces par source ───────────────────────────────────────────
st.subheader("Annonces par source de donnees")
col_s1, col_s2 = st.columns([1, 2])

with col_s1:
    st.dataframe(df_sources, use_container_width=True, hide_index=True)

with col_s2:
    fig_sources = px.pie(
        df_sources, values="nombre_annonces", names="source",
        color_discrete_sequence=px.colors.qualitative.Set2,
        title="Repartition des annonces par source"
    )
    fig_sources.update_traces(textposition="inside", textinfo="percent+label")
    st.plotly_chart(fig_sources, use_container_width=True)

st.divider()

# ── Section 2 : Prix moyen par zone ───────────────────────────────────────────
st.subheader("Prix moyen au m2 par zone geographique")
df_bar = df_filtered.groupby("zone_nom")["prix_moyen_m2"].mean().reset_index()
df_bar = df_bar.sort_values("prix_moyen_m2", ascending=True).tail(20)
fig_bar = px.bar(
    df_bar, x="prix_moyen_m2", y="zone_nom",
    orientation="h",
    labels={"prix_moyen_m2": "Prix moyen / m2 (FCFA)", "zone_nom": "Zone"},
    color="prix_moyen_m2",
    color_continuous_scale="Oranges",
    title="Top 20 zones - Prix moyen au m2"
)
st.plotly_chart(fig_bar, use_container_width=True)

st.divider()

# ── Section 3 : Distribution des prix ─────────────────────────────────────────
st.subheader("Distribution des prix au m2")
p95 = df_ann_filtered["prix_m2"].quantile(0.95)
df_histo = df_ann_filtered[df_ann_filtered["prix_m2"] <= p95]

fig_histo = px.histogram(
    df_histo, x="prix_m2",
    nbins=50,
    color="type_offre" if type_offre == "Tous" else None,
    labels={"prix_m2": "Prix au m2 (FCFA)", "count": "Nombre d annonces"},
    title="Distribution des prix au m2 (95e percentile - valeurs aberrantes exclues)",
    color_discrete_map={"VENTE": "#E67E22", "LOCATION": "#2980B9"}
)
fig_histo.update_layout(bargap=0.1)
st.plotly_chart(fig_histo, use_container_width=True)

st.divider()

# ── Section 4 : Comparaison prix marche vs valeurs venales ────────────────────
st.subheader("Comparaison Prix Marche vs Valeurs Venales Officielles")

if not df_venales.empty:
    marche = df_ann_filtered.groupby("zone")["prix_m2"].mean().reset_index()
    marche.columns = ["zone", "prix_marche_m2"]
    venales_agg = df_venales.groupby("zone")["prix_m2_officiel"].mean().reset_index()
    venales_agg.columns = ["zone", "prix_venale_m2"]
    df_comp = marche.merge(venales_agg, on="zone", how="inner")
    df_comp = df_comp.sort_values("prix_marche_m2", ascending=False).head(15)

    if not df_comp.empty:
        fig_comp = go.Figure()
        fig_comp.add_trace(go.Bar(
            name="Prix Marche (annonces)",
            x=df_comp["zone"], y=df_comp["prix_marche_m2"],
            marker_color="#E67E22"
        ))
        fig_comp.add_trace(go.Bar(
            name="Valeur Venale Officielle",
            x=df_comp["zone"], y=df_comp["prix_venale_m2"],
            marker_color="#2ECC71"
        ))
        fig_comp.update_layout(
            barmode="group",
            title="Prix marche vs Valeurs venales par zone (FCFA/m2)",
            xaxis_title="Zone", yaxis_title="Prix / m2 (FCFA)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02)
        )
        st.plotly_chart(fig_comp, use_container_width=True)
    else:
        st.info("Pas assez de zones en commun pour la comparaison.")
else:
    st.info("Valeurs venales non disponibles.")

st.divider()

# ── Section 5 : Carte Folium ───────────────────────────────────────────────────
st.subheader("Carte des prix au m2 a Lome")

COORDS_LOME = {
    "baguida": [6.1167, 1.3500], "agoe": [6.1900, 1.2200],
    "adidogome": [6.1700, 1.2000], "adetikope": [6.2200, 1.1800],
    "be": [6.1300, 1.2300], "tokoin": [6.1500, 1.2100],
    "djidjole": [6.1600, 1.1900], "nyekonakpoe": [6.1400, 1.2200],
    "hedzranawoe": [6.1350, 1.2150], "ablogame": [6.1250, 1.2050],
    "kodjoviakope": [6.1200, 1.2100], "aflao": [6.1050, 1.1950],
    "kegue": [6.1450, 1.2300], "nukafu": [6.1550, 1.2400],
    "zanguera": [6.1650, 1.2500], "cacaveli": [6.1500, 1.2600],
    "lome": [6.1375, 1.2123], "bè": [6.1300, 1.2300],
}

df_carte = df_filtered.groupby("zone_nom")["prix_moyen_m2"].mean().reset_index()
df_carte.columns = ["zone", "prix_moyen_m2"]
df_carte["zone_lower"] = df_carte["zone"].str.lower().str.strip()
df_carte = df_carte.merge(
    pd.DataFrame([(k, v[0], v[1]) for k, v in COORDS_LOME.items()],
                 columns=["zone_lower", "lat", "lon"]),
    on="zone_lower", how="inner"
)

m = folium.Map(location=[6.1375, 1.2123], zoom_start=12, tiles="CartoDB positron")

if not df_carte.empty:
    prix_max = df_carte["prix_moyen_m2"].max()
    prix_min = df_carte["prix_moyen_m2"].min()
    for _, row in df_carte.iterrows():
        ratio = (row["prix_moyen_m2"] - prix_min) / (prix_max - prix_min + 1)
        r = int(255 * ratio)
        g = int(255 * (1 - ratio))
        couleur = f"#{r:02x}{g:02x}33"
        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=15,
            color=couleur,
            fill=True,
            fill_color=couleur,
            fill_opacity=0.7,
            popup=folium.Popup(
                f"<b>{row['zone'].title()}</b><br>"
                f"Prix moyen : {row['prix_moyen_m2']:,.0f} FCFA/m2",
                max_width=200
            ),
            tooltip=f"{row['zone'].title()} - {row['prix_moyen_m2']:,.0f} FCFA/m2"
        ).add_to(m)

st_folium(m, width=None, height=450, use_container_width=True)
st.caption("Vert = moins cher | Rouge = plus cher | Clic sur un point pour le detail")

st.divider()

# ── Section 6 : Biens les plus chers et moins chers ───────────────────────────
st.subheader("Biens les plus chers vs moins chers")
col_top, col_bot = st.columns(2)

with col_top:
    st.markdown("#### Top 10 - Plus chers")
    top10 = df_ann_filtered.nlargest(10, "prix_m2")[
        ["titre", "zone", "type_bien", "prix_m2", "source"]
    ].reset_index(drop=True)
    top10["prix_m2"] = top10["prix_m2"].apply(lambda x: f"{x:,.0f} FCFA")
    st.dataframe(top10, use_container_width=True, hide_index=True)

with col_bot:
    st.markdown("#### Top 10 - Moins chers")
    bot10 = df_ann_filtered[df_ann_filtered["prix_m2"] > 100].nsmallest(10, "prix_m2")[
        ["titre", "zone", "type_bien", "prix_m2", "source"]
    ].reset_index(drop=True)
    bot10["prix_m2"] = bot10["prix_m2"].apply(lambda x: f"{x:,.0f} FCFA")
    st.dataframe(bot10, use_container_width=True, hide_index=True)

st.divider()

# ── Section 7 : Indice Immobilier ─────────────────────────────────────────────
st.subheader("Indice Immobilier ID Immobilier - Comparaison par type de bien")

if not df_ind_filtered.empty:
    # Calculer l indice : prix moyen de chaque type / prix moyen global * 100
    prix_global = df_ind_filtered["prix_moyen_m2"].mean()

    df_indice_calc = df_ind_filtered.groupby(["type_bien", "type_offre"]).agg(
        prix_moyen_m2=("prix_moyen_m2", "mean"),
        nb_annonces=("nb_annonces", "sum")
    ).reset_index()

    df_indice_calc["indice"] = (df_indice_calc["prix_moyen_m2"] / prix_global * 100).round(2)
    df_indice_calc["tendance"] = df_indice_calc["indice"].apply(
        lambda x: "Au dessus de la moyenne" if x > 105 else ("En dessous de la moyenne" if x < 95 else "Dans la moyenne")
    )
    df_indice_calc = df_indice_calc.sort_values("indice", ascending=False)

    col_i1, col_i2 = st.columns([2, 1])

    with col_i1:
        fig_indice = px.bar(
            df_indice_calc,
            x="type_bien", y="indice",
            color="tendance",
            color_discrete_map={
                "Au dessus de la moyenne": "#E74C3C",
                "Dans la moyenne": "#F39C12",
                "En dessous de la moyenne": "#27AE60"
            },
            barmode="group",
            facet_col="type_offre" if type_offre == "Tous" else None,
            labels={"indice": "Indice (Base 100 = moyenne globale)", "type_bien": "Type de bien"},
            title="Indice par type de bien (Base 100 = prix moyen de tous les biens)",
            text="indice"
        )
        fig_indice.add_hline(y=100, line_dash="dash", line_color="gray",
                             annotation_text="Base 100 (moyenne)")
        fig_indice.update_traces(texttemplate="%{text:.1f}", textposition="outside")
        st.plotly_chart(fig_indice, use_container_width=True)

    with col_i2:
        st.markdown("#### Detail par type")
        df_display = df_indice_calc[["type_bien", "type_offre", "prix_moyen_m2", "indice", "nb_annonces"]].copy()
        df_display["prix_moyen_m2"] = df_display["prix_moyen_m2"].apply(lambda x: f"{x:,.0f}")
        df_display["indice"] = df_display["indice"].apply(lambda x: f"{x:.1f}")
        st.dataframe(df_display, use_container_width=True, hide_index=True)

    st.caption(
        f"Indice > 105 : type de bien au dessus de la moyenne du marche | "
        f"Indice < 95 : en dessous | Base = prix moyen global : {prix_global:,.0f} FCFA/m2"
    )
else:
    st.info("Pas assez de donnees pour calculer l indice avec les filtres actuels.")

st.divider()

# ── Section 8 : Tableau comparatif ────────────────────────────────────────────
st.subheader("Tableau comparatif des zones")
cols_affichage = ["zone_nom", "type_bien", "type_offre", "prix_moyen_m2",
                  "prix_median_m2", "nombre_annonces", "ecart_valeur_venale"]
cols_dispo = [c for c in cols_affichage if c in df_filtered.columns]
df_tableau = df_filtered[cols_dispo].sort_values("prix_moyen_m2", ascending=False)
st.dataframe(df_tableau, use_container_width=True, hide_index=True)

# ── Export CSV ─────────────────────────────────────────────────────────────────
st.sidebar.download_button(
    label="Exporter Statistiques (CSV)",
    data=df_filtered.to_csv(index=False, encoding="utf-8").encode("utf-8"),
    file_name="id_immobilier_statistiques.csv",
    mime="text/csv"
)
st.sidebar.download_button(
    label="Exporter Annonces (CSV)",
    data=df_ann_filtered.to_csv(index=False, encoding="utf-8").encode("utf-8"),
    file_name="id_immobilier_annonces.csv",
    mime="text/csv"
)

# ── Footer ─────────────────────────────────────────────────────────────────────
st.divider()
st.caption("Projet ID Immobilier - Cours Introduction Big Data | Donnees : ImmoAsk, Facebook, CoinAfrique, OTR Togo")