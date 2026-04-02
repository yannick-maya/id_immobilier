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
from pymongo import MongoClient
import unicodedata
import re
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="ID Immobilier - Togo",
    page_icon="🏠",
    layout="wide"
)

@st.cache_resource
def get_connection():
    client = MongoClient(os.getenv("MONGO_URI", "mongodb://mongo:27017"))
    return client[os.getenv("MONGO_DB", "id_immobilier")]

@st.cache_data(ttl=3600)
def load_statistiques():
    db = get_connection()
    docs = list(db.statistiques.find({}))
    return pd.DataFrame(docs)

@st.cache_data(ttl=3600)
def load_annonces():
    db = get_connection()
    docs = list(db.annonces.find({}))
    return pd.DataFrame(docs)

@st.cache_data(ttl=3600)
def load_venales():
    db = get_connection()
    docs = list(db.valeurs_venales.find({}))
    return pd.DataFrame(docs)

@st.cache_data(ttl=3600)
def load_sources():
    db = get_connection()
    pipeline = [{"$group": {"_id": "$source", "nombre_annonces": {"$sum": 1}}}, {"$project": {"source": "$_id", "nombre_annonces": 1, "_id": 0}}]
    docs = list(db.annonces.aggregate(pipeline))
    return pd.DataFrame(docs)

@st.cache_data(ttl=3600)
def load_indice_par_type():
    db = get_connection()
    pipeline = [
        {"$group": {"_id": {"zone": "$zone", "type_bien": "$type_bien", "type_offre": "$type_offre"}, "prix_moyen_m2": {"$avg": "$prix_m2"}, "nb_annonces": {"$sum": 1}}},
        {"$project": {"zone": "$_id.zone", "type_bien": "$_id.type_bien", "type_offre": "$_id.type_offre", "prix_moyen_m2": 1, "nb_annonces": 1, "_id": 0}}
    ]
    docs = list(db.indices.aggregate(pipeline))
    return pd.DataFrame(docs)

@st.cache_data(ttl=3600)
def load_indice_carte():
    db = get_connection()
    pipeline = [
        {"$group": {"_id": "$zone", "prix_moyen_m2": {"$avg": "$prix_moyen_m2"}, "indice_valeur": {"$avg": "$indice_valeur"}, "nb_indices": {"$sum": 1}, "tendance": {"$first": "$tendance"}}},
        {"$project": {"zone": "$_id", "prix_moyen_m2": 1, "indice_valeur": 1, "tendance": 1, "nb_indices": 1, "_id": 0}},
        {"$sort": {"indice_valeur": -1}}
    ]
    docs = list(db.indices.aggregate(pipeline))
    return pd.DataFrame(docs)

st.sidebar.title("ID Immobilier")
page = st.sidebar.radio(
    "Navigation",
    ["Tableau de bord", "Toutes les annonces"],
    index=0
)
st.sidebar.divider()

# ── Chargement des données (commun aux deux pages) ─────────────────────────────
try:
    df_stats    = load_statistiques()
    df_annonces = load_annonces()
    df_venales  = load_venales()
    df_sources  = load_sources()
    df_indice   = load_indice_par_type()
except Exception as e:
    st.error(f"Erreur de connexion MySQL : {e}")
    st.stop()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — TABLEAU DE BORD
# ══════════════════════════════════════════════════════════════════════════════
if page == "Tableau de bord":

    st.title("ID Immobilier — Indice Intelligent du Marche Immobilier au Togo")
    st.markdown("**Donnees : ImmoAsk · Facebook · CoinAfrique · Valeurs Venales OTR**")
    st.divider()

    # Filtres sidebar
    st.sidebar.subheader("Filtres")
    zones_dispo = sorted(df_stats["zone"].unique().tolist())
    zone_sel = st.sidebar.selectbox("Zone", ["Toutes"] + zones_dispo)

    types_bien = sorted(df_stats["type_bien"].unique().tolist())
    type_sel = st.sidebar.multiselect("Type de bien", types_bien, default=types_bien)
    if not type_sel:
        type_sel = types_bien

    offre_sel = st.sidebar.radio("Type d'offre", ["Tous", "VENTE", "LOCATION"])
    periodes = sorted(df_stats["periode"].dropna().unique().tolist()) if "periode" in df_stats else []
    periode_sel = st.sidebar.selectbox("Période", ["Toutes"] + periodes)

    # Filtrage
    df_f   = df_stats.copy()
    df_ann = df_annonces.copy()
    df_ind = df_indice.copy()

    if zone_sel != "Toutes":
        df_f   = df_f[df_f["zone"] == zone_sel]
        df_ann = df_ann[df_ann["zone"] == zone_sel]
        df_ind = df_ind[df_ind["zone"] == zone_sel]
    df_f   = df_f[df_f["type_bien"].isin(type_sel)]
    df_ann = df_ann[df_ann["type_bien"].isin(type_sel)]
    df_ind = df_ind[df_ind["type_bien"].isin(type_sel)]
    if offre_sel != "Tous":
        df_f   = df_f[df_f["type_offre"] == offre_sel]
        df_ann = df_ann[df_ann["type_offre"] == offre_sel]
        df_ind = df_ind[df_ind["type_offre"] == offre_sel]
    if periode_sel != "Toutes":
        df_f   = df_f[df_f["periode"] == periode_sel]
        df_ann = df_ann[df_ann["periode"] == periode_sel] if "periode" in df_ann else df_ann
        df_ind = df_ind[df_ind["periode"] == periode_sel] if "periode" in df_ind else df_ind

    # KPIs
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Prix moyen / m2",    f"{df_f['prix_moyen_m2'].mean():,.0f} FCFA")
    k2.metric("Prix median / m2",   f"{df_f['prix_median_m2'].median():,.0f} FCFA")
    k3.metric("Annonces analysees", f"{df_f['nombre_annonces'].sum():,}")
    k4.metric("Zones couvertes",    f"{df_f['zone'].nunique()}")
    k5.metric("Biens uniques",      f"{len(df_ann):,}")
    st.divider()

    # Sources
    st.subheader("Annonces par source de donnees")
    cs1, cs2 = st.columns([1, 2])
    with cs1:
        st.dataframe(df_sources, use_container_width=True, hide_index=True)
    with cs2:
        fig_src = px.pie(df_sources, values="nombre_annonces", names="source",
                         color_discrete_sequence=px.colors.qualitative.Set2,
                         title="Repartition par source")
        fig_src.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_src, use_container_width=True)
    st.divider()

    # Top 20 zones
    st.subheader("Prix moyen au m2 par zone")
    df_bar = df_f.groupby("zone")["prix_moyen_m2"].mean().reset_index()
    df_bar = df_bar.sort_values("prix_moyen_m2", ascending=True).tail(20)
    fig_bar = px.bar(df_bar, x="prix_moyen_m2", y="zone", orientation="h",
                     color="prix_moyen_m2", color_continuous_scale="Oranges",
                     labels={"prix_moyen_m2": "Prix moyen / m2 (FCFA)", "zone": "Zone"},
                     title="Top 20 zones — Prix moyen au m2")
    st.plotly_chart(fig_bar, use_container_width=True)
    st.divider()

    # Distribution
    st.subheader("Distribution des prix au m2")
    p95 = df_ann["prix_m2"].quantile(0.95)
    fig_hist = px.histogram(
        df_ann[df_ann["prix_m2"] <= p95], x="prix_m2", nbins=50,
        color="type_offre" if offre_sel == "Tous" else None,
        labels={"prix_m2": "Prix au m2 (FCFA)"},
        title="Distribution des prix au m2 (valeurs aberrantes exclues)",
        color_discrete_map={"VENTE": "#E67E22", "LOCATION": "#2980B9"}
    )
    fig_hist.update_layout(bargap=0.1)
    st.plotly_chart(fig_hist, use_container_width=True)
    st.divider()

    # Comparaison marche vs venales
    st.subheader("Prix Marche vs Valeurs Venales OTR")
    if not df_venales.empty:
        marche = df_ann.groupby("zone")["prix_m2"].mean().reset_index()
        marche.columns = ["zone", "prix_marche"]
        venales = df_venales.groupby("zone")["prix_m2_officiel"].mean().reset_index()
        venales.columns = ["zone", "prix_venale"]
        df_comp = marche.merge(venales, on="zone").sort_values("prix_marche", ascending=False).head(15)
        if not df_comp.empty:
            fig_comp = go.Figure([
                go.Bar(name="Prix Marche",  x=df_comp["zone"], y=df_comp["prix_marche"],  marker_color="#E67E22"),
                go.Bar(name="Valeur Venale", x=df_comp["zone"], y=df_comp["prix_venale"], marker_color="#2ECC71"),
            ])
            fig_comp.update_layout(barmode="group", title="Marche vs OTR (FCFA/m2)",
                                   legend=dict(orientation="h", yanchor="bottom", y=1.02))
            st.plotly_chart(fig_comp, use_container_width=True)
        else:
            st.info("Pas de zones en commun pour la comparaison.")
    else:
        st.info("Valeurs venales non disponibles.")
    st.divider()

    # Carte — echantillon representatif
    st.subheader("Carte des prix au m2 — Zones representatives")

    df_zones = (df_f.groupby("zone")["prix_moyen_m2"].mean()
                .reset_index().rename(columns={"zone": "zone"})
                .dropna(subset=["prix_moyen_m2"]))

    rows_map, not_found = [], []
    for _, row in df_zones.iterrows():
        c = get_coords(row["zone"])
        if c:
            rows_map.append({"zone": row["zone"], "prix": float(row["prix_moyen_m2"]),
                             "lat": c[0], "lon": c[1]})
        else:
            not_found.append(row["zone"])

    df_geo = pd.DataFrame(rows_map) if rows_map else pd.DataFrame(columns=["zone","prix","lat","lon"])

    # Echantillon : 10 bas + 10 milieu + 10 haut
    def sample_zones(df, n=10):
        if len(df) <= n * 3:
            return df
        s = df.sort_values("prix").reset_index(drop=True)
        mid = len(s) // 2 - n // 2
        idx = sorted(set(list(range(n)) + list(range(mid, mid+n)) + list(range(len(s)-n, len(s)))))
        return s.iloc[idx]

    df_sample = sample_zones(df_geo, 10)

    cm1, cm2, cm3 = st.columns(3)
    cm1.metric("Zones dans la base", str(len(df_zones)))
    cm2.metric("Zones geocodees",    str(len(df_geo)))
    cm3.metric("Zones affichees",    str(len(df_sample)), delta="10 bas + 10 milieu + 10 haut")

    col_map, col_leg = st.columns([4, 1])

    with col_leg:
        st.markdown("**Legende**")
        st.markdown(
            "<div style='font-size:13px;line-height:2.2'>"
            "<span style='color:#dc3200;font-size:18px'>●</span>  Prix eleve<br>"
            "<span style='color:#dc8000;font-size:18px'>●</span>  Prix moyen<br>"
            "<span style='color:#28b432;font-size:18px'>●</span>  Prix bas<br>"
            "<small style='color:#888'>Taille proportionnelle au prix<br>Clic pour le detail</small>"
            "</div>", unsafe_allow_html=True
        )
        if not_found:
            with st.expander(f"{len(not_found)} zones sans GPS"):
                st.write("\n".join(sorted(not_found)[:40]))

    with col_map:
        has_outside = (df_sample["lat"] > 7.0).any() if not df_sample.empty else False
        if zone_sel != "Toutes" and not df_sample.empty:
            clat, clon, zoom = float(df_sample.iloc[0]["lat"]), float(df_sample.iloc[0]["lon"]), 13
        elif has_outside:
            clat, clon, zoom = 8.0, 1.1, 7
        else:
            clat, clon, zoom = 6.1550, 1.2200, 12

        m = folium.Map(location=[clat, clon], zoom_start=zoom, tiles="CartoDB positron")

        if not df_sample.empty:
            pmin, pmax = df_sample["prix"].min(), df_sample["prix"].max()
            pr = max(pmax - pmin, 1)
            seuil_bas  = pmin + pr * 0.33
            seuil_haut = pmin + pr * 0.66

            for _, row in df_sample.iterrows():
                ratio = (row["prix"] - pmin) / pr
                if ratio <= 0.5:
                    r, g, b = int(255*ratio*2), 180, 50
                else:
                    r, g, b = 220, int(180*(1-(ratio-0.5)*2)), 50
                couleur = f"#{r:02x}{g:02x}{b:02x}"
                niveau = "Bas" if ratio <= 0.33 else ("Moyen" if ratio <= 0.66 else "Eleve")

                folium.CircleMarker(
                    location=[row["lat"], row["lon"]],
                    radius=8 + ratio * 12,
                    color=couleur, fill=True, fill_color=couleur,
                    fill_opacity=0.82, weight=1.5,
                    popup=folium.Popup(
                        f"<div style='font-family:Arial;padding:6px;min-width:180px'>"
                        f"<b>{row['zone'].title()}</b><hr style='margin:4px 0'>"
                        f"<b style='color:{couleur}'>{niveau}</b><br>"
                        f"Prix : <b>{row['prix']:,.0f} FCFA/m2</b></div>",
                        max_width=210
                    ),
                    tooltip=f"{row['zone'].title()} — {niveau} — {row['prix']:,.0f} FCFA/m2"
                ).add_to(m)

            m.get_root().html.add_child(folium.Element(f"""
            <div style="position:fixed;bottom:30px;left:30px;z-index:1000;
                        background:white;padding:12px 16px;border-radius:8px;
                        border:1px solid #ddd;font-family:Arial;font-size:12px;
                        box-shadow:2px 3px 6px rgba(0,0,0,0.15)">
              <b>Prix moyen / m2</b><br><br>
              <span style="color:#dc3200;font-size:16px">●</span>
                Eleve &gt; {seuil_haut:,.0f} FCFA<br>
              <span style="color:#dc8000;font-size:16px">●</span>
                Moyen {seuil_bas:,.0f}–{seuil_haut:,.0f}<br>
              <span style="color:#28b432;font-size:16px">●</span>
                Bas &lt; {seuil_bas:,.0f} FCFA
            </div>"""))
        else:
            st.warning("Aucun point a afficher — ouvre le panneau 'zones sans GPS' pour calibrer.")

        st_folium(m, width=None, height=500, use_container_width=True)
        st.caption(f"Vert = moins cher  |  Orange = intermediaire  |  Rouge = plus cher  |  {len(df_sample)} zones affichees")
    st.divider()

    # Top 10 / Moins chers
    st.subheader("Biens les plus chers vs moins chers")
    ct, cb = st.columns(2)
    with ct:
        st.markdown("#### Top 10 — Plus chers")
        top10 = df_ann.nlargest(10, "prix_m2")[["titre","zone","type_bien","prix_m2","source"]].reset_index(drop=True)
        top10["prix_m2"] = top10["prix_m2"].apply(lambda x: f"{x:,.0f} FCFA")
        st.dataframe(top10, use_container_width=True, hide_index=True)
    with cb:
        st.markdown("#### Top 10 — Moins chers")
        bot10 = df_ann[df_ann["prix_m2"] > 100].nsmallest(10, "prix_m2")[["titre","zone","type_bien","prix_m2","source"]].reset_index(drop=True)
        bot10["prix_m2"] = bot10["prix_m2"].apply(lambda x: f"{x:,.0f} FCFA")
        st.dataframe(bot10, use_container_width=True, hide_index=True)
    st.divider()

    # Indice
    st.subheader("Indice ID Immobilier — par type de bien")
    if not df_ind.empty:
        prix_global = df_ind["prix_moyen_m2"].mean()
        df_ic = df_ind.groupby(["type_bien","type_offre"]).agg(
            prix_moyen_m2=("prix_moyen_m2","mean"), nb_annonces=("nb_annonces","sum")
        ).reset_index()
        df_ic["indice"] = (df_ic["prix_moyen_m2"] / prix_global * 100).round(2)
        df_ic["tendance"] = df_ic["indice"].apply(
            lambda x: "Au-dessus" if x > 105 else ("En-dessous" if x < 95 else "Dans la moyenne"))
        df_ic = df_ic.sort_values("indice", ascending=False)
        ci1, ci2 = st.columns([2, 1])
        with ci1:
            fig_ind = px.bar(df_ic, x="type_bien", y="indice", color="tendance",
                             color_discrete_map={"Au-dessus":"#E74C3C","Dans la moyenne":"#F39C12","En-dessous":"#27AE60"},
                             barmode="group",
                             facet_col="type_offre" if offre_sel == "Tous" else None,
                             labels={"indice":"Indice (Base 100)","type_bien":"Type de bien"},
                             title="Indice par type de bien (Base 100 = prix moyen global)",
                             text="indice")
            fig_ind.add_hline(y=100, line_dash="dash", line_color="gray", annotation_text="Base 100")
            fig_ind.update_traces(texttemplate="%{text:.1f}", textposition="outside")
            st.plotly_chart(fig_ind, use_container_width=True)
        with ci2:
            st.markdown("#### Detail")
            df_disp = df_ic[["type_bien","type_offre","prix_moyen_m2","indice","nb_annonces"]].copy()
            df_disp["prix_moyen_m2"] = df_disp["prix_moyen_m2"].apply(lambda x: f"{x:,.0f}")
            df_disp["indice"] = df_disp["indice"].apply(lambda x: f"{x:.1f}")
            st.dataframe(df_disp, use_container_width=True, hide_index=True)
        st.caption(f"Base = prix moyen global : {prix_global:,.0f} FCFA/m2")
    else:
        st.info("Pas assez de donnees pour calculer l'indice.")
    st.divider()

    # Tableau comparatif zones
    st.subheader("Tableau comparatif des zones")
    cols_ok = [c for c in ["zone","type_bien","type_offre","prix_moyen_m2",
                            "prix_median_m2","nombre_annonces","ecart_valeur_venale"]
               if c in df_f.columns]
    st.dataframe(df_f[cols_ok].sort_values("prix_moyen_m2", ascending=False),
                 use_container_width=True, hide_index=True)

    # Exports sidebar
    st.sidebar.divider()
    st.sidebar.download_button("Exporter statistiques (CSV)",
        data=df_f.to_csv(index=False).encode("utf-8"),
        file_name="statistiques.csv", mime="text/csv")
    st.sidebar.download_button("Exporter annonces (CSV)",
        data=df_ann.to_csv(index=False).encode("utf-8"),
        file_name="annonces.csv", mime="text/csv")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — TOUTES LES ANNONCES
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Toutes les annonces":

    st.title("Tableau d'ensemble des annonces")
    st.markdown("Toutes les annonces validees et inserees dans la base de donnees.")
    st.divider()

    df = df_annonces.copy()

    # Filtres sidebar
    st.sidebar.subheader("Filtres")

    zones = sorted(df["zone"].dropna().unique().tolist())
    zone_sel2 = st.sidebar.selectbox("Zone", ["Toutes"] + zones)

    types2 = sorted(df["type_bien"].dropna().unique().tolist())
    type_sel2 = st.sidebar.multiselect("Type de bien", types2, default=types2)
    if not type_sel2:
        type_sel2 = types2

    offre_sel2 = st.sidebar.radio("Type d'offre", ["Tous", "VENTE", "LOCATION"])
    periodes2 = sorted(df["periode"].dropna().unique().tolist()) if "periode" in df else []
    periode_sel2 = st.sidebar.selectbox("Période", ["Toutes"] + periodes2)

    sources2 = sorted(df["source"].dropna().unique().tolist())
    source_sel2 = st.sidebar.multiselect("Source", sources2, default=sources2)
    if not source_sel2:
        source_sel2 = sources2

    pmin_val = float(df["prix"].min()) if not df.empty else 0.0
    pmax_val = float(df["prix"].max()) if not df.empty else 1e10
    prix_range = st.sidebar.slider("Fourchette de prix (FCFA)",
                                   min_value=pmin_val, max_value=pmax_val,
                                   value=(pmin_val, pmax_val), format="%.0f")

    search = st.sidebar.text_input("Recherche dans le titre", placeholder="ex: villa, terrain...")

    # Appliquer filtres
    df2 = df.copy()
    if zone_sel2 != "Toutes":
        df2 = df2[df2["zone"] == zone_sel2]
    df2 = df2[df2["type_bien"].isin(type_sel2)]
    df2 = df2[df2["source"].isin(source_sel2)]
    if offre_sel2 != "Tous":
        df2 = df2[df2["type_offre"] == offre_sel2]
    if periode_sel2 != "Toutes":
        df2 = df2[df2["periode"] == periode_sel2]
    df2 = df2[(df2["prix"] >= prix_range[0]) & (df2["prix"] <= prix_range[1])]
    if search:
        df2 = df2[df2["titre"].str.contains(search, case=False, na=False)]

    # KPIs
    ka1, ka2, ka3, ka4 = st.columns(4)
    ka1.metric("Annonces affichees", f"{len(df2):,}")
    ka2.metric("Prix moyen",         f"{df2['prix'].mean():,.0f} FCFA"    if not df2.empty else "—")
    ka3.metric("Prix moyen / m2",    f"{df2['prix_m2'].mean():,.0f} FCFA" if not df2.empty else "—")
    ka4.metric("Surface moyenne",    f"{df2['surface_m2'].mean():,.0f} m2" if not df2.empty else "—")
    st.divider()

    # Tri
    cols_map = {"titre":"Titre","zone":"Zone","type_bien":"Type de bien",
                "type_offre":"Type d'offre","prix":"Prix (FCFA)",
                "prix_m2":"Prix / m2 (FCFA)","surface_m2":"Surface (m2)",
                "source":"Source","date_annonce":"Date"}
    cols_dispo = [c for c in cols_map if c in df2.columns]

    tc1, tc2 = st.columns([2, 1])
    with tc1:
        tri_opts = [cols_map[c] for c in ["prix","prix_m2","surface_m2"] if c in df2.columns]
        col_tri = st.selectbox("Trier par", tri_opts)
    with tc2:
        ordre = st.radio("Ordre", ["Decroissant", "Croissant"], horizontal=True)

    col_tri_raw = {v: k for k, v in cols_map.items()}.get(col_tri, "prix")
    if col_tri_raw in df2.columns:
        df2 = df2.sort_values(col_tri_raw, ascending=(ordre == "Croissant"))

    # Tableau
    df_aff = df2[cols_dispo].rename(columns=cols_map).copy()
    for col in ["Prix (FCFA)", "Prix / m2 (FCFA)", "Surface (m2)"]:
        if col in df_aff.columns:
            df_aff[col] = df_aff[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "—")

    st.dataframe(df_aff, use_container_width=True, hide_index=True, height=600)
    st.caption(f"{len(df2):,} annonces affichees sur {len(df):,} au total")

    # Exports
    st.divider()
    ex1, ex2 = st.columns(2)
    with ex1:
        st.download_button("Exporter la selection (CSV)",
            data=df2.to_csv(index=False).encode("utf-8"),
            file_name="annonces_selection.csv", mime="text/csv")
    with ex2:
        st.download_button("Exporter tout (CSV)",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="annonces_complet.csv", mime="text/csv")


# ── Footer commun ──────────────────────────────────────────────────────────────
st.divider()
st.caption("Projet ID Immobilier — Cours Introduction Big Data 2026  |  Donnees : ImmoAsk, Facebook, CoinAfrique, OTR Togo")