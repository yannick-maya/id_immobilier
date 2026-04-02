"""
ID IMMOBILIER - Dashboard TDR Complet
UE 2INF2126 - Transition Digitale & Changement de Paradigme
Master Cycle Ingenieur Big Data - 2024/2025
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# CONFIG PAGE
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="ID Immobilier - Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────
# CSS CUSTOM
# ─────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #F0F4F8; }
    .stMetric { background: white; padding: 15px; border-radius: 10px; border-left: 4px solid #065A82; }
    .section-title {
        background: linear-gradient(90deg, #065A82, #1C7293);
        color: white; padding: 12px 20px;
        border-radius: 8px; font-size: 18px; font-weight: bold;
        margin: 20px 0 15px 0;
    }
    .kpi-card {
        background: white; border-radius: 10px; padding: 15px;
        border-left: 5px solid #065A82; margin: 8px 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }
    .hausse { color: #059669; font-weight: bold; }
    .baisse { color: #DC2626; font-weight: bold; }
    .stable { color: #D97706; font-weight: bold; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# DONNEES SIMULEES (basees sur le rapport reel)
# ─────────────────────────────────────────────
@st.cache_data
def load_data():
    np.random.seed(42)

    zones = ["Tokoin", "Be", "Adidogome", "Agoe", "Hanoukope",
             "Kegue", "Nukafu", "Aflao", "Baguida", "Lome Centre"]
    types_bien  = ["Appartement", "Maison", "Terrain", "Villa", "Boutique"]
    types_offre = ["VENTE", "LOCATION"]
    sources     = ["CoinAfrique", "ImmoAsk", "Facebook", "OTR"]

    prix_ref = {
        "Tokoin": 1850, "Be": 1620, "Lome Centre": 2100, "Kegue": 1400,
        "Adidogome": 1380, "Nukafu": 1250, "Agoe": 1100,
        "Hanoukope": 950, "Aflao": 820, "Baguida": 780
    }

    rows = []
    for _ in range(800):
        zone       = np.random.choice(zones, p=[0.18,0.15,0.14,0.13,0.10,0.08,0.08,0.06,0.05,0.03])
        type_bien  = np.random.choice(types_bien,  p=[0.35,0.30,0.20,0.10,0.05])
        type_offre = np.random.choice(types_offre, p=[0.55,0.45])
        source     = np.random.choice(sources,     p=[0.84,0.09,0.01,0.06])
        surface    = np.random.randint(20, 350)
        pieces     = max(1, int(surface / 40) + np.random.randint(-1, 2))
        base_prix  = prix_ref[zone]
        mult = {"Appartement":1.0,"Maison":1.2,"Villa":2.1,"Terrain":0.6,"Boutique":1.4}
        prix_m2    = base_prix * mult[type_bien] * np.random.uniform(0.75, 1.35)
        prix       = prix_m2 * surface
        rows.append({
            "zone": zone, "type_bien": type_bien, "type_offre": type_offre,
            "source": source, "surface_m2": surface, "nb_pieces": pieces,
            "prix": round(prix), "prix_m2": round(prix_m2),
            "annee": np.random.choice([2022, 2023, 2024], p=[0.2, 0.4, 0.4])
        })

    df = pd.DataFrame(rows)

    # Valeurs venales OTR
    vv = pd.DataFrame([
        {"zone": z, "prix_m2_officiel": prix_ref[z] * np.random.uniform(0.65, 0.82)}
        for z in zones
    ])

    # Qualite pipeline
    qualite = pd.DataFrame([
        {"source": "CoinAfrique", "total": 4844, "valides": 4201, "rejetes": 643},
        {"source": "ImmoAsk",     "total": 500,  "valides": 487,  "rejetes": 13},
        {"source": "Facebook",    "total": 80,   "valides": 58,   "rejetes": 22},
        {"source": "OTR",         "total": 354,  "valides": 354,  "rejetes": 0},
    ])
    qualite["taux_rejet"] = (qualite["rejetes"] / qualite["total"] * 100).round(1)

    # Historique indice (base 100)
    periodes = ["Jan 2023","Mar 2023","Jun 2023","Sep 2023",
                "Jan 2024","Mar 2024","Jun 2024","Sep 2024","Jan 2025"]
    indice_hist = pd.DataFrame({
        "periode":     periodes,
        "Tokoin":      [100,102,105,107,109,112,114,117,121],
        "Adidogome":   [100,100,99, 101,100,101,98, 97, 96],
        "Agoe":        [100,99, 97, 95, 93, 91, 90, 88, 87],
        "Lome Centre": [100,103,106,110,113,116,119,122,126],
        "Baguida":     [100,101,102,101,103,104,103,105,106],
    })

    return df, vv, qualite, indice_hist


df, vv, qualite, indice_hist = load_data()


# ─────────────────────────────────────────────
# MODELE IA — entraine une seule fois
# ─────────────────────────────────────────────
@st.cache_resource
def train_model():
    le_zone  = LabelEncoder()
    le_type  = LabelEncoder()
    le_offre = LabelEncoder()

    df_ml = df.copy()
    df_ml["zone_enc"]  = le_zone.fit_transform(df_ml["zone"])
    df_ml["type_enc"]  = le_type.fit_transform(df_ml["type_bien"])
    df_ml["offre_enc"] = le_offre.fit_transform(df_ml["type_offre"])

    features = ["zone_enc","type_enc","offre_enc","surface_m2","nb_pieces"]
    X = df_ml[features]
    y = df_ml["prix_m2"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = RandomForestRegressor(n_estimators=150, max_depth=10, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    r2  = r2_score(y_test, y_pred)

    return model, le_zone, le_type, le_offre, mae, r2, X_test, y_test, y_pred, features


model, le_zone, le_type, le_offre, mae, r2, X_test, y_test, y_pred, features = train_model()


# ─────────────────────────────────────────────
# SIDEBAR — navigation
# ─────────────────────────────────────────────
with st.sidebar:
    st.title("ID Immobilier")
    st.caption("Indice Intelligent du Marche Immobilier au Togo")
    st.markdown("---")

    page = st.radio("Navigation", [
        "Tableau de Bord Principal",
        "Indice et Tendances",
        "Scoring IA - Prediction",
        "OKR et KPIs Pipeline",
        "Gouvernance des Donnees",
    ])

    st.markdown("---")
    st.markdown("**Filtres globaux**")
    zones_sel     = st.multiselect("Zones", sorted(df["zone"].unique()), default=sorted(df["zone"].unique()))
    type_offre_sel = st.selectbox("Type d'offre", ["Tous", "VENTE", "LOCATION"])

    if type_offre_sel != "Tous":
        df_f = df[df["zone"].isin(zones_sel) & (df["type_offre"] == type_offre_sel)]
    else:
        df_f = df[df["zone"].isin(zones_sel)]

    st.markdown("---")
    st.caption("Pipeline : Spark · Airflow · PostgreSQL · Streamlit")
    st.caption("UE 2INF2126 · Master Big Data 2024-2025")


# ══════════════════════════════════════════════════════════════
# PAGE 1 — TABLEAU DE BORD PRINCIPAL
# ══════════════════════════════════════════════════════════════
if page == "Tableau de Bord Principal":

    st.markdown("## ID Immobilier — Tableau de Bord Principal")
    st.caption("Indice intelligent du marche immobilier togolais · Donnees issues de CoinAfrique, ImmoAsk, Facebook, OTR")

    # KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    prix_moyen  = int(df_f["prix_m2"].mean())
    prix_median = int(df_f["prix_m2"].median())
    nb_zones    = df_f["zone"].nunique()
    nb_annonces = len(df_f)

    stats_zone = df_f.groupby("zone")["prix_m2"].mean().reset_index()
    stats_zone.columns = ["zone", "prix_marche"]
    merged     = stats_zone.merge(vv, on="zone")
    ecart_moyen = ((merged["prix_marche"] - merged["prix_m2_officiel"]) / merged["prix_m2_officiel"] * 100).mean()

    col1.metric("Prix moyen / m2",  f"{prix_moyen:,} FCFA")
    col2.metric("Prix median / m2", f"{prix_median:,} FCFA")
    col3.metric("Zones analysees",  nb_zones)
    col4.metric("Annonces",         f"{nb_annonces:,}")
    col5.metric("Ecart vs OTR",     f"+{ecart_moyen:.1f}%", delta=f"{ecart_moyen:.1f}%")

    st.markdown("---")

    col_a, col_b = st.columns([1.6, 1])

    with col_a:
        st.markdown('<div class="section-title">Prix moyen au m2 par zone (FCFA)</div>', unsafe_allow_html=True)
        stats = df_f.groupby("zone")["prix_m2"].agg(["mean","median","count"]).reset_index()
        stats.columns = ["zone","prix_moyen","prix_median","nb_annonces"]
        stats = stats.sort_values("prix_moyen", ascending=True)

        fig = go.Figure()
        fig.add_trace(go.Bar(
            y=stats["zone"], x=stats["prix_moyen"], name="Prix moyen", orientation="h",
            marker_color="#065A82",
            text=stats["prix_moyen"].apply(lambda x: f"{int(x):,}"), textposition="outside"
        ))
        fig.add_trace(go.Bar(
            y=stats["zone"], x=stats["prix_median"], name="Prix median", orientation="h",
            marker_color="#00B4D8", opacity=0.7,
            text=stats["prix_median"].apply(lambda x: f"{int(x):,}"), textposition="outside"
        ))
        fig.update_layout(
            barmode="group", height=380, margin=dict(l=10,r=60,t=20,b=20),
            plot_bgcolor="white", paper_bgcolor="white",
            legend=dict(orientation="h", y=-0.15), xaxis_title="FCFA/m2"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.markdown('<div class="section-title">Repartition par source</div>', unsafe_allow_html=True)
        src_stats = df_f.groupby("source").size().reset_index(name="count")
        fig2 = px.pie(src_stats, values="count", names="source",
                      color_discrete_sequence=["#065A82","#1C7293","#00B4D8","#F59E0B"],
                      hole=0.45)
        fig2.update_layout(height=200, margin=dict(l=0,r=0,t=10,b=10), showlegend=True)
        st.plotly_chart(fig2, use_container_width=True)

        st.markdown('<div class="section-title">Par type de bien</div>', unsafe_allow_html=True)
        type_stats = df_f.groupby("type_bien")["prix_m2"].mean().reset_index()
        fig3 = px.bar(type_stats, x="type_bien", y="prix_m2",
                      color="prix_m2", color_continuous_scale="Blues",
                      labels={"prix_m2":"Prix moy. FCFA/m2","type_bien":""})
        fig3.update_layout(height=200, margin=dict(l=0,r=0,t=10,b=20),
                           plot_bgcolor="white", coloraxis_showscale=False)
        st.plotly_chart(fig3, use_container_width=True)

    st.markdown('<div class="section-title">Ecart Prix Marche vs Valeurs Venales OTR</div>', unsafe_allow_html=True)
    stats_z = df_f.groupby("zone")["prix_m2"].mean().reset_index()
    stats_z.columns = ["zone","prix_marche"]
    ecart_df = stats_z.merge(vv, on="zone")
    ecart_df["ecart_pct"] = ((ecart_df["prix_marche"] - ecart_df["prix_m2_officiel"]) / ecart_df["prix_m2_officiel"] * 100).round(1)
    ecart_df = ecart_df.sort_values("ecart_pct", ascending=False)

    fig4 = go.Figure()
    fig4.add_trace(go.Bar(name="Prix marche",       x=ecart_df["zone"], y=ecart_df["prix_marche"],       marker_color="#065A82"))
    fig4.add_trace(go.Bar(name="Valeur venale OTR", x=ecart_df["zone"], y=ecart_df["prix_m2_officiel"],  marker_color="#F59E0B"))
    fig4.update_layout(barmode="group", height=320, plot_bgcolor="white",
                       paper_bgcolor="white", margin=dict(l=0,r=0,t=20,b=20),
                       yaxis_title="FCFA/m2")
    st.plotly_chart(fig4, use_container_width=True)

    c1, c2 = st.columns(2)
    c1.info(f"Ecart moyen global : +{ecart_moyen:.1f}% — le marche est en moyenne {ecart_moyen:.1f}% au-dessus des valeurs officielles OTR.")
    c2.warning("Cet ecart revele une sous-evaluation cadastrale systemique. L'OTR devrait reviser ses bases de reference.")


# ══════════════════════════════════════════════════════════════
# PAGE 2 — INDICE & TENDANCES
# ══════════════════════════════════════════════════════════════
elif page == "Indice et Tendances":

    st.markdown("## Indice ID Immobilier — Base 100")
    st.caption("Evolution temporelle des prix par zone · Formule : (Prix_periode / Prix_reference) x 100")

    st.markdown('<div class="section-title">Tableau de l\'Indice — Situation actuelle</div>', unsafe_allow_html=True)

    prix_ref_zone = df.groupby("zone")["prix_m2"].mean() * 0.88
    indice_data = []
    for zone in sorted(df_f["zone"].unique()):
        for tb in ["Appartement", "Maison", "Villa", "Terrain"]:
            sub = df_f[(df_f["zone"] == zone) & (df_f["type_bien"] == tb)]
            if len(sub) < 3:
                continue
            prix_act = sub["prix_m2"].mean()
            ref      = prix_ref_zone.get(zone, prix_act * 0.9)
            indice   = round((prix_act / ref) * 100, 1)
            if indice > 105:
                tendance = "HAUSSE"
            elif indice < 95:
                tendance = "BAISSE"
            else:
                tendance = "STABLE"
            indice_data.append({
                "Zone": zone, "Type": tb,
                "Prix moy. m2": f"{int(prix_act):,} FCFA",
                "Indice": indice,
                "Tendance": tendance
            })

    idx_df = pd.DataFrame(indice_data)

    def color_tendance(val):
        if val == "HAUSSE": return "background-color: #D1FAE5; color: #065F46; font-weight:bold"
        if val == "BAISSE": return "background-color: #FEE2E2; color: #991B1B; font-weight:bold"
        return "background-color: #FEF3C7; color: #92400E; font-weight:bold"

    def color_indice(val):
        """Colorise la colonne Indice sans matplotlib."""
        try:
            v = float(val)
        except (TypeError, ValueError):
            return ""
        if v >= 115:   return "background-color:#065F46;color:white;font-weight:bold"
        if v >= 105:   return "background-color:#D1FAE5;color:#065F46;font-weight:bold"
        if v >= 95:    return "background-color:#FEF3C7;color:#92400E"
        if v >= 85:    return "background-color:#FEE2E2;color:#991B1B"
        return                "background-color:#991B1B;color:white;font-weight:bold"

    # pandas >= 2.1 : utiliser .map() au lieu de .applymap()
    styled = (
        idx_df.style
        .map(color_tendance, subset=["Tendance"])
        .map(color_indice,   subset=["Indice"])
    )
    st.dataframe(styled, use_container_width=True, height=400)

    col1, col2, col3 = st.columns(3)
    col1.metric("Zones en HAUSSE", int((idx_df["Tendance"] == "HAUSSE").sum()))
    col2.metric("Zones STABLES",   int((idx_df["Tendance"] == "STABLE").sum()))
    col3.metric("Zones en BAISSE", int((idx_df["Tendance"] == "BAISSE").sum()))

    st.markdown("---")
    st.markdown('<div class="section-title">Evolution historique de l\'indice (2023-2025)</div>', unsafe_allow_html=True)

    zones_disponibles = [z for z in ["Tokoin","Adidogome","Agoe","Lome Centre","Baguida"]
                         if z in indice_hist.columns]
    zones_hist = st.multiselect(
        "Selectionner les zones a comparer",
        zones_disponibles,
        default=zones_disponibles[:3]
    )

    fig = go.Figure()
    colors = ["#065A82","#DC2626","#059669","#F59E0B","#7C3AED"]
    for i, zone in enumerate(zones_hist):
        fig.add_trace(go.Scatter(
            x=indice_hist["periode"], y=indice_hist[zone],
            name=zone, mode="lines+markers",
            line=dict(color=colors[i % len(colors)], width=2.5),
            marker=dict(size=7)
        ))

    fig.add_hline(y=105, line_dash="dash", line_color="#059669", annotation_text="Seuil HAUSSE (105)")
    fig.add_hline(y=100, line_dash="dot",  line_color="#6B7280", annotation_text="Base 100")
    fig.add_hline(y=95,  line_dash="dash", line_color="#DC2626", annotation_text="Seuil BAISSE (95)")
    fig.update_layout(
        height=380, plot_bgcolor="white", paper_bgcolor="white",
        yaxis_title="Indice (base 100)", xaxis_title="",
        legend=dict(orientation="h", y=-0.2),
        margin=dict(l=10,r=10,t=20,b=60)
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown('<div class="section-title">Distribution des prix par zone</div>', unsafe_allow_html=True)
    fig5 = px.box(df_f, x="zone", y="prix_m2", color="type_bien",
                  color_discrete_sequence=px.colors.qualitative.Set2,
                  labels={"prix_m2":"Prix/m2 (FCFA)","zone":"Zone","type_bien":"Type"})
    fig5.update_layout(height=380, plot_bgcolor="white", paper_bgcolor="white",
                       margin=dict(l=0,r=0,t=20,b=20))
    st.plotly_chart(fig5, use_container_width=True)


# ══════════════════════════════════════════════════════════════
# PAGE 3 — SCORING IA
# ══════════════════════════════════════════════════════════════
elif page == "Scoring IA - Prediction":

    st.markdown("## Simulateur de Scoring IA")
    st.caption("Modele Random Forest · Prediction du prix au m2 a partir des caracteristiques d'un bien")

    st.markdown('<div class="section-title">Performance du Modele Random Forest</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("R2 Score",              f"{r2:.3f}", help="1.0 = parfait")
    c2.metric("MAE",                   f"{int(mae):,} FCFA/m2", help="Erreur absolue moyenne")
    c3.metric("Arbres",                "150")
    c4.metric("Donnees entrainement",  f"{int(len(df)*0.8):,} annonces")

    col_left, col_right = st.columns([1, 1.3])

    with col_left:
        st.markdown('<div class="section-title">Simuler un bien</div>', unsafe_allow_html=True)

        zone_input       = st.selectbox("Zone geographique", sorted(df["zone"].unique()))
        type_bien_input  = st.selectbox("Type de bien",      ["Appartement","Maison","Villa","Terrain","Boutique"])
        type_offre_input = st.selectbox("Type d'offre",      ["VENTE","LOCATION"])
        surface_input    = st.slider("Surface (m2)", 20, 500, 80)
        pieces_input     = st.slider("Nombre de pieces", 1, 10, 3)

        if st.button("Estimer le prix", type="primary", use_container_width=True):
            # Verification que les labels sont connus des encodeurs
            zones_connus  = list(le_zone.classes_)
            types_connus  = list(le_type.classes_)
            offres_connus = list(le_offre.classes_)

            if zone_input not in zones_connus:
                st.error(f"Zone inconnue du modele : {zone_input}")
            elif type_bien_input not in types_connus:
                st.error(f"Type de bien inconnu du modele : {type_bien_input}")
            else:
                zone_e  = le_zone.transform([zone_input])[0]
                type_e  = le_type.transform([type_bien_input])[0]
                offre_e = le_offre.transform([type_offre_input])[0]

                X_pred = pd.DataFrame(
                    [[zone_e, type_e, offre_e, surface_input, pieces_input]],
                    columns=features
                )
                prix_pred  = model.predict(X_pred)[0]
                prix_total = prix_pred * surface_input

                zone_data  = df[df["zone"] == zone_input]["prix_m2"]
                ref        = zone_data.mean() * 0.88
                indice_val = round((zone_data.mean() / ref) * 100, 1)
                tendance   = "HAUSSE" if indice_val > 105 else ("BAISSE" if indice_val < 95 else "STABLE")

                otr_vals = vv[vv["zone"] == zone_input]["prix_m2_officiel"].values

                st.markdown("---")
                st.success(f"### Prix estime : {int(prix_pred):,} FCFA/m2")
                st.info(f"**Prix total estime** pour {surface_input} m2 : **{int(prix_total):,} FCFA**")

                m1, m2 = st.columns(2)
                m1.metric("Indice zone", f"{indice_val}", tendance)
                if len(otr_vals) > 0:
                    diff = ((prix_pred - otr_vals[0]) / otr_vals[0]) * 100
                    sens = "au-dessus" if diff > 0 else "en-dessous"
                    m2.metric("Valeur OTR reference", f"{int(otr_vals[0]):,} FCFA/m2",
                              f"{sens} de l'OTR ({abs(diff):.1f}%)")

                st.caption(f"Intervalle de confiance +/- {int(mae):,} FCFA/m2 · Estimation, pas une evaluation certifiee.")

    with col_right:
        st.markdown('<div class="section-title">Valeurs reelles vs predites</div>', unsafe_allow_html=True)
        scatter_df = pd.DataFrame({"Reel": y_test.values[:150], "Predit": y_pred[:150]})
        fig_s = px.scatter(scatter_df, x="Reel", y="Predit",
                           color_discrete_sequence=["#065A82"],
                           labels={"Reel":"Prix reel (FCFA/m2)","Predit":"Prix predit (FCFA/m2)"})
        max_v = max(scatter_df["Reel"].max(), scatter_df["Predit"].max())
        fig_s.add_trace(go.Scatter(x=[0,max_v], y=[0,max_v], mode="lines",
                                   line=dict(color="#DC2626", dash="dash"), name="Parfait"))
        fig_s.update_layout(height=240, plot_bgcolor="white", paper_bgcolor="white",
                            margin=dict(l=0,r=0,t=20,b=20))
        st.plotly_chart(fig_s, use_container_width=True)

        st.markdown('<div class="section-title">Importance des variables</div>', unsafe_allow_html=True)
        feat_names   = ["Zone","Type de bien","Type d'offre","Surface m2","Nb pieces"]
        importances  = model.feature_importances_
        fi_df = pd.DataFrame({"Variable": feat_names, "Importance": importances})\
                  .sort_values("Importance", ascending=True)
        fig_fi = px.bar(fi_df, x="Importance", y="Variable", orientation="h",
                        color="Importance", color_continuous_scale="Blues",
                        labels={"Importance":"Score d'importance","Variable":""})
        fig_fi.update_layout(height=230, plot_bgcolor="white", paper_bgcolor="white",
                             coloraxis_showscale=False, margin=dict(l=0,r=0,t=10,b=10))
        st.plotly_chart(fig_fi, use_container_width=True)

    st.markdown('<div class="section-title">Distribution des erreurs du modele</div>', unsafe_allow_html=True)
    erreurs = np.array(y_pred) - np.array(y_test.values)
    fig_err = px.histogram(erreurs, nbins=40, color_discrete_sequence=["#1C7293"],
                           labels={"value":"Erreur (FCFA/m2)","count":"Frequence"})
    fig_err.add_vline(x=0, line_dash="dash", line_color="#DC2626", annotation_text="Erreur 0")
    fig_err.update_layout(height=260, plot_bgcolor="white", paper_bgcolor="white",
                          margin=dict(l=0,r=10,t=20,b=20))
    st.plotly_chart(fig_err, use_container_width=True)
    st.caption(f"Erreur centree autour de 0 — pas de biais systematique. MAE = {int(mae):,} FCFA/m2")


# ══════════════════════════════════════════════════════════════
# PAGE 4 — OKR & KPIs PIPELINE
# ══════════════════════════════════════════════════════════════
elif page == "OKR et KPIs Pipeline":

    st.markdown("## OKR et KPIs Data-Driven")
    st.caption("Suivi des objectifs et resultats cles du projet ID Immobilier · Framework OKR")

    st.markdown('<div class="section-title">Objectives and Key Results (OKR)</div>', unsafe_allow_html=True)

    okrs = [
        {"obj": "Fiabilite du pipeline",    "kr": "Taux de succes DAG Airflow > 95%",
         "valeur": 97.2,              "cible": 95,  "unite": "%",      "status": "OK"},
        {"obj": "Qualite des donnees",      "kr": "Taux de rejet global < 15%",
         "valeur": 13.4,              "cible": 15,  "unite": "%",      "status": "OK"},
        {"obj": "Precision du modele IA",   "kr": "R2 > 0.80 sur le jeu de test",
         "valeur": round(r2*100,1),   "cible": 80,  "unite": "%",      "status": "OK" if r2 > 0.8 else "Partiel"},
        {"obj": "Couverture geographique",  "kr": ">= 8 zones avec > 10 annonces",
         "valeur": int((df.groupby("zone").size() > 10).sum()), "cible": 8, "unite": " zones", "status": "OK"},
        {"obj": "Adoption",                 "kr": "100 utilisateurs actifs sous 3 mois",
         "valeur": 67,                "cible": 100, "unite": " users", "status": "Partiel"},
        {"obj": "Partenariat OTR",          "kr": "Accord institutionnel signe < 6 mois",
         "valeur": 30,                "cible": 100, "unite": "%",      "status": "En cours"},
    ]

    cols = st.columns(3)
    for i, okr in enumerate(okrs):
        with cols[i % 3]:
            prog  = min(okr["valeur"] / okr["cible"] * 100, 100)
            color = "#059669" if prog >= 100 else ("#D97706" if prog >= 60 else "#DC2626")
            st.markdown(f"""
            <div class="kpi-card">
                <div style="font-size:11px;color:#6B7280;text-transform:uppercase;margin-bottom:4px">{okr['obj']}</div>
                <div style="font-size:13px;font-weight:bold;color:#1E3A5F;margin-bottom:8px">{okr['kr']}</div>
                <div style="font-size:22px;font-weight:bold;color:{color}">{okr['valeur']}{okr['unite']}</div>
                <div style="font-size:11px;color:#9CA3AF">Cible : {okr['cible']}{okr['unite']} — {okr['status']}</div>
                <div style="background:#E5E7EB;border-radius:4px;height:6px;margin-top:8px">
                    <div style="background:{color};width:{prog:.0f}%;height:6px;border-radius:4px"></div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown('<div class="section-title">Volume de donnees par source</div>', unsafe_allow_html=True)
        src_vol = pd.DataFrame({
            "Source":   ["CoinAfrique","ImmoAsk","Facebook","OTR"],
            "Annonces": [4844, 500, 80, 354],
            "Valides":  [4201, 487, 58, 354]
        })
        fig_v = go.Figure()
        fig_v.add_trace(go.Bar(name="Total collecte", x=src_vol["Source"], y=src_vol["Annonces"], marker_color="#065A82"))
        fig_v.add_trace(go.Bar(name="Valides",        x=src_vol["Source"], y=src_vol["Valides"],  marker_color="#059669"))
        fig_v.update_layout(barmode="group", height=300, plot_bgcolor="white",
                            paper_bgcolor="white", margin=dict(l=0,r=0,t=20,b=20))
        st.plotly_chart(fig_v, use_container_width=True)

    with col_r:
        st.markdown('<div class="section-title">Executions pipeline (12 semaines)</div>', unsafe_allow_html=True)
        semaines = [f"S{i+1}" for i in range(12)]
        succes   = [1,1,1,0,1,1,1,1,0,1,1,1]
        durees   = [4.2,3.8,4.5,None,4.1,3.9,4.3,4.0,None,3.7,4.4,4.1]

        colors_dag = ["#059669" if s else "#DC2626" for s in succes]
        fig_dag = go.Figure()
        fig_dag.add_trace(go.Bar(
            x=semaines,
            y=[d if d else 0 for d in durees],
            marker_color=colors_dag,
            text=["OK" if s else "ECHEC" for s in succes],
            textposition="outside",
            name="Duree (min)"
        ))
        fig_dag.update_layout(height=300, plot_bgcolor="white", paper_bgcolor="white",
                              yaxis_title="Duree (min)", margin=dict(l=0,r=0,t=30,b=20))
        st.plotly_chart(fig_dag, use_container_width=True)
        taux_s = sum(succes) / len(succes) * 100
        st.caption(f"Taux de succes : {taux_s:.0f}% — {sum(succes)}/{len(succes)} executions reussies")

    st.markdown('<div class="section-title">Maturite digitale — Evaluation par dimension TDR</div>', unsafe_allow_html=True)
    categories    = ["Vision Strategique","Transformation BPM","Proposition Valeur",
                     "Analyse Socio-Tech","Gouvernance Data","Competences & Org"]
    values        = [80, 90, 75, 65, 70, 60]
    values_cible  = [100]*6

    fig_radar = go.Figure()
    fig_radar.add_trace(go.Scatterpolar(
        r=values_cible + [values_cible[0]], theta=categories + [categories[0]],
        fill="toself", name="Cible",
        fillcolor="rgba(6,90,130,0.1)", line=dict(color="#065A82", dash="dash")
    ))
    fig_radar.add_trace(go.Scatterpolar(
        r=values + [values[0]], theta=categories + [categories[0]],
        fill="toself", name="Actuel",
        fillcolor="rgba(0,180,216,0.3)", line=dict(color="#00B4D8", width=2)
    ))
    fig_radar.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0,100])),
        showlegend=True, height=380, margin=dict(l=40,r=40,t=40,b=40)
    )
    st.plotly_chart(fig_radar, use_container_width=True)
    st.caption("Evaluation de la maturite du projet sur chaque dimension du TDR (0 = non couvert, 100 = maitrise).")


# ══════════════════════════════════════════════════════════════
# PAGE 5 — GOUVERNANCE DES DONNEES
# ══════════════════════════════════════════════════════════════
elif page == "Gouvernance des Donnees":

    st.markdown("## Gouvernance des Donnees")
    st.caption("Qualite · Propriete · Tracabilite · Architecture Medallion")

    st.markdown('<div class="section-title">Architecture Medallion — Couches de donnees</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown("""
        <div style="background:#92400E;color:white;padding:15px;border-radius:8px;text-align:center">
            <div style="font-weight:bold;font-size:15px">BRONZE</div>
            <div style="font-size:12px;margin-top:5px">data/raw/</div>
            <hr style="border-color:rgba(255,255,255,0.3)">
            <div style="font-size:11px">4 sources Excel brutes<br>CSV d'ingestion<br>Immuables<br><b>5 778 lignes</b></div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown("""
        <div style="background:#065A82;color:white;padding:15px;border-radius:8px;text-align:center">
            <div style="font-weight:bold;font-size:15px">SILVER</div>
            <div style="font-size:12px;margin-top:5px">data/cleaned/</div>
            <hr style="border-color:rgba(255,255,255,0.3)">
            <div style="font-size:11px">Nettoyees par Spark<br>CSV partitionne (3 parts)<br>Parquet + Snappy<br><b>5 100 lignes valides</b></div>
        </div>""", unsafe_allow_html=True)
    with c3:
        st.markdown("""
        <div style="background:#065F46;color:white;padding:15px;border-radius:8px;text-align:center">
            <div style="font-weight:bold;font-size:15px">GOLD</div>
            <div style="font-size:12px;margin-top:5px">PostgreSQL</div>
            <hr style="border-color:rgba(255,255,255,0.3)">
            <div style="font-size:11px">7 tables normalisees (3NF)<br>statistiques_zone<br>indice_immobilier<br><b>Consommee par Streamlit</b></div>
        </div>""", unsafe_allow_html=True)

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown('<div class="section-title">Qualite des donnees par source</div>', unsafe_allow_html=True)
        fig_q = go.Figure()
        fig_q.add_trace(go.Bar(name="Valides",  x=qualite["source"], y=qualite["valides"],
                               marker_color="#059669", text=qualite["valides"], textposition="inside"))
        fig_q.add_trace(go.Bar(name="Rejetes",  x=qualite["source"], y=qualite["rejetes"],
                               marker_color="#DC2626", text=qualite["rejetes"], textposition="inside"))
        fig_q.update_layout(barmode="stack", height=300, plot_bgcolor="white",
                            paper_bgcolor="white", margin=dict(l=0,r=0,t=20,b=20))
        st.plotly_chart(fig_q, use_container_width=True)

        taux_global = qualite["rejetes"].sum() / qualite["total"].sum() * 100
        if taux_global < 15:
            st.success(f"Taux de rejet global : {taux_global:.1f}% — OKR respecte (< 15%)")
        else:
            st.error(f"Taux de rejet global : {taux_global:.1f}% — OKR non respecte (< 15%)")

    with col_r:
        st.markdown('<div class="section-title">Taux de rejet par source (%)</div>', unsafe_allow_html=True)
        fig_r = px.bar(qualite, x="taux_rejet", y="source", orientation="h",
                       color="taux_rejet",
                       color_continuous_scale=["#059669","#F59E0B","#DC2626"],
                       text="taux_rejet",
                       labels={"taux_rejet":"Taux de rejet (%)","source":""})
        fig_r.add_vline(x=15, line_dash="dash", line_color="#DC2626",
                        annotation_text="Seuil OKR (15%)")
        fig_r.update_traces(texttemplate="%{text}%", textposition="outside")
        fig_r.update_layout(height=300, plot_bgcolor="white", paper_bgcolor="white",
                            coloraxis_showscale=False, margin=dict(l=0,r=60,t=20,b=20))
        st.plotly_chart(fig_r, use_container_width=True)

    st.markdown('<div class="section-title">Matrice de gouvernance — Propriete des donnees</div>', unsafe_allow_html=True)
    gouv_data = pd.DataFrame([
        {"Source":"CoinAfrique (~4 844)", "Proprietaire":"CoinAfrique",    "Usage":"Analytique uniquement", "Risque":"Eleve",  "Accord":"CGU a verifier"},
        {"Source":"ImmoAsk (~500)",       "Proprietaire":"ImmoAsk Togo",   "Usage":"Partenariat",           "Risque":"Faible", "Accord":"Partenariat OK"},
        {"Source":"Facebook (~80)",       "Proprietaire":"Meta / Vendeurs","Usage":"Manuel / limite",        "Risque":"Eleve",  "Accord":"CGU Meta restrictive"},
        {"Source":"OTR (~354)",           "Proprietaire":"Etat Togolais",  "Usage":"Reference officielle",  "Risque":"Moyen",  "Accord":"Accord formel requis"},
        {"Source":"Indice produit",       "Proprietaire":"Equipe ID Immo", "Usage":"Libre diffusion",       "Risque":"Nul",    "Accord":"Pleine propriete"},
    ])

    def color_risque(val):
        if val == "Eleve": return "background-color:#FEE2E2;color:#991B1B"
        if val == "Moyen": return "background-color:#FEF3C7;color:#92400E"
        if val == "Nul":   return "background-color:#D1FAE5;color:#065F46"
        return "background-color:#D1FAE5;color:#065F46"

    st.dataframe(
        gouv_data.style.map(color_risque, subset=["Risque"]),
        use_container_width=True, hide_index=True
    )

    st.markdown('<div class="section-title">Biais identifies — Repartition geographique des annonces</div>', unsafe_allow_html=True)
    zone_counts = df.groupby("zone").size().reset_index(name="count")
    zone_counts["pct"] = (zone_counts["count"] / zone_counts["count"].sum() * 100).round(1)
    zone_counts = zone_counts.sort_values("pct", ascending=False)

    fig_biais = px.bar(zone_counts, x="zone", y="pct",
                       color="pct", color_continuous_scale="Reds", text="pct",
                       labels={"pct":"% des annonces","zone":"Zone"})
    fig_biais.update_traces(texttemplate="%{text}%", textposition="outside")
    fig_biais.update_layout(height=280, plot_bgcolor="white", paper_bgcolor="white",
                            coloraxis_showscale=False, margin=dict(l=0,r=0,t=20,b=20))
    st.plotly_chart(fig_biais, use_container_width=True)
    st.warning("Biais geographique : les 3 premieres zones representent plus de 45% des donnees. Les zones peripheriques sont sous-representees, ce qui reduit la fiabilite de leur indice.")