Voici les étapes de test en développement — sans Docker, rapide et efficace :

---

## Ordre de test développement

**1. Modifier tes données sources** (si besoin)
```
data/raw/sources/*.xlsx
```

**2. Ingestion**
```powershell
python pipeline/ingestion.py
```

**3. Nettoyage rapide (pandas)**
```powershell
python pipeline/cleaning_v2_pandas.py
```

**4. Vérifier le résultat du nettoyage**
```powershell
python -c "import pandas as pd; df=pd.read_csv('data/cleaned_v2/annonces_clean.csv'); print(df['type_bien'].value_counts())"
```

**5. Modélisation MySQL**
```powershell
python pipeline/modeling_v2.py
```

**6. Indicateurs**
```powershell
python pipeline/indicators.py
```

**7. Indice**
```powershell
python pipeline/index.py
```

**8. Dashboard**
```powershell
streamlit run dashboard/app.py
```

---

## Résumé

| Contexte | Utilise |
|----------|---------|
| Dev quotidien | `cleaning_v2_pandas.py` — 10 sec |
| Production / Soutenance | Airflow + `cleaning_v2.py` via Spark |

