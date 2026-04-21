import pandas as pd
import aiohttp
import asyncio
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from io import BytesIO

# ── Config ──────────────────────────────────────────────────────────────────
URL_CSV      = "http://www.e-liquide-fr.com/Data/Doofinder/fr/Oxatis-fr-E-Liquide-23691.csv"
API_BASE     = "https://api.guaranteed-reviews.com/public/v3/reviews/552918c9aca2931de4c712cec1d792e5/"
OUTPUT_FILE  = "produits_avec_avis.csv"
USER_AGENT   = "E-Liquide-FR-DoofinderSync/1.0"

# ── Session HTTP robuste avec retry automatique ──────────────────────────────
def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,                          # 5 tentatives max
        backoff_factor=2,                 # attente : 2s, 4s, 8s, 16s...
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent":      USER_AGENT,
        "Accept":          "text/csv,text/plain,*/*;q=0.9",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Referer":         "https://www.e-liquide-fr.com/",
        "Connection":      "keep-alive",
    })
    return session


# ── Téléchargement CSV ───────────────────────────────────────────────────────
def fetch_csv(session: requests.Session) -> pd.DataFrame:
    print(f"📥 Téléchargement CSV Oxatis...")
    r = session.get(URL_CSV, timeout=30)
    r.raise_for_status()
    print(f"✅ CSV récupéré : {len(r.content):,} bytes")
    return pd.read_csv(BytesIO(r.content), sep=";", encoding="Windows-1252")


# ── Récupération des avis (async + concurrence limitée) ─────────────────────
async def fetch_review(sem, session, parent_id):
    async with sem:  # max 20 requêtes simultanées
        url = API_BASE + str(int(parent_id))
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                total   = data.get("ratings", {}).get("total", 0)
                average = data.get("ratings", {}).get("average", 0)
                return (total, average)
        except Exception as e:
            print(f"⚠️  Avis {parent_id} : {e}")
            return (0, 0)


async def fetch_all_reviews(parent_ids: list) -> list:
    sem = asyncio.Semaphore(20)  # 20 requêtes parallèles max
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_review(sem, session, pid) for pid in parent_ids]
        results = await asyncio.gather(*tasks)
    return results


# ── Pipeline principal ───────────────────────────────────────────────────────
async def main():
    session = make_session()

    # 1. CSV Oxatis
    df = fetch_csv(session)
    print(f"   → {len(df)} lignes chargées")

    # 2. Calcul des ParentIDs
    df["ParentID"] = (
        df["item_group_id"]
        .where(df["item_group_id"].notnull(), df["pdt_id"])
        .astype("Int64")
    )
    df["item_group_id"] = df["item_group_id"].astype("Int64")

    # 3. Avis en parallèle
    print(f"🔍 Récupération des avis ({len(df)} produits en parallèle)...")
    results = await fetch_all_reviews(df["ParentID"].tolist())
    df["review_count"], df["df_rating"] = zip(*results)

    # 4. Formatage
    df["df_rating"]    = df["df_rating"].apply(lambda x: "" if x == 0 else f"{x:.1f}")
    df["review_count"] = df["review_count"].apply(lambda x: "" if x == 0 else int(x))

    # 5. Export
    df.to_csv(OUTPUT_FILE, index=False, sep=";")
    filled = (df["review_count"] != "").sum()
    print(f"💾 CSV sauvegardé : {OUTPUT_FILE}")
    print(f"   → {len(df)} produits | {filled} avec avis")


if __name__ == "__main__":
    asyncio.run(main())
