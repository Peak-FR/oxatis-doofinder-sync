import pandas as pd
import aiohttp
import asyncio
import requests
from io import BytesIO

url_csv = "http://www.e-liquide-fr.com/Data/Doofinder/fr/Oxatis-fr-E-Liquide-23691.csv"
api_url_base = "https://api.guaranteed-reviews.com/public/v3/reviews/552918c9aca2931de4c712cec1d792e5/"
output_file = "produits_avec_avis.csv"


async def fetch_review_data(session, parent_id):
    api_url = api_url_base + str(int(parent_id))
    try:
        async with session.get(api_url) as response:
            data = await response.json()
            total = data.get('ratings', {}).get('total', 0)
            average = data.get('ratings', {}).get('average', 0)
            print(f"✅ {parent_id}: {total} avis, {average:.1f}")
            return (total, average)
    except Exception as e:
        print(f"❌ {parent_id}: {e}")
        return (0, 0)


async def fetch_all_reviews(product_ids):
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.ensure_future(fetch_review_data(session, pid)) for pid in product_ids]
        return await asyncio.gather(*tasks)


def fetch_csv():
    print("📥 Téléchargement CSV Oxatis...")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'text/csv,text/plain,*/*;q=0.9',
        'Accept-Language': 'fr-FR,fr;q=0.9',
        'Referer': 'https://www.e-liquide-fr.com/',
    }
    r = requests.get(url_csv, headers=headers, timeout=30)
    r.raise_for_status()
    print(f"✅ CSV OK : {len(r.content)} bytes")
    return pd.read_csv(BytesIO(r.content), sep=';', encoding='Windows-1252')


async def main():
    products_df = fetch_csv()
    products_df['df_rating'] = None
    products_df['review_count'] = None

    products_df['ParentID'] = products_df['item_group_id'].where(
        products_df['item_group_id'].notnull(),
        products_df['pdt_id']
    )
    products_df['item_group_id'] = products_df['item_group_id'].astype('Int64')
    products_df['ParentID'] = products_df['ParentID'].astype('Int64')

    print(f"🔍 Récupération des avis pour {len(products_df)} produits...")
    parent_ids = products_df['ParentID'].tolist()
    reviews_data = await fetch_all_reviews(parent_ids)
    products_df['review_count'], products_df['df_rating'] = zip(*reviews_data)

    products_df['df_rating'] = products_df['df_rating'].apply(
        lambda x: '' if x == 0 else f"{x:.1f}"
    )
    products_df['review_count'] = products_df['review_count'].apply(
        lambda x: '' if x == 0 else int(x)
    )

    products_df.to_csv(output_file, index=False, sep=';')
    print(f"💾 CSV sauvegardé : {output_file} ({len(products_df)} produits)")


if __name__ == "__main__":
    asyncio.run(main())
