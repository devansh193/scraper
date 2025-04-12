import requests
import csv
import time
from datetime import datetime

BASE_URL = "https://blinkit.com"

LAT_LON_PAIRS = [
    {"lat": "28.678051", "lon": "77.314262"},
    {"lat": "28.5045", "lon": "77.012"},
    {"lat": "22.59643333", "lon": "88.39996667"},
    {"lat": "23.1090018", "lon": "72.57299832"},
    {"lat": "18.95833333", "lon": "72.83333333"},
    {"lat": "28.7045", "lon": "77.15366667"},
    {"lat": "12.88326667", "lon": "77.5594"},
    {"lat": "28.7295", "lon": "77.12866667"},
    {"lat": "28.67571622", "lon": "77.36149677"},
    {"lat": "28.3501", "lon": "77.31673333"},
    {"lat": "28.59086667", "lon": "77.3054"},
    {"lat": "28.49553604", "lon": "77.51297417"},
    {"lat": "28.44176667", "lon": "77.3084"},
    {"lat": "28.48783333", "lon": "77.09533333"},
    {"lat": "12.93326667", "lon": "77.61773333"},
    {"lat": "13.00826667", "lon": "77.64273333"},
    {"lat": "28.4751", "lon": "77.4334"},
    {"lat": "26.85653333", "lon": "75.71283333"},
    {"lat": "26.8982", "lon": "75.8295"},
    {"lat": "18.54316667", "lon": "73.914"},
]

CATEGORY_PAIRS = [
    {"label_l0": "munchies", "label_l1": "Bhujia & mixtures", "l0": 1237, "l1": 1178},
    {"label_l0": "munchies", "label_l1": "Munchies gift pack", "l0": 1237, "l1": 1694},
    {"label_l0": "munchies", "label_l1": "Namkeen and Snacks", "l0": 1237, "l1": 29},
    {"label_l0": "munchies", "label_l1": "Papad & fryums", "l0": 1237, "l1": 80},
    {"label_l0": "munchies", "label_l1": "Chips and crips", "l0": 1237, "l1": 940},
    {"label_l0": "Sweets", "label_l1": "Indian sweets", "l0": 9, "l1": 943},
]


def delay(ms):
    time.sleep(ms / 1000)


def scrape_blinkit(label_l0, label_l1, l0, l1, headers, writer, write_header):
    url = f"{BASE_URL}/v1/layout/listing_widgets?l0_cat={l0}&l1_cat={l1}"
    all_products = []

    while url:
        print(f"🌐 Fetching: {url}\n")
        try:
            res = requests.post(url, headers=headers, json={})
            if not res.ok:
                print(f"❌ Failed at {url}: {res.status_code} {res.reason}")
                break

            data = res.json()
            snippets = data.get("response", {}).get("snippets", [])
            for item in snippets:
                d = item.get("data", {})
                if not d:
                    continue
                product = {
                    "date": datetime.utcnow().isoformat(),
                    "id": d.get("identity", {}).get("id"),
                    "variant_name": d.get("variant", {}).get("text"),
                    "name": d.get("name", {}).get("text"),
                    "display_name": d.get("display_name", {}).get("text"),
                    "brand_name": d.get("brand_name", {}).get("text"),
                    "mrp": d.get("mrp", {}).get("text"),
                    "normal_price": d.get("normal_price", {}).get("text"),
                    "product_state": d.get("product_state"),
                    "inventory": d.get("inventory"),
                    "image": d.get("image", {}).get("url"),
                    "product_Id": d.get("meta", {}).get("product_id"),
                    "merchant_Id": d.get("meta", {}).get("merchant_id"),
                    "group_id": d.get("group_id"),
                    "l0_category": item.get("tracking", {}).get("common_attributes", {}).get("l0_category"),
                    "l1_category": item.get("tracking", {}).get("common_attributes", {}).get("l1_category"),
                    "l2_category": item.get("tracking", {}).get("common_attributes", {}).get("l2_category"),
                }
                all_products.append(product)

            next_url = data.get("response", {}).get("pagination", {}).get("next_url")
            url = f"{BASE_URL}{next_url}" if next_url else ""

            if url:
                query = url.split("?")[-1]
                params = dict(x.split("=") for x in query.split("&") if "=" in x)
                total_items = int(params.get("total_pagination_items", 0))
                delay(700 if total_items > 300 else 100)

        except Exception as e:
            print(f"Error fetching l0:{l0}, l1:{l1} => {e}")
            break

    if all_products:
        if write_header:
            writer.writeheader()
        writer.writerows(all_products)
        print(f"✅ Scraped {len(all_products)} products for {label_l1}, written to CSV.\n")
    else:
        print(f"⚠️ No products found for {label_l1}, nothing saved.\n")


final_file_path = "blinkit-all-products.csv"
csv_initialized = False

with open(final_file_path, "w", encoding="utf-8", newline='') as f:
    fieldnames = [
        "date", "id", "variant_name", "name", "display_name", "brand_name", "mrp", "normal_price",
        "product_state", "inventory", "image", "product_Id", "merchant_Id", "group_id",
        "l0_category", "l1_category", "l2_category"
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames)

    for loc in LAT_LON_PAIRS:
        lat = loc["lat"]
        lon = loc["lon"]

        HEADERS = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
            "lat": lat,
            "lon": lon,
        }

        print(f"\n📍 Scraping for location {lat}, {lon}\n")

        for cat in CATEGORY_PAIRS:
            scrape_blinkit(cat["label_l0"], cat["label_l1"], cat["l0"], cat["l1"], HEADERS, writer, not csv_initialized)
            csv_initialized = True
            print(f"🗂️ Finished {cat['label_l0']} > {cat['label_l1']} for {lat}, {lon}\n")
            delay(5000)

print(f"\n✅ All data has been written to {final_file_path}")
