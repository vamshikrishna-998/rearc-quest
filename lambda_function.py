import boto3
import requests
import json
from bs4 import BeautifulSoup

BUCKET_NAME = "vamshi-rearc-quest-844513166649-us-east-2-an"
BASE_URL    = "https://download.bls.gov"
BLS_URL     = "https://download.bls.gov/pub/time.series/pr/"
HEADERS     = {"User-Agent": "vamshi-rearc-quest/1.0 (youremail@gmail.com)"}
API_URL     = "https://api.census.gov/data/2019/pep/population?get=DATE_CODE,DATE_DESC,POP&for=us:1"

s3 = boto3.client("s3", region_name="us-east-2")

def sync_bls():
    print("Starting BLS sync...")
    resp = requests.get(BLS_URL, headers=HEADERS)
    soup = BeautifulSoup(resp.text, "html.parser")
    links = [
        a["href"] for a in soup.find_all("a")
        if a["href"].startswith("/pub/time.series/pr/pr.")
    ]

    # Get existing S3 files
    result = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="pub/time.series/pr/")
    existing = {obj["Key"] for obj in result.get("Contents", [])}

    for path in links:
        filename = path.split("/")[-1]
        s3_key   = f"pub/time.series/pr/{filename}"

        if s3_key in existing:
            print(f"  [SKIP] {filename}")
            continue

        content = requests.get(BASE_URL + path, headers=HEADERS).content
        if b"<!DOCTYPE" in content[:200]:
            print(f"  [BLOCKED] {filename}")
            continue

        s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=content)
        print(f"  [OK] {filename}")

    # Delete removed files
    remote_keys = {f"pub/time.series/pr/{p.split('/')[-1]}" for p in links}
    for key in existing - remote_keys:
        s3.delete_object(Bucket=BUCKET_NAME, Key=key)
        print(f"  [DELETED] {key}")

def sync_population():
    print("Starting population sync...")
    resp = requests.get(API_URL, timeout=30)
    raw  = resp.json()
    records = raw[1:]
    all_records = []
    for r in records:
        all_records.append({
            "Year": r[1].split(" ")[-1],
            "Population": int(r[2]),
            "Nation": "United States",
            "DATE_DESC": r[1]
        })
    data = {"data": all_records}
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key="population/population.json",
        Body=json.dumps(data),
        ContentType="application/json"
    )
    print("Population data updated!")

def lambda_handler(event, context):
    sync_bls()
    sync_population()
    return {"statusCode": 200, "body": "Sync complete!"}exit