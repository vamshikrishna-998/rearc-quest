import boto3
import requests
import json

# --- Config ---
AWS_ACCESS_KEY = "AKIA4JIHIIU43W4JXVMZ"
AWS_SECRET_KEY = "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"


BUCKET_NAME    = "vamshi-rearc-quest-844513166649-us-east-2-an"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name="us-east-2"
)

def fetch_and_upload():
    # Single API call — gets all years 2013-2019 in one shot
    url = "https://api.census.gov/data/2019/pep/population?get=DATE_CODE,DATE_DESC,POP&for=us:1"
    print("Fetching population data...")
    
    resp = requests.get(url, timeout=30)
    raw = resp.json()
    
    headers = raw[0]
    records = raw[1:]
    
    all_records = []
    for r in records:
        all_records.append({
            "Year": r[1].split(" ")[-1],   # extract year from DATE_DESC
            "Population": int(r[2]),
            "Nation": "United States",
            "DATE_DESC": r[1]
        })

    data = {"data": all_records}
    print(f"Total records collected: {len(all_records)}")
    for rec in all_records:
        print(f"  {rec['Year']}: {rec['Population']:,}")

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key="population/population.json",
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print("\nUploaded to S3!")
    print(f"File URL: https://{BUCKET_NAME}.s3.us-east-2.amazonaws.com/population/population.json")

fetch_and_upload()