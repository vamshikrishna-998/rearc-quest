import boto3
import requests
from bs4 import BeautifulSoup

# --- Config ---
AWS_ACCESS_KEY = "AKIA4JIHIIU43W4JXVMZ"
AWS_SECRET_KEY = "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"

BUCKET_NAME    = "vamshi-rearc-quest-844513166649-us-east-2-an"

BASE_URL = "https://download.bls.gov"
BLS_URL  = "https://download.bls.gov/pub/time.series/pr/"

# BLS requires contact info in User-Agent
HEADERS = {
    "User-Agent": "vamshi-rearc-quest/1.0 (youremail@gmail.com)",
}

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name="us-east-2"
)

def sync():
    print("Fetching BLS file list...")
    resp = requests.get(BLS_URL, headers=HEADERS)
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Only get actual files (not parent directory links)
    links = [
        a["href"] for a in soup.find_all("a")
        if a["href"].startswith("/pub/time.series/pr/pr.")
    ]
    print(f"Found {len(links)} files")

    for path in links:
        filename = path.split("/")[-1]
        full_url = BASE_URL + path
        s3_key   = f"pub/time.series/pr/{filename}"

        print(f"  Downloading {filename}...")
        resp = requests.get(full_url, headers=HEADERS)
        content = resp.content

        if b"<!DOCTYPE" in content[:200] or b"<html" in content[:200]:
            print(f"  [BLOCKED] {filename}")
            continue

        s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=content)
        print(f"  [OK] {filename}")

    print("\nDone!")

sync()