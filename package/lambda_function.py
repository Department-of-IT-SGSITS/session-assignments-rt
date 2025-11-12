import boto3
import requests
import os
import pymongo
import json
import re

def extract_invoice_fields(text):
    """Extracts basic fields using regex patterns from OCR text."""
    invoice_no = None
    total = None
    date = None
    vendor = None

    # Common regex patterns for invoices
    invoice_patterns = [
        r'Invoice\s*No[:\s]*([A-Z0-9-]+)',
        r'Invoice\s*#[:\s]*([A-Z0-9-]+)',
        r'Facture\s*No[:\s]*([A-Z0-9/]+)',
    ]

    date_patterns = [
        r'Date[:\s]*([0-9]{2}[/-][0-9]{2}[/-][0-9]{4})',
        r'([0-9]{4}[/-][0-9]{2}[/-][0-9]{2})',
    ]

    total_patterns = [
        r'Total[:\s]*\$?([\d,]+\.\d{2})',
        r'Amount\s*Due[:\s]*\$?([\d,]+\.\d{2})',
    ]

    vendor_patterns = [
        r'Vendor[:\s]*(.*)',
        r'From[:\s]*(.*)',
        r'Invoice\s*from[:\s]*(.*)',
    ]

    # Try to find invoice number
    for p in invoice_patterns:
        match = re.search(p, text, re.IGNORECASE)
        if match:
            invoice_no = match.group(1).strip()
            break

    # Try to find total amount
    for p in total_patterns:
        match = re.search(p, text, re.IGNORECASE)
        if match:
            total = match.group(1).strip()
            break

    # Try to find date
    for p in date_patterns:
        match = re.search(p, text, re.IGNORECASE)
        if match:
            date = match.group(1).strip()
            break

    # Try to find vendor
    for p in vendor_patterns:
        match = re.search(p, text, re.IGNORECASE)
        if match:
            vendor = match.group(1).strip()
            break

    return {
        "invoice_no": invoice_no,
        "total": total,
        "date": date,
        "vendor": vendor
    }


def lambda_handler(event, context):
    print("✅ Lambda triggered successfully.")
    print("Incoming Event:", json.dumps(event))

    # --- Step 1: Extract file info from S3 event ---
    s3 = boto3.client('s3')
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    print(f"📁 Processing file from bucket: {bucket}, key: {key}")

    # --- Step 2: Download file from S3 to /tmp ---
    local_path = f"/tmp/{os.path.basename(key)}"
    s3.download_file(bucket, key, local_path)
    print("📥 File downloaded successfully.")

    # --- Step 3: Send to OCR.space API ---
    api_key = os.environ['OCR_SPACE_API_KEY']
    print("🔠 Sending to OCR.space...")
    with open(local_path, 'rb') as f:
        response = requests.post(
            'https://api.ocr.space/parse/image',
            files={'file': f},
            data={'apikey': api_key, 'language': 'eng'}
        )

    result = response.json()
    print("✅ OCR API responded:", json.dumps(result)[:1000])  # print partial

    if 'ParsedResults' not in result:
        print("❌ OCR failed. Response:", result)
        return {"statusCode": 500, "body": "OCR failed"}

    raw_text = result['ParsedResults'][0]['ParsedText']
    print("📝 Extracted text (first 500 chars):", raw_text[:500])

    # --- Step 4: Extract structured fields ---
    extracted = extract_invoice_fields(raw_text)
    print("📊 Extracted fields:", extracted)

    # --- Step 5: Store in MongoDB ---
    mongo_uri = os.environ['MONGO_URI']
    mongo_db = os.environ.get("MONGO_DB", "invoices_db")
    mongo_collection = os.environ.get("MONGO_COLLECTION", "invoices")

    client = pymongo.MongoClient(mongo_uri)
    db = client[mongo_db]
    collection = db[mongo_collection]

    doc = {
        **extracted,
        "raw_text": raw_text,
        "s3_bucket": bucket,
        "s3_key": key,
        "processed_at": record['responseElements']['x-amz-request-id']
    }

    collection.insert_one(doc)
    print("✅ Data inserted into MongoDB successfully.")

    return {"statusCode": 200, "body": "Invoice processed and saved."}
