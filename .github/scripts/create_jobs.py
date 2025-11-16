import os
import json
import requests
from datetime import datetime

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

FILES_URL = f"{SUPABASE_URL}/rest/v1/files_manifest"
JOBS_URL = f"{SUPABASE_URL}/rest/v1/jobs"

headers = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}


def fetch_pending_files():
    params = {
        "select": "id,path,language,kind,status",
        "status": "eq.pending",
        "limit": "50"  # process in small batches
    }
    resp = requests.get(FILES_URL, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()


def create_job(file_row):
    prompt = f"""
You are converting a file from {file_row['language']} to <TARGET_LANGUAGE>.
IMPORTANT: You must not add features or change behavior.
Preserve existing APIs unless explicitly allowed.

Context:
- Source file path: {file_row['path']}
- Language: {file_row['language']}
- Kind: {file_row['kind']}
- Task: Convert to <TARGET_LANGUAGE> code
- Output must be ONLY code, nothing else.
"""

    payload = {
        "file_id": file_row["id"],
        "status": "queued",
        "prompt": prompt,
    }
    resp = requests.post(JOBS_URL, headers=headers, data=json.dumps(payload))
    resp.raise_for_status()
    print(f"Created job for file: {file_row['path']}")


if __name__ == "__main__":
    files = fetch_pending_files()
    if not files:
        print("No pending files found.")
    else:
        for f in files:
            create_job(f)
