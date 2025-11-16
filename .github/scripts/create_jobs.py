import os
import json
import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars")

FILES_URL = f"{SUPABASE_URL}/rest/v1/files_manifest"
JOBS_URL = f"{SUPABASE_URL}/rest/v1/jobs"

headers = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}


def fetch_some_files():
    """
    Fetch up to 20 files from files_manifest, regardless of status.
    This is just to prove the connection works and that we see rows.
    """
    params = {
        "select": "id,path,language,kind,status",
        "limit": "20",
    }
    resp = requests.get(FILES_URL, headers=headers, params=params)
    print("FILES GET URL:", resp.url)
    print("FILES status code:", resp.status_code)
    print("FILES response (first 500 chars):")
    print(resp.text[:500])
    resp.raise_for_status()
    return resp.json()


def create_job(file_row):
    prompt = f"""
You are converting a file from {file_row['language']} to <TARGET_LANGUAGE>.
IMPORTANT RULES:
- Do NOT add new features.
- Keep the same behavior and inputs/outputs.
- Keep the same API surface unless explicitly impossible.
- Output ONLY <TARGET_LANGUAGE> code, no explanations.

Context:
- Source file path: {file_row['path']}
- Source language: {file_row['language']}
- Kind: {file_row['kind']}
"""

    payload = {
        "file_id": file_row["id"],
        "status": "queued",
        "prompt": prompt,
    }
    resp = requests.post(JOBS_URL, headers=headers, data=json.dumps(payload))
    print(f"JOB POST status for {file_row['path']}: {resp.status_code}")
    print("JOB POST response (first 300 chars):", resp.text[:300])
    resp.raise_for_status()


if __name__ == "__main__":
    print("=== Starting job generator ===")
    files = fetch_some_files()
    print(f"Fetched {len(files)} files from files_manifest")

    if not files:
        print("No files returned from Supabase. Check table and RLS.")
    else:
        for f in files:
            print("Creating job for:", f["path"])
            create_job(f)

    print("=== Job generator finished ===")

