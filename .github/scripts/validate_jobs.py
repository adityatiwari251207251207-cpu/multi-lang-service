import os
import json
import requests
from datetime import datetime

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars")

JOBS_URL = f"{SUPABASE_URL}/rest/v1/jobs"

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}


def fetch_jobs_to_validate():
    """
    Get jobs where status = 'converted_pending_validation'
    """
    params = {
        "select": "id,file_id,status,converted_code,last_error",
        "status": "eq.converted_pending_validation",
        "limit": "100",
    }
    resp = requests.get(JOBS_URL, headers=HEADERS, params=params)
    print("GET jobs URL:", resp.url)
    print("GET jobs status:", resp.status_code)
    print("GET jobs response (first 300 chars):", resp.text[:300])
    resp.raise_for_status()
    return resp.json()


def update_job(job_id, new_status, error_message=None):
    """
    Update a single job row in Supabase.
    """
    url = f"{JOBS_URL}?id=eq.{job_id}"

    payload = {
        "status": new_status,
        "last_error": error_message,
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }

    resp = requests.patch(
        url,
        headers={
            **HEADERS,
            "Prefer": "return=representation,resolution=merge-duplicates",
        },
        data=json.dumps(payload),
    )
    print(f"PATCH job {job_id} → {new_status}, status code:", resp.status_code)
    print("PATCH response (first 300 chars):", resp.text[:300])
    resp.raise_for_status()


def main():
    jobs = fetch_jobs_to_validate()
    print(f"Found {len(jobs)} jobs to validate")

    if not jobs:
        print("Nothing to validate. Exiting.")
        return

    for job in jobs:
        jid = job["id"]
        code = job.get("converted_code") or ""
        code_stripped = code.strip()

        if not code_stripped:
            print(f"Job {jid}: FAIL – no converted_code present")
            update_job(jid, "failed_validation", "No converted_code saved for this job.")
        else:
            # Minimal validation passed – we have some code
            print(f"Job {jid}: OK – converted_code is present (length={len(code_stripped)})")
            update_job(jid, "converted_ok", None)


if __name__ == "__main__":
    print("=== Starting validator ===")
    main()
    print("=== Validator finished ===")
