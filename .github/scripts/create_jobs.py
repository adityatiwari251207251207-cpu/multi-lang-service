import os
import json
import requests
from textwrap import dedent

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars")

FILES_URL = f"{SUPABASE_URL}/rest/v1/files_manifest"
JOBS_URL = f"{SUPABASE_URL}/rest/v1/jobs"

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}


BATCH_SIZE = 20  # up to 20 files per job


def fetch_pending_files(limit=200):
    """
    Fetch up to `limit` files with status = 'pending'.
    """
    params = {
        "select": "id,path,language,kind,status",
        "status": "eq.pending",
        "limit": str(limit),
    }
    resp = requests.get(FILES_URL, headers=HEADERS, params=params)
    print("FILES GET URL:", resp.url)
    print("FILES status:", resp.status_code)
    print("FILES response (first 300 chars):", resp.text[:300])
    resp.raise_for_status()
    return resp.json()


def chunk_list(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def build_prompt_for_batch(files_batch):
    """
    Build a single Gemini prompt for 15–20 files.
    You will still paste the code manually, but this tells Gemini
    which files and languages are involved.
    """
    file_lines = []
    for idx, f in enumerate(files_batch, start=1):
        file_lines.append(
            f"{idx}. path={f['path']} | language={f.get('language') or 'unknown'} | kind={f.get('kind') or 'source'}"
        )

    files_info = "\n".join(file_lines)

    prompt = dedent(
        f"""
        You are converting multiple source files to <TARGET_LANGUAGE>.

        RULES (IMPORTANT):
        - Do NOT add new features.
        - Preserve the behavior and public APIs as much as possible.
        - If a file's language is 'unknown', infer from the syntax.
        - Output ONLY <TARGET_LANGUAGE> code blocks for each file, no explanations.
        - Keep file separation clear in your output (e.g. comments like // FILE 1: path).

        FILES IN THIS BATCH:
        {files_info}

        I will paste the contents of these files below in order, clearly separated.
        For each file, return the converted <TARGET_LANGUAGE> code, in the same order.
        """
    ).strip()

    return prompt


def create_job_for_batch(files_batch):
    if not files_batch:
        return

    prompt = build_prompt_for_batch(files_batch)

    first_file = files_batch[0]
    file_paths = [f["path"] for f in files_batch]

    payload = {
        "file_id": first_file["id"],   # keep FK
        "status": "queued",
        "prompt": prompt,
        "file_paths": file_paths,      # jsonb array
    }

    resp = requests.post(JOBS_URL, headers=HEADERS, data=json.dumps(payload))
    print("JOB POST status:", resp.status_code)
    print("JOB POST response (first 300 chars):", resp.text[:300])
    resp.raise_for_status()
    print(f"Created batch job for {len(files_batch)} files")


def main():
    files = fetch_pending_files(limit=200)
    print(f"Fetched {len(files)} pending files")

    if not files:
        print("No pending files found.")
        return

    for batch in chunk_list(files, BATCH_SIZE):
        print("Creating job for batch of size:", len(batch))
        create_job_for_batch(batch)


if __name__ == "__main__":
    print("=== Starting batch job generator ===")
    main()
    print("=== Done ===")
