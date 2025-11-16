import os
import json
import requests
from textwrap import dedent

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
REPO_RAW_BASE_URL = os.environ.get("REPO_RAW_BASE_URL")  # <-- REQUIRED

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY or not REPO_RAW_BASE_URL:
    raise SystemExit("Missing SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, or REPO_RAW_BASE_URL")

FILES_URL = f"{SUPABASE_URL}/rest/v1/files_manifest"
JOBS_URL = f"{SUPABASE_URL}/rest/v1/jobs"

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}

BATCH_SIZE = 20  # 15–20 files per batch recommended


def fetch_pending_files(limit=200):
    params = {
        "select": "id,path,language,kind,status",
        "status": "eq.pending",
        "limit": str(limit),
    }
    resp = requests.get(FILES_URL, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json()


def chunk_list(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def make_raw_url(file_path):
    # Assumes repo is public or tokenless raw access works
    # REPO_RAW_BASE_URL should look like:
    # https://raw.githubusercontent.com/<user>/<repo>/main
    return f"{REPO_RAW_BASE_URL}/{file_path}"


def build_prompt(files_batch):
    """
    Gemini WILL fetch code from GitHub directly using raw URLs.
    We enforce strict structure to eliminate hallucinations.
    """

    file_lines = []
    for idx, f in enumerate(files_batch, start=1):
        raw_url = make_raw_url(f["path"])
        file_lines.append(f"{idx}. {f['path']}  →  {raw_url}")

    files_list = "\n".join(file_lines)

    prompt = dedent(f"""
    CONVERSION TASK (STRICT - NO HALLUCINATIONS)

    You will convert **exactly** the files listed below to <TARGET_LANGUAGE>.

    RULES:
    - You MUST fetch each file from the raw GitHub URLs provided.
    - Do NOT guess or invent any file content.
    - If you cannot fetch a file, STOP and report the problem.
    - NO new features.
    - Preserve behavior, function signatures, and return shapes.
    - Output MUST follow this exact format:

    === FILE: original/path.ext ===
    <converted code block only>

    === FILE: original/path2.ext ===
    <converted code block only>

    (no explanations, no comments, no filler text)

    FILES TO CONVERT (in order):
    {files_list}

    Fetch all files and wait for me to paste nothing further
    because all source code is already available from URLs.
    """).strip()

    return prompt


def create_job(files_batch):
    first_file = files_batch[0]
    file_paths = [f["path"] for f in files_batch]

    payload = {
        "file_id": first_file["id"],
        "file_paths": file_paths,
        "status": "queued",
        "prompt": build_prompt(files_batch),
    }

    resp = requests.post(JOBS_URL, headers=HEADERS, data=json.dumps(payload))
    resp.raise_for_status()
    print(f"Created batch job for {len(files_batch)} files")


def main():
    files = fetch_pending_files(limit=200)
    if not files:
        print("No pending files.")
        return

    for batch in chunk_list(files, BATCH_SIZE):
        create_job(batch)


if __name__ == "__main__":
    main()
