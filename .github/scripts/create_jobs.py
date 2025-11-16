import os
import json
import requests
from textwrap import dedent

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
REPO_RAW_BASE_URL = os.environ.get("REPO_RAW_BASE_URL")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY or not REPO_RAW_BASE_URL:
    raise SystemExit("Missing SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, or REPO_RAW_BASE_URL")

FILES_URL = f"{SUPABASE_URL}/rest/v1/files_manifest"
JOBS_URL = f"{SUPABASE_URL}/rest/v1/jobs"

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}

BATCH_SIZE = 20  # <= 20 files per job (upper limit only)


def fetch_pending_files(limit=500):
    """
    Get files where status = 'pending'.
    """
    params = {
        "select": "id,path,language,kind,status",
        "status": "eq.pending",
        "limit": str(limit),
    }
    resp = requests.get(FILES_URL, headers=HEADERS, params=params)
    print("FILES GET status:", resp.status_code)
    print("FILES GET sample:", resp.text[:300])
    resp.raise_for_status()
    rows = resp.json()

    # Deduplicate by path (keep first occurrence)
    seen = {}
    for row in rows:
        p = row["path"]
        if p not in seen:
            seen[p] = row

    unique_files = list(seen.values())
    print(f"Fetched {len(rows)} rows, {len(unique_files)} unique paths")
    return unique_files


def chunk_list(items, size):
    """
    Yield chunks of size <= size (last chunk can be smaller).
    """
    for i in range(0, len(items), size):
        yield items[i: i + size]


def make_raw_url(file_path: str) -> str:
    # REPO_RAW_BASE_URL looks like:
    # https://raw.githubusercontent.com/<user>/<repo>/main
    return f"{REPO_RAW_BASE_URL}/{file_path}"


def build_prompt(files_batch):
    """
    Build strict, link-based, NO-PADDING, NO-HALLUCINATION prompt.
    """
    lines = []
    for idx, f in enumerate(files_batch, start=1):
        raw_url = make_raw_url(f["path"])
        lang = f.get("language") or "unknown"
        kind = f.get("kind") or "source"
        lines.append(f"{idx}. {f['path']} | language={lang} | kind={kind} | url={raw_url}")

    files_list = "\n".join(lines)

    prompt = dedent(f"""
    CONVERSION TASK (STRICT - NO HALLUCINATIONS)

    You will convert **exactly** the files listed below to <TARGET_LANGUAGE>.

    RULES:
    - You MUST fetch each file from its raw GitHub URL (do not guess contents).
    - Do NOT invent or assume missing files or functions.
    - If any URL is unreachable, STOP and say which one failed.
    - NO new features.
    - Preserve behavior, inputs, outputs, and public APIs.
    - Only convert the files listed; nothing else.

    OUTPUT FORMAT (MUST FOLLOW THIS EXACTLY, ONE BLOCK PER FILE IN ORDER):

    === FILE: original/path.ext ===
    <converted <TARGET_LANGUAGE> code only>

    === FILE: next/path2.ext ===
    <converted <TARGET_LANGUAGE> code only>

    (no explanations, no commentary, no extra text)

    FILES TO CONVERT (in order):
    {files_list}
    """).strip()

    return prompt


def create_job(files_batch):
    """
    Create a single job for this batch (size <= BATCH_SIZE) and
    then mark those files as 'in_job' so they won't be reused.
    """
    if not files_batch:
        return

    first_file = files_batch[0]
    file_paths = [f["path"] for f in files_batch]

    payload = {
        "file_id": first_file["id"],   # keep FK valid
        "file_paths": file_paths,      # jsonb array
        "status": "queued",
        "prompt": build_prompt(files_batch),
    }

    resp = requests.post(JOBS_URL, headers=HEADERS, data=json.dumps(payload))
    print("JOB POST status:", resp.status_code, "for", len(files_batch), "files")
    print("JOB POST sample:", resp.text[:200])
    resp.raise_for_status()

    # Mark these files as 'in_job' so they won't be picked again
    for f in files_batch:
        file_id = f["id"]
        url = f"{FILES_URL}?id=eq.{file_id}"
        update_payload = {
            "status": "in_job"
        }
        u_resp = requests.patch(
            url,
            headers={
                **HEADERS,
                "Prefer": "return=minimal,resolution=merge-duplicates",
            },
            data=json.dumps(update_payload),
        )
        print(f"Updated file {file_id} → in_job, status:", u_resp.status_code)
        u_resp.raise_for_status()


def main():
    files = fetch_pending_files(limit=500)
    if not files:
        print("No pending files found.")
        return

    for batch in chunk_list(files, BATCH_SIZE):
        # batch length will always be <= BATCH_SIZE; last chunk may be smaller
        print(f"Creating job for batch of {len(batch)} files")
        create_job(batch)


if __name__ == "__main__":
    print("=== Starting batch job generator ===")
    main()
    print("=== Done ===")
