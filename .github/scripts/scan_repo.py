import os
import json
import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars")

FILES_TABLE_URL = f"{SUPABASE_URL}/rest/v1/files_manifest"

# Simple mapping from file extensions to language names
EXT_TO_LANG = {
    ".py": "python",
    ".js": "javascript",
    ".ts": "typescript",
    ".go": "go",
    ".java": "java",
    ".cs": "csharp",
    ".php": "php",
    ".rb": "ruby",
    ".rs": "rust",
    ".cpp": "cpp",
    ".c": "c",
    ".h": "c-header",
    ".html": "html",
    ".css": "css",
    ".sql": "sql",
    ".json": "json",
    ".yml": "yaml",
    ".yaml": "yaml",
}

# Folders to skip
SKIP_DIRS = {".git", ".github", "node_modules", "__pycache__", "dist", "build", ".venv", "venv"}


def detect_language(filename: str) -> str:
    _, ext = os.path.splitext(filename)
    return EXT_TO_LANG.get(ext.lower(), "unknown")


def detect_kind(path: str) -> str:
    lower = path.lower()
    if "test" in lower or lower.endswith("_test.py") or lower.endswith("_test.go"):
        return "test"
    if "config" in lower or lower.endswith(".yml") or lower.endswith(".yaml"):
        return "config"
    return "source"


def scan_repo(root_dir: str = "."):
    records = []

    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Remove skipped directories from traversal
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]

        for filename in filenames:
            rel_path = os.path.relpath(os.path.join(dirpath, filename), root_dir)

            # Skip files in .github except our script if you want
            if rel_path.startswith(".github/"):
                continue

            language = detect_language(filename)
            kind = detect_kind(rel_path)

            record = {
                "path": rel_path.replace("\\", "/"),
                "language": language,
                "kind": kind,
                "dependencies": None,
                "has_tests": False,
                "priority_score": 0,
                "status": "pending",
            }
            records.append(record)

    return records


def upsert_files_manifest(records):
    if not records:
        print("No files found to upload")
        return

    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    # Supabase REST can accept batch inserts
    resp = requests.post(FILES_TABLE_URL, headers=headers, data=json.dumps(records))
    if resp.status_code not in (200, 201, 204):
        print("Error from Supabase:", resp.status_code, resp.text)
        resp.raise_for_status()
    else:
        print(f"Uploaded {len(records)} records to files_manifest")


if __name__ == "__main__":
    print("Scanning repository...")
    records = scan_repo(".")
    print(f"Found {len(records)} files")
    upsert_files_manifest(records)
