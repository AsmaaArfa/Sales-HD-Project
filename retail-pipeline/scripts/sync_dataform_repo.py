#!/usr/bin/env python3
"""
sync_dataform_repo.py
---------------------
Syncs local Dataform source files to a GCP Dataform repository
by writing files via the Dataform API (workspaces endpoint).

Usage:
    python3 scripts/sync_dataform_repo.py \
        --project my-project \
        --region us-central1 \
        --repo retail-dataform \
        --source dataform/
"""

import argparse
import base64
import sys
from pathlib import Path

import google.auth
import google.auth.transport.requests
import requests


def get_token():
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token


def dataform_api(project, region, repo):
    return (
        f"https://dataform.googleapis.com/v1beta1"
        f"/projects/{project}/locations/{region}/repositories/{repo}"
    )

def ensure_workspace_exists(api_base, token, workspace):
    url = f"{api_base}/workspaces?workspaceId={workspace}"

    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )

    # 200 = created, 409 = already exists
    if resp.status_code in (200, 409):
        return True

    raise Exception(f"Workspace error: {resp.text}")

def write_file(api_base, token, workspace, relative_path, content_bytes):
    url = f"{api_base}/workspaces/{workspace}:writeFile"
    payload = {
        "path": relative_path,
        "contents": base64.b64encode(content_bytes).decode(),
    }
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=(5,120),  # (connect timeout, read timeout)
    )
    if resp.status_code not in (200, 204):
        print(f"  ⚠️  Failed to write {relative_path}: {resp.status_code} {resp.text}")
        return False
    return True


SYNC_EXTENSIONS = {".sqlx", ".js", ".json", ".yaml", ".yml"}
WORKSPACE = "default"  # Dataform workspace name


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--region",  required=True)
    parser.add_argument("--repo",    required=True)
    parser.add_argument("--source",  default="dataform/") 
    args = parser.parse_args()

    source_dir = Path(args.source).resolve()
    if not source_dir.exists():
        print(f"❌ Source directory not found: {source_dir}")
        sys.exit(1)

    token = get_token()
    api_base = dataform_api(args.project, args.region, args.repo)

    files = [
        f for f in source_dir.rglob("*")
        if f.is_file() and f.suffix in SYNC_EXTENSIONS
    ]

    print(f"📂 Syncing {len(files)} files to {args.repo}/{WORKSPACE}")
    success = failure = 0

    ensure_workspace_exists(api_base, token, WORKSPACE)
    
    for f in sorted(files):
        rel = str(f.relative_to(source_dir))
        content = f.read_bytes()
        ok = write_file(api_base, token, WORKSPACE, rel, content)
        status = "✅" if ok else "❌"
        print(f"  {status} {rel}")
        if ok:
            success += 1
        else:
            failure += 1

    print(f"\n  Synced: {success}/{len(files)} files")
    if failure:
        print(f"  Failed: {failure} file(s)")
        sys.exit(1)


if __name__ == "__main__":
    main()
