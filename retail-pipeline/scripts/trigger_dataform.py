#!/usr/bin/env python3
"""
trigger_dataform.py
-------------------
Triggers a Dataform workflow invocation and optionally waits for completion.

Usage:
    python3 scripts/trigger_dataform.py \
        --project my-gcp-project \
        --region us-central1 \
        --repo retail-dataform \
        --vars "execution_date=2024-01-15" \
        --wait
"""

import argparse
import sys
import time

import google.auth
import google.auth.transport.requests
import requests


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--project", required=True)
    p.add_argument("--region",  required=True)
    p.add_argument("--repo",    required=True)
    p.add_argument("--vars",    default="", help="key=value pairs, comma-separated")
    p.add_argument("--wait",    action="store_true", help="Block until workflow completes")
    p.add_argument("--timeout", type=int, default=3600, help="Max wait seconds (default 3600)")
    return p.parse_args()


def get_token():
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token


def base_url(project, region, repo):
    return (
        f"https://dataform.googleapis.com/v1beta1"
        f"/projects/{project}/locations/{region}/repositories/{repo}"
    )


# def create_compilation_result(base, token, project, vars_dict):
#     resp = requests.post(
#         f"{base}/compilationResults",
#         headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
#         json={
#             "gitCommitish": "main",
#             "codeCompilationConfig": {
#                 "defaultDatabase": project,
#                 "defaultSchema": "retail_staging",
#                 "vars": vars_dict,
#             },
#         },
#         timeout=(5, 120),
#     )
#     resp.raise_for_status()
#     data = resp.json()
#     name = data["name"]
#     print(f"  Compilation result: {name}")
#     return name


def create_workflow_invocation(base, token, vars_dict):
    resp = requests.post(
        f"{base}/workflowInvocations",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "releaseConfig": f"{base}/releaseConfigs/main",
            "invocationConfig": {
            "vars": vars_dict,
            "transitiveDependenciesIncluded": True
            }
        },
        timeout=(5, 120),
    )
    resp.raise_for_status()
    data = resp.json()
    name = data["name"]
    print(f"  Workflow invocation: {name}")
    return name


def wait_for_completion(base, token, invocation_name, timeout):
    url = f"https://dataform.googleapis.com/v1beta1/{invocation_name}"
    deadline = time.time() + timeout
    poll_interval = 15

    print(f"  Polling every {poll_interval}s (timeout: {timeout}s)...")
    while time.time() < deadline:
        token = get_token()   # Refresh token periodically
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=(5, 120),
        )
        resp.raise_for_status()
        state = resp.json().get("state", "UNKNOWN")
        print(f"  State: {state}")

        if state == "SUCCEEDED":
            print("✅ Dataform workflow completed successfully")
            return
        if state in ("FAILED", "CANCELLED"):
            print(f"❌ Dataform workflow ended with state: {state}")
            sys.exit(1)

        time.sleep(poll_interval)

    print(f"❌ Timed out after {timeout}s waiting for Dataform workflow")
    sys.exit(1)


def main():
    args = parse_args()

    # Parse vars string: "key1=val1,key2=val2"
    vars_dict = {}
    if args.vars:
        for pair in args.vars.split(","):
            if "=" in pair:
                k, v = pair.split("=", 1)
                vars_dict[k.strip()] = v.strip()

    print(f"🚀 Triggering Dataform for project={args.project} repo={args.repo}")
    print(f"   Vars: {vars_dict}")

    token = get_token()
    base = base_url(args.project, args.region, args.repo)

    # compilation_result = create_compilation_result(base, token, args.project, vars_dict)
    invocation_name = create_workflow_invocation(base, token, vars_dict)

    if args.wait:
        wait_for_completion(base, token, invocation_name, args.timeout)
    else:
        print("  Not waiting for completion (--wait not set)")


if __name__ == "__main__":
    main()
