#!/usr/bin/env python3

import argparse
import google.auth
import google.auth.transport.requests
import requests


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--project", required=True)
    p.add_argument("--region", required=True)
    p.add_argument("--repo", required=True)
    return p.parse_args()


def get_token():
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token


def base_url(project, region, repo):
    return (
        "https://dataform.googleapis.com/v1beta1/"
        f"projects/{project}/locations/{region}/repositories/{repo}"
    )

def run_workflow(project, region, repo, token):
    url = f"{base_url(project, region, repo)}/workflowInvocations"

    payload = {
        "workflowConfig": (
            f"projects/{project}/locations/{region}/repositories/{repo}/"
            "workflowConfigs/default"
        )  # MUST exist in Dataform UI
    }

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=120,
    )

    print(resp.text)

    resp.raise_for_status()
    return resp.json()["name"]


def main():
    args = parse_args()

    print("🚀 Triggering Dataform workflow...")
    token = get_token()
    # base = base_url(args.project, args.region, args.repo)

    invocation = run_workflow(args.project, args.region, args.repo, token)

    print(f"✅ Started workflow: {invocation}")


if __name__ == "__main__":
    main()