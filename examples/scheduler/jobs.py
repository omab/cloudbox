"""Cloud Scheduler — create, list, pause, resume, delete jobs, and configure retry.

    uv run python examples/scheduler/jobs.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import SCHEDULER_BASE, PROJECT, LOCATION, client, ok

JOB_ID = "example-job"


def main():
    http = client()

    parent = f"projects/{PROJECT}/locations/{LOCATION}"
    job_name = f"{parent}/jobs/{JOB_ID}"

    # Create job: runs every minute, hits a local HTTP endpoint
    ok(http.post(
        f"{SCHEDULER_BASE}/v1/{parent}/jobs",
        json={
            "name": job_name,
            "schedule": "* * * * *",
            "timeZone": "UTC",
            "httpTarget": {
                "uri": "http://localhost:8080/cron",
                "httpMethod": "GET",
            },
        },
    ))
    print(f"Created job: {job_name}")

    # List jobs
    r = ok(http.get(f"{SCHEDULER_BASE}/v1/{parent}/jobs"))
    jobs = r.json().get("jobs", [])
    for job in jobs:
        print(f"  {job['name'].split('/')[-1]:20s}  schedule={job['schedule']}  state={job.get('state', 'ENABLED')}")

    # Get job
    r = ok(http.get(f"{SCHEDULER_BASE}/v1/{job_name}"))
    print(f"\nJob state: {r.json().get('state', 'ENABLED')}")

    # Pause
    ok(http.post(f"{SCHEDULER_BASE}/v1/{job_name}:pause"))
    r = ok(http.get(f"{SCHEDULER_BASE}/v1/{job_name}"))
    print(f"After pause: {r.json().get('state')}")

    # Resume
    ok(http.post(f"{SCHEDULER_BASE}/v1/{job_name}:resume"))
    r = ok(http.get(f"{SCHEDULER_BASE}/v1/{job_name}"))
    print(f"After resume: {r.json().get('state')}")

    # Create a job with retry configuration
    retry_job_name = f"{parent}/jobs/retry-job"
    ok(http.post(
        f"{SCHEDULER_BASE}/v1/{parent}/jobs",
        json={
            "name": retry_job_name,
            "schedule": "0 * * * *",
            "timeZone": "UTC",
            "httpTarget": {"uri": "http://localhost:8080/cron", "httpMethod": "POST"},
            "retryConfig": {
                "retryCount": 5,
                "minBackoffDuration": "5s",
                "maxBackoffDuration": "300s",
                "maxDoublings": 4,
                "maxRetryDuration": "10m",
            },
        },
    ))
    r = ok(http.get(f"{SCHEDULER_BASE}/v1/{retry_job_name}"))
    rc = r.json()["retryConfig"]
    print(f"\nRetry job config: retryCount={rc['retryCount']}, "
          f"minBackoff={rc['minBackoffDuration']}, maxBackoff={rc['maxBackoffDuration']}, "
          f"maxDoublings={rc['maxDoublings']}, maxDuration={rc['maxRetryDuration']}")

    # Cleanup
    http.delete(f"{SCHEDULER_BASE}/v1/{job_name}")
    http.delete(f"{SCHEDULER_BASE}/v1/{retry_job_name}")
    print("Deleted jobs")


if __name__ == "__main__":
    main()
