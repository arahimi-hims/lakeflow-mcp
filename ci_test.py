import lakeflow
import time
import sys


def run_ci_test():
    print("Building wheel...")
    wheel_path = lakeflow.build_wheel("lakeflow_demo")
    print(f"Wheel built at {wheel_path}")

    print("Uploading wheel...")
    remote_path = lakeflow.upload_wheel(wheel_path)
    print(f"Wheel uploaded to {remote_path}")

    print("Creating job...")
    # Launch on just one worker
    job_id = lakeflow.create_job(
        f"ci-test-{int(time.time())}", "lakeflow_demo", remote_path, max_workers=1
    )
    job_id = int(job_id)
    print(f"Job created with ID {job_id}")

    print("Triggering run...")
    run_id = lakeflow.trigger_run(job_id, ["ci_test_param"])
    print(f"Run triggered with ID {run_id}")

    print("Waiting for run to complete...")
    while True:
        run = lakeflow.workspace.jobs.get_run(run_id)
        state = str(run.state.life_cycle_state)
        print(f"Run status: {state}")

        if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            break

        time.sleep(10)

    print("Run completed. Fetching logs...")
    logs = lakeflow.get_run_logs(run_id)

    print("Logs retrieved.")
    print("-" * 20)
    print(logs)
    print("-" * 20)

    if "error" in logs.lower():
        print("Test FAILED: 'error' found in logs.")
        sys.exit(1)

    result_state = str(run.state.result_state)
    if result_state != "SUCCESS":
        print(f"Test FAILED: Run finished with state {result_state}")
        sys.exit(1)

    print("Test PASSED: No errors in logs and run succeeded.")


if __name__ == "__main__":
    run_ci_test()
