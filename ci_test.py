import lakeflow
import os
import time
import sys
import logging


def run_ci_test():
    logging.info("Building wheel...")
    wheel_path = lakeflow.build_wheel("lakeflow_demo")
    logging.info(f"Wheel built at {wheel_path}")

    logging.info("Uploading wheel...")
    remote_path = lakeflow.upload_wheel(wheel_path)
    logging.info(f"Wheel uploaded to {remote_path}")

    logging.info("Creating job...")

    os.environ["TEST_ENV_VAR"] = "test_secret_value"

    # Launch on just one worker
    job_id = lakeflow.create_job(
        f"ci-test-{int(time.time())}",
        "lakeflow_demo",
        remote_path,
        max_workers=1,
        secret_env_vars=("TEST_ENV_VAR",),
    )
    job_id = int(job_id)
    logging.info(f"Job created with ID {job_id}")

    logging.info("Triggering run...")
    run_id = lakeflow.trigger_run(job_id, ["ci_test_param"])
    logging.info(f"Run triggered with ID {run_id}")

    logging.info("Verifying job runs...")
    runs = lakeflow.list_job_runs(job_id)
    if len(runs) != 1:
        logging.error(f"Test FAILED: Expected exactly 1 job run, found {len(runs)}")
        sys.exit(1)

    logging.info("Waiting for run to complete...")
    while True:
        run = lakeflow.workspace.jobs.get_run(run_id)
        state = str(run.state.life_cycle_state)
        logging.info(f"Run status: {state}")

        if state != "RunLifeCycleState.RUNNING":
            break

        time.sleep(30)

    logging.info("Run completed. Fetching logs...")
    logs = lakeflow.get_run_logs(run_id)

    logging.info("Logs: " + "-" * 20)
    logging.info(logs)
    logging.info("-" * 20)

    if "error" in logs.lower():
        logging.error("Test FAILED: 'error' found in logs.")
        sys.exit(1)

    result_state = str(run.state.result_state)
    if result_state != "RunResultState.SUCCESS":
        logging.error(f"Test FAILED: Run finished with state {result_state}")
        sys.exit(1)

    # Secrets get automatically redacted in the logs. Instead, check for the
    # length of the secret.
    if "Secret length: 17" not in logs:
        logging.error(f"Test FAILED: Secret didn't come through. Logs:\n{logs}")
        sys.exit(1)

    logging.info("Test PASSED: No errors in logs and run succeeded.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    run_ci_test()
