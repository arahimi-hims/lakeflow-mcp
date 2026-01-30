import numpy as np
import os
import sys


def main():
    task_id = os.environ.get("DATABRICKS_RUN_ID", "local_run")
    print(f"Starting execution for Run ID: {task_id}")
    print(f"Python version: {sys.version}")

    if len(sys.argv) > 1:
        custom_param = sys.argv[1]
    else:
        custom_param = "default"

    print("My parameter:", custom_param)
    print("Test Env Var:", os.environ.get("TEST_ENV_VAR"))
    print("Numpy result:", np.random.rand(5, 5).sum())
    print(f"Task {task_id} completed successfully!")


if __name__ == "__main__":
    main()
