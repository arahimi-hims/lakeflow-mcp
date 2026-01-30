import numpy as np
import os
import sys
import pyspark.dbutils
import pyspark.sql


def main():
    dbutils = pyspark.dbutils.DBUtils(pyspark.sql.SparkSession.builder.getOrCreate())

    task_id = os.environ.get("DATABRICKS_RUN_ID", "local_run")
    print(f"Starting execution for Run ID: {task_id}")
    print(f"Python version: {sys.version}")

    if len(sys.argv) > 1:
        custom_param = sys.argv[1]
    else:
        custom_param = "default"

    print("My parameter:", custom_param)

    # The databricks secrets for this package are stored in a scope named after
    # the package.
    print(
        "Secret length:",
        len(dbutils.secrets.get(scope="lakeflow_demo", key="TEST_ENV_VAR")),
    )

    # Do some silly computation to show that we have access to numpy.
    print("Numpy result:", np.random.rand(5, 5).sum())
    print(f"Task {task_id} completed successfully!")


if __name__ == "__main__":
    main()
