import argparse
import os
import sys

import numpy as np
import pyspark.dbutils
import pyspark.sql


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lakeflow-secret-scope")
    parser.add_argument("custom_param", nargs="?", default="default")
    args = parser.parse_args()

    print("Python version:", sys.version)
    print(
        "Starting execution for Run ID:",
        os.environ.get("DATABRICKS_RUN_ID", "local_run"),
    )
    print("Script path:", os.path.abspath(__file__))
    print("My parameter:", args.custom_param)

    dbutils = pyspark.dbutils.DBUtils(pyspark.sql.SparkSession.builder.getOrCreate())
    print(
        "Secret length:",
        len(
            dbutils.secrets.get(
                scope=args.lakeflow_secret_scope, key="TEST_ENV_VAR"
            )
        ),
    )

    # Do some silly computation to show that we have access to numpy.
    print("Numpy result:", np.random.rand(5, 5).sum())
    print("Task completed successfully!")


if __name__ == "__main__":
    main()
