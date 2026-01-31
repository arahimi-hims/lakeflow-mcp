import numpy as np
import os
import sys
import pyspark.dbutils
import pyspark.sql


def main():
    print("Python version:", sys.version)
    print(
        "Starting execution for Run ID:",
        os.environ.get("DATABRICKS_RUN_ID", "local_run"),
    )
    print("Script path:", os.path.abspath(__file__))

    if len(sys.argv) > 1:
        custom_param = sys.argv[1]
    else:
        custom_param = "default"

    print("My parameter:", custom_param)

    # The databricks secrets for this package are stored in a scope named after
    # the package.
    dbutils = pyspark.dbutils.DBUtils(pyspark.sql.SparkSession.builder.getOrCreate())
    print(
        "Secret length:",
        len(
            dbutils.secrets.get(
                scope=os.environ["LAKEFLOW_SECRET_SCOPE"], key="TEST_ENV_VAR"
            )
        ),
    )

    # Do some silly computation to show that we have access to numpy.
    print("Numpy result:", np.random.rand(5, 5).sum())
    print("Task completed successfully!")


if __name__ == "__main__":
    main()
