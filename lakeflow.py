#! /usr/bin/env python3
from typing import Annotated, List, Optional
import glob
import logging
import os
import shutil
import subprocess
import sys
import time

import databricks.sdk
import databricks.sdk.errors
import databricks.sdk.service.compute
import databricks.sdk.service.jobs
import databricks.sdk.service.workspace
import typer
import mcp.server.fastmcp

app = typer.Typer()

mcp = mcp.server.fastmcp.FastMCP(
    "lakeflow",
    instructions="""To use this server:
1. Build the wheel using build_wheel().
2. Upload the wheel using upload_wheel().
3. Create or reuse a cluster with create_cluster(), or use an existing cluster ID.
4. Create the job with create_job(cluster_id=...) and trigger it with trigger_run().
   Alternatively, use create_job_from_source() to do steps 1-4 in one go
   (it creates a cluster automatically when cluster_id is not provided).
5. Run a copy by calling trigger_run() again.
6. Run multiple copies with different parameters using trigger_run(job_id, ["arg1", "arg2"]).
7. Pass secrets via trigger_run(job_id, [...], secret_env_vars=["MY_KEY"]).
8. Get a list of running jobs using list_job_runs().""",
)
logger = logging.getLogger(__name__)

try:
    workspace = databricks.sdk.WorkspaceClient()
except Exception as e:
    logging.error(
        f"Failed to initialize Databricks WorkspaceClient: {e}. Ensure DATABRICKS_HOST and DATABRICKS_TOKEN are set."
    )
    sys.exit(1)


def export(func):
    """Decorator to register a function as both an MCP tool and a CLI command."""
    mcp.tool()(func)
    app.command()(func)
    return func


@export
def build_wheel(target: Annotated[str, typer.Argument()] = ".") -> str:
    """Builds the Python wheel using 'uv build --wheel'.

    Args:
        target: The path to the directory containing pyproject.toml. To avoid
            confusing uv, it's best to use absolute paths.

    Returns:
        The path to the generated wheel file.
    """
    if not os.path.exists(os.path.join(target, "pyproject.toml")):
        raise ValueError(
            f"Target directory '{target}' does not contain pyproject.toml."
        )

    dist_dir = os.path.join(target, "dist")
    shutil.rmtree(dist_dir, ignore_errors=True)

    subprocess.run("uv build --wheel", cwd=target, shell=True, check=True)

    try:
        return glob.glob(os.path.join(dist_dir, "*.whl"))[0]
    except IndexError:
        raise FileNotFoundError(f"No wheel found in {dist_dir} after build.")


@export
def upload_wheel(local_path: str) -> str:
    """Uploads a local wheel file to the Databricks workspace.

    Args:
        local_path: The local path to the wheel file.

    Returns:
        The full remote path of the uploaded wheel.
    """
    username = workspace.current_user.me().user_name
    filename = os.path.basename(local_path)

    remote_path = f"/Users/{username}/wheels/{filename}"
    logger.info(f"Uploading to {remote_path}")

    # Ensure directory existsdo
    workspace.workspace.mkdirs(os.path.dirname(remote_path))

    with open(local_path, "rb") as f:
        workspace.workspace.upload(
            path=remote_path,
            content=f,
            format=databricks.sdk.service.workspace.ImportFormat.AUTO,
            overwrite=True,
        )

    logger.info("Wheel uploaded successfully.")
    return remote_path


def get_smallest_node_type() -> str:
    """Finds the smallest available node type with more than 2GB of memory."""
    node_types = workspace.clusters.list_node_types().node_types
    suitable_nodes = [n for n in node_types if n.memory_mb > 2048]
    smallest_node = min(suitable_nodes, key=lambda x: x.memory_mb).node_type_id
    logger.info(f"Selected Node Type: {smallest_node}")
    return smallest_node


def put_secret_safe(scope: str, key: str, value: str):
    """Uploads a secret to Databricks Secrets, creating the scope if needed."""
    try:
        workspace.secrets.create_scope(scope=scope)
    except databricks.sdk.errors.ResourceAlreadyExists:
        pass

    workspace.secrets.put_secret(scope=scope, key=key, string_value=value)
    logger.info(f"Secret '{key}' uploaded to scope '{scope}'")


@export
def ensure_cluster_running(cluster_id: str) -> str:
    """Starts the cluster if it's not already running and waits until it's ready."""
    logger.info(f"Ensuring cluster {cluster_id} is running...")
    workspace.clusters.ensure_cluster_is_running(cluster_id)
    state = workspace.clusters.get(cluster_id).state.value
    logger.info(f"Cluster {cluster_id} is {state}")
    return state


@export
def create_cluster(max_workers: int = 4) -> str:
    """Creates a new Databricks cluster for running jobs.

    Args:
        max_workers: The maximum number of workers for autoscaling.

    Returns:
        The cluster ID of the created cluster.
    """
    logger.info("Creating cluster...")
    cluster = workspace.clusters.create_and_wait(
        cluster_name=f"lakeflow-{int(time.time())}",
        spark_version=workspace.clusters.select_spark_version(long_term_support=True),
        node_type_id=get_smallest_node_type(),
        data_security_mode=databricks.sdk.service.compute.DataSecurityMode.SINGLE_USER,
        autoscale=databricks.sdk.service.compute.AutoScale(
            min_workers=1, max_workers=max_workers
        ),
        aws_attributes=databricks.sdk.service.compute.AwsAttributes(
            ebs_volume_count=1, ebs_volume_size=32
        ),
    )
    logger.info(f"Cluster created: {cluster.cluster_id}")
    return cluster.cluster_id


@export
def create_job(
    job_name: str,
    package_name: str,
    remote_wheel_path: str,
    cluster_id: str,
    max_concurrent_runs: Optional[int] = None,
) -> str:
    """Creates a Databricks job with the specified wheel and entry point.

    The job runs on the given existing cluster (starting it first if necessary).
    Use create_cluster() to provision a cluster if you don't already have one.

    Args:
        job_name: The name of the job to create.
        package_name: The name of the Python package.
        remote_wheel_path: The remote path to the uploaded wheel file.
        cluster_id: The cluster to run the job on. The cluster is started if
            it's not running.
        max_concurrent_runs: The maximum number of concurrent runs for the job.

    Returns:
        The ID of the created job.
    """
    logger.info(f"Creating job: {job_name}")

    if not remote_wheel_path.startswith("/"):
        raise ValueError(
            f"remote_wheel_path must start with '/', got: {remote_wheel_path}"
        )

    ensure_cluster_running(cluster_id)

    task_config = dict(
        task_key="wheel_task",
        existing_cluster_id=cluster_id,
        python_wheel_task=databricks.sdk.service.jobs.PythonWheelTask(
            entry_point="lakeflow-task",
            package_name=package_name,
        ),
        libraries=[
            databricks.sdk.service.compute.Library(whl=f"/Workspace{remote_wheel_path}")
        ],
    )

    if max_concurrent_runs is None or max_concurrent_runs == -1:
        cluster_info = workspace.clusters.get(cluster_id)
        if cluster_info.autoscale:
            num_workers = cluster_info.autoscale.max_workers
        else:
            num_workers = cluster_info.num_workers or 1
        max_concurrent_runs = num_workers * 8

    created_job = workspace.jobs.create(
        name=job_name,
        max_concurrent_runs=max_concurrent_runs,
        tasks=[databricks.sdk.service.jobs.Task(**task_config)],
    )

    logger.info(f"View Job: {workspace.config.host}/#job/{created_job.job_id}")
    logger.info(f"Job ID: {created_job.job_id}")
    return str(created_job.job_id)


@export
def create_job_from_source(
    job_name: str,
    package_name: str,
    target: Annotated[str, typer.Option("--target")] = ".",
    max_workers: int = 4,
    max_concurrent_runs: Optional[int] = None,
    cluster_id: Optional[str] = None,
) -> str:
    """Builds wheel, uploads it, and creates a Databricks job in one go.

    When cluster_id is provided, the job targets that existing cluster instead
    of creating a new one. Otherwise a new cluster is created automatically.
    """
    return create_job(
        job_name=job_name,
        package_name=package_name,
        remote_wheel_path=upload_wheel(build_wheel(target)),
        cluster_id=cluster_id or create_cluster(max_workers),
        max_concurrent_runs=max_concurrent_runs,
    )


@export
def trigger_run(
    job_id: int,
    job_args: Annotated[List[str], typer.Argument()] = None,
    secret_env_vars: Annotated[List[str], typer.Option("--secret-env-var")] = [],
) -> int:
    """Triggers a run of the specified job.

    When secret_env_vars is provided, the values are read from the local
    environment, uploaded to Databricks Secrets under a scope named after the
    job, and ``--lakeflow-secret-scope <scope>`` is prepended to the task's
    command-line arguments so the task can locate its secrets.

    Args:
        job_id: The ID of the job to run.
        job_args: A list of Python parameters to pass to the run.
        secret_env_vars: Environment variable names whose values should be
            uploaded to Databricks Secrets and made available to the task.
    """
    if job_args is None:
        job_args = []
    if secret_env_vars:
        scope = f"lakeflow-job-{job_id}"
        for var in secret_env_vars:
            put_secret_safe(scope=scope, key=var, value=os.environ[var])
        job_args = ["--lakeflow-secret-scope", scope] + list(job_args)
    run = workspace.jobs.run_now(job_id=job_id, python_params=job_args)
    logger.info(f" - Started Run ID {run.run_id}")
    return run.run_id


@export
def get_run_logs(run_id: int) -> str:
    """Retrieves the driver logs for a specific run.

    Args:
        run_id: The ID of the run.

    Returns:
        The logs as a string.
    """
    # First, get the task run ID from the job run ID
    job_run = workspace.jobs.get_run(run_id)
    if job_run.tasks:
        id = job_run.tasks[0].run_id
    else:
        id = run_id

    logs = workspace.jobs.get_run_output(id).logs
    logging.info(logs)
    return logs


@export
def list_job_runs(job_id: int) -> List[dict]:
    """Lists runs for a specific job.

    Args:
        job_id: The ID of the job to list runs for.
    """
    runs = list(workspace.jobs.list_runs(job_id=job_id, expand_tasks=False))
    logger.info(
        "\n".join(
            f"{r.run_id}: {r.state.life_cycle_state} - {r.run_page_url}" for r in runs
        )
    )
    return [r.as_dict() for r in runs]


if __name__ == "__main__":
    if len(sys.argv) > 1:
        app()
    else:
        mcp.run()
