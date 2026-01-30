# Launching jobs on Lakeflow

This tool is an opinionated way to spawn compute jobs on the cloud. By "compute
job", I mean a massively parallel data processing job like training a deep net,
analyzing a large corpus of text that's sitting in an S3 bucket, or 1000
parallel simulations of something. To let you do these things, this package
asks you to author your code as a Python package and forces you to specify your
package dependencies in a `pyproject.toml`. It then uploads that package (as a
python wheel) for Databricks to execute it.

This is heavier-weight than Databrick's built-in notebook approach of editing a
Python script in their web UI. In return, it lets you capture large package
dependencies across repos via git submodules, and import third party packages
via uv. It's lighter-weight than most other job submission systems because it
doesn't require you to build docker containers. Docker containers take a large
snapshot of your system, enough to build a full unix environment. These
snapshots are on the order of gigabytes and difficult to upload from a home
computer. For most of our work, wheels provide all the containerization we need
(a wheel is a few kilobytes).

It has one more opinion: That `uv` is a good way to capture those Python
dependencies, with a `pyproject.toml`. We're exploring
[Pants](https://www.pantsbuild.org/) as a way to manage more complex packages,
but I've recently fallen in love with [uv](https://github.com/astral-sh/uv).

You can use this tool to build your wheel, upload it to Databricks, spawn copies
of it each with different command line arguments, and track your jobs's status.
You can also use a Databricks UI to check the state of your jobs. The tool
provides several interfaces:

- An MCP server so you can have AIs spawn jobs for you.
- A CLI you can use from the shell.
- A programmatic Python interface you can call from a Python program.

## Getting access to Databricks

Check if you have access to Databrick by visiting [this url](https://hims-machine-learning-staging-workspace.cloud.databricks.com/). If
you get stuck in an infinite loop where Databricks sends you a code that doesn't
work, it means you don't have an account. Ask for one in #help-data-platform.

## Your package's structure

This package assumes the package you want to run on the cluster has a
structure like this and it can be run with `uv run`:

```
my_project/
├── pyproject.toml
├── src/
    └── my_package/
        ├── __init__.py
        └── my_package_py.py
```

It also assumes you've added an entry point to your `pyproject.toml` called
"lakeflow-task". If your package is called `my_package`, and it has a driver
script called `my_package_py.py`, and the main function in this script is called
`main`, you would define the "lakeflow-task" entry point like this:

```toml
[project.scripts]
lakeflow-task = "my_package.my_package_py:main"
```

The package [`lakeflow_demo`](lakeflow_demo) under this directory gives you a
concrete example of how to set up a package.

## Building and launching your package with the CLI

To run the package on the cluster, first build the wheel, then upload
it, then tell Databricks to run it:

1.  **Build the wheel**:

    ```bash
    uv run lakeflow.py build-wheel ~/my_project
    # Output: /path/to/dist/my_package-0.1.0-py3-none-any.whl
    ```

    This outputs the local wheel path, which we'll use in the next step.

2.  **Upload the wheel**:

    ```bash
    uv run lakeflow.py upload-wheel /path/to/dist/my_package-0.1.0-py3-none-any.whl
    # Output: /Users/me/wheels/my_package-0.1.0-py3-none-any.whl
    ```

    This outputs the remote wheel path, which we'll use in the next step.

3.  **Create the job**:

    ```bash
    uv run lakeflow.py create-job \
      "my-lakeflow-job" \
      "my-package" \
      "/Users/me/wheels/my_package-0.1.0-py3-none-any.whl" \
      --max-workers 4
      --env-var MY_SECRET_KEY MY_OTHER_SECRET_KEY
    # Output: 123456 (Job ID)
    ```

    This returns the job ID, which we'll use in the next step. This doesn't yet
    run any jobs. It just starts a cluster that can run them. The
    `--max-workers` argument sets the maximum number of workers for autoscaling.
    You can also pass environment variables to the remote job without leaking secrets
    (like API keys) through your command line. The tool reads the values from your
    local environment and shuttles the values to Databricks.

4.  **Start the job**:

    ```bash
    uv run lakeflow.py trigger-run 123456 arg11 arg12
    uv run lakeflow.py trigger-run 123456 arg21 arg22
    uv run lakeflow.py trigger-run 123456 arg31 arg32
    ```

    This starts three instances of the job with three different sets of
    arguments. You can have the arguments refer to different shards of data, and
    kick off as many parallel jobs as you want. Your job can retrieve these
    arguments through argv. It can retrieve its job id from the environment
    variable `DATABRICKS_RUN_ID`.

5.  **Monitor the runs**:

    ```bash
    uv run lakeflow.py list-job-runs 123456
    ```

    This lists the runs for the given job ID.

6.  **Get Run Logs**:

    ```bash
    uv run lakeflow.py get-run-logs 987654321
    ```

    This retrieves the logs for a specific run ID. It takes the run returned by `trigger-run`.

## Using Python programmatic interface

The above illustrated how to use the CLI. You might find it easier to use the
programmatic Python interface to the package instead. See
[run_lakeflow_demo.py](run_lakeflow_demo.py) for an example.

## Using the MCP server

You can install this package as an MCP server. To do that, add this to `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "lakeflow": {
      "command": "uv",
      "args": [
        "run",
        "--quiet",
        "--directory",
        "/path/to/lakeflow-mcp",
        "python",
        "lakeflow.py"
      ],
      "env": {
        "DATABRICKS_HOST": "https://hims-machine-learning-staging-workspace.cloud.databricks.com",
        "DATABRICKS_TOKEN": "<your token>"
      }
    },
    ...
  }
}
```

Then you can ask the agent to do things like this:

```
let's launch 4 copies of this job on lakeflow, and pass them the arguments "fi", "fie", "fo", and "fum" respectively.
```
