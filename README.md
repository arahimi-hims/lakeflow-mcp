# Launching jobs on Lakeflow

Lakeflow is Databrick's way to manage jobs on a cluster. Databricks lets you
edit scripts in their UI (they call these "notebooks"). That's too simple for
some our more complex experiments. Thankfully, Databricks supports lots of
other ways to submit jobs.

This tool is an opinionated way to spawn jobs on Databricks: It asks you author
your code as a Python package, which forces you to specific its dependencies. It
then uploads that package (as a python wheel) for Databricks to run it. This is
heavier-weight than Databrick's notebook approach of shipping scripts, but it
lets you capture large package dependencies across repos via git submodules.
It's lighter weight than other job submisison systems that operate on docker
containers. For most of our work, wheels represent all the containerization we
neeed.

It has one more opinion: that `uv` is a good way to capture those Python
dependencies, with a `pyproject.toml`.

Once you have your packaged defined in a `pyproject.toml`, you can use this tool
to build the wheel, upload it to Databricks, and spawn copies of it with
different command line arguments. Databricks gives you a UI to check the state
of your jobs.

The tool provides several interfaces:

- A CLI you can use from the shell.
- An MCP server you can connect to from an AI agent.
- A set of Python functions you can call from a Python program.

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
│   └── my_package/
│       ├── __init__.py
│       └── my_package_py.py
```

It also assumes you've added an entry point to your `pyproject.toml` called
"lakeflow-task". If your package is called `my_package`, and it has a driver
script called `my_package_py.py`, and the main function this script is called
`main`, you would define the "lakeflow-task" entry point like this:

```toml
[project.scripts]
lakeflow-task = "my_package.my_package_py:main"
```

The pakage [`lakeflow_demo`](lakeflow_demo) under this directory gives you a
concrete example of this.

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

3.  **Create the Job**:

    ```bash
    python lakeflow.py create-job \
      "my-lakeflow-job" \
      "my-package" \
      "/Users/me/wheels/my_package-0.1.0-py3-none-any.whl"
    # Output: 123456 (Job ID)
    ```

    This returns the job ID, which we'll use in the next step.
    This doesn't yet run any jobs. It just starts a cluster that can run them.

4.  **Trigger a Run**:

    ```bash
    python lakeflow.py trigger-run 123456 argv1 argv2
    ```

    This starts one instance of the job with the given arguments. If you have
    shards of data, you can call this operation multiple times with different
    arguments to kick off a bunch of jobs in parallel. argv will be populated
    with the arguments, and the environment variable `DATABRICKS_RUN_ID` will be
    populated with the run ID.

5.  **Monitor the Runs**:

    ```bash
    python lakeflow.py list-job-runs 123456
    ```

    This lists the runs for the given job ID.

## Using Python programmatic interface

The above illustrated how to use the CLI. You might find it easier to use the
programmatic interface to the package instead. See
[run_lakeflow_demo.py](run_lakeflow_demo.py) for an example.

## Using the MCP server

You can install this package as an MCP server. To do that, add this to `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "lakeflow": {
      "command": "/Users/arahimi/.local/bin/uv",
      "args": [
        "run",
        "--quiet",
        "--directory",
        "/Users/arahimi/lakeflow-mcp",
        "python",
        "lakeflow.py"
      ],
      "env": {
        "DATABRICKS_HOST": "https://hims-machine-learning-staging-workspace.cloud.databricks.com",
        "DATABRICKS_TOKEN": "<your tocken>>"
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
