# %%
import logging
import time
import lakeflow
from importlib import reload

reload(lakeflow)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Connect to Databricks (workspace initialized in lakeflow)
if lakeflow.workspace:
    print(f"Connected to Databricks at {lakeflow.workspace.config.host}")

# %%
# 1. Build and Upload Python Wheel
remote_wheel_path = lakeflow.upload_wheel(lakeflow.build_wheel("lakeflow_demo"))

# %%
# 2. Define the Lakeflow Job configuration
job_id = lakeflow.create_job(
    job_name=f"lakeflow-demo-wheel-{int(time.time())}",
    package_name="lakeflow_demo",
    remote_wheel_path=remote_wheel_path,
)
job_id = int(job_id)  # create_job returns str now

# %%
# 3. Kick off several copies simultaneously
num_copies = 5
print(f"\nKicking off {num_copies} runs simultaneously...")

for i in range(num_copies):
    lakeflow.trigger_run(job_id, [f"parameter_from_run_{i + 1}"])

# %%
# 4. Monitor results
for run in lakeflow.list_job_runs(job_id=job_id):
    state_info = run.get("state", {})
    state = (
        state_info.get("life_cycle_state")
        if isinstance(state_info, dict)
        else "UNKNOWN"
    ) or "UNKNOWN"
    url = run.get("run_page_url", "N/A")
    run_id = run.get("run_id", "N/A")

    print(f"Run {run_id}: {state} - {url}")
