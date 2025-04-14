import logging

from dask import compute, delayed
from dask.distributed import Client, LocalCluster

from pyseasters.cli._utils import capture_logging, setup_cli_logging

log = logging.getLogger(__name__)
setup_cli_logging(logging.DEBUG)


@capture_logging()
@delayed
def func(i: int):
    log.debug("Debug statement: %i", i)
    log.info("Info statement: %i", i)
    log.warning("Warning statement: %i", i)
    log.error("Error statement: %i", i)
    log.critical("Critical statement: %i", i)


# res, log_output = func(1)
cluster = LocalCluster(n_workers=2, threads_per_worker=1)
client = Client(cluster)
try:
    log.info("Dask cluster is running.")
    res = compute(func(1), func(2))
finally:
    client.close()
    cluster.close()
    log.info("Dask cluster has been properly shut down.")

print(res)
"""
for line in log_output.split("\n"):
    if not line.strip():
        continue

    level, message = line.strip().split(": ", 1)
    getattr(log, level.lower())(message)

for log_output in [r[1] for r in res]:
    for line in log_output.split("\n"):
        if not line.strip():
            continue

        level, message = line.strip().split(": ", 1)
        getattr(log, level.lower())(message)
"""
