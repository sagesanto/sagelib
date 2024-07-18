import sys, os

if __name__ == "__main__":
    if len(sys.argv)==1:
        print("Usage: graph_run [run id] {optional}[pipeline_db_path]")
        exit(1)

import logging
sys.path.append(os.path.join(os.path.dirname(__file__),os.path.pardir,os.path.pardir))
import matplotlib.pyplot as plt
from sagelib import PipelineRun, Product, TaskRun, pipeline_utils, utils, configure_db
from sagelib.utils import now_stamp, tts, stt, dt_to_utc, current_dt_utc


def graph_run(session, run:PipelineRun):
    # build graph dict
    g = {}
    for i in run.Inputs:
        g[i.ID] = i.traverse_derivatives(lambda s: s.ID)
    print(g)



if __name__ == "__main__":
    run_id = int(sys.argv[1])

    try:
        database_path = sys.argv[2]
    except Exception:
        try:
            cfg_path = os.getenv("PIPELINE_DEFAULTS_PATH")
            cfg = utils._read_config(cfg_path)
            database_path = cfg["DB_PATH"]
        except Exception as e:
            raise ValueError("Either the environment variable 'PIPELINE_DEFAULTS_PATH' must point to a config file containing the key 'DB_PATH' or a database path must be provided as the second argument.") from e 

    logging.basicConfig(level=logging.ERROR)

    session, _ = configure_db(database_path)

    print(run_info(session, run_id))