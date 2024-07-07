import sys, os

if __name__ == "__main__":
    if len(sys.argv)==1:
        print("Usage: run_info [run id] {optional}[pipeline_db_path]")
        exit(1)

import logging
sys.path.append(os.path.join(os.path.dirname(__file__),os.path.pardir,os.path.pardir))

from sagelib import PipelineRun, Product, TaskRun, pipeline_utils, utils, configure_db
from sagelib.utils import now_stamp, tts, stt, dt_to_utc, current_dt_utc



def run_info(session, run_id, verbose=False):
    run = session.query(PipelineRun).filter(PipelineRun.ID==run_id).first()
    if not run:
        raise ValueError(f"Couldn't find a pipeline run with ID {run_id}")
    
    lines = []

    outputs = [o for o in run.OutputProducts if not o.is_input]
    
    # summary
    line_1 = f"Pipeline Run #{run_id}: '{run.PipelineName}' v{run.PipelineVersion}"
    section_sep = "=" * len(line_1)
    lines.append(section_sep)
    lines.append(line_1)
    lines.append(section_sep)
    start, end = run.StartTimeUTC,run.EndTimeUTC
    duration = "Unknown"
    if start and end:
        duration = stt(end) - stt(start)
        end += " UTC"
    else:
        end = "Unknown"
    start += " UTC"
    lines.append(f"Start: {start}, End {end}, Duration: {duration}")
    lines.append(f"{len(run.TaskRuns)} tasks run, {len(run.Inputs)} inputs, {len(outputs)} outputs")
    lines.append(f"Logfile: {run.LogFilepath}")
    lines.append("")
    lines.append(section_sep)
    
    # tasks
    num_fail, num_crash = len(run.FailedTasks), len(run.CrashedTasks)
    num_success = len(run.TaskRuns) - (len(run.FailedTasks) + len(run.CrashedTasks))
    lines.append(f"Tasks: {num_success} successful, {num_fail} failed, {num_crash} crashed")
    lines.append(section_sep)
    longest_name_len = max([len(t.TaskName) for t in run.TaskRuns])
    num_pad = 2 + len(str(len(run.TaskRuns)+1)) + 1 # 1 for the '#', 1 for the ':' and 1 for the space, plus len of biggest number
    name_pad = longest_name_len

    for i, task in enumerate(run.TaskRuns):
        start, end = task.StartTimeUTC,task.EndTimeUTC
        duration = stt(end) - stt(start)
        status = "Success"
        if task.StatusCodes == -1:
            status = "CRASHED"
        elif task.StatusCodes:
            status = f"FAILED (code {task.StatusCodes})"
        num = f"#{i+1}:".rjust(num_pad)
        name = task.TaskName.ljust(name_pad)
        lines.append(f"{num} {name}\t{start} - {end} UTC ({duration})    {status}")
    lines.append('')

    # inputs and outputs
    lines.append(section_sep)
    lines.append("Inputs and Outputs")
    lines.append(section_sep)
    provenances = {"User Input":0}
    types = {}
    for i in run.Inputs:
        dtype = i.data_type + (f".{i.data_subtype}" if i.data_subtype else '')
        if dtype not in types:
            types[dtype] = 0
        types[dtype] += 1

        if i.is_input:
            provenances["User Input"]+=1
        else:
            pipe_id = f"Run {i.producing_pipeline_run_id}"
            if pipe_id not in provenances:
                provenances[pipe_id] = 0
            provenances[pipe_id] += 1
        
    lines.append("Input Types:")
    lines.extend([f"    {key}: {val}" for key,val in types.items()])
    lines.append("Input Provenances:")
    lines.extend([f"    {key}: {val}" for key,val in provenances.items()])
    lines.append('')

    producers = {}
    output_types = {}
    for i in outputs:
        dtype = i.data_type + (f".{i.data_subtype}" if i.data_subtype else '')
        if dtype not in output_types:
            output_types[dtype] = 0
        output_types[dtype] += 1

        producer = i.task_name
        if producer not in producers:
            producers[producer] = 0
        producers[producer] += 1
        
    lines.append("Output Types:")
    lines.extend([f"    {key}: {val}" for key,val in output_types.items()])
    lines.append("Producing Tasks:")
    lines.extend([f"    {key}: {val}" for key,val in producers.items()])
    lines.append('')

    #config
    lines.append(section_sep)
    lines.append("Config")
    cfg_str = ""
    indent_count = -1
    for char in str(run.Config):
        if char == "{":
            indent_count += 1
            cfg_str += "\n" + "\t" * indent_count
            continue
        if char == ",":
            cfg_str += "\n" + "\t" * indent_count
            continue
        if char == "}":
            indent_count -= 1
            cfg_str += "\n" + ("\t" * indent_count)
            # cfg_str += "\t" * indent_count
            continue
        if char == "\n":
            cfg_str+= "\n"+ ("\t" * indent_count)
            continue
        cfg_str += char

    lines.append(section_sep)

    lines.append(cfg_str)

    lines.append('')
    return("\n".join(lines))

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
