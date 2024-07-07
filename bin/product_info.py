import sys, os

if __name__ == "__main__":
    if len(sys.argv)==1:
        print("Usage: product_info [product filename] {optional}[pipeline_db_path]")
        exit(1)

import logging
sys.path.append(os.path.join(os.path.dirname(__file__),os.path.pardir,os.path.pardir))

from sagelib import PipelineRun, Product, TaskRun, pipeline_utils, utils, configure_db
from sagelib.utils import now_stamp, tts, stt, dt_to_utc, current_dt_utc



def product_info(session, filepath, verbose=False):
    prod = session.query(Product).filter(Product.product_location==filepath).first()
    if not prod:
        raise ValueError(f"Couldn't find a product with filepath '{filepath}'")
    
    lines = []

    derivatives = sorted([p for p in prod.all_derivatives()], key=lambda i: i.producing_pipeline_run_id)
    direct_precursors = [p for p in prod.precursors]
    pipeline = prod.ProducingPipeline


    # summary
    line_1 = f"Product #{prod.ID}: {prod.data_type + (f".{prod.data_subtype}" if prod.data_subtype else '')}"
    section_sep = "=" * len(line_1)
    lines.append(section_sep)
    lines.append(line_1)
    lines.append(section_sep)
    lines.append(f"{len(direct_precursors)} immediate precursors and {len(prod.derivatives)} direct derivatives ({len(derivatives)} total)")
    if prod.is_input:
        lines.append(f"Origin: Input to pipeline run #{pipeline.ID} ({pipeline.PipelineName} v{pipeline.PipelineVersion})")
    else:
        lines.append(f"Origin: Produced by task '{prod.ProducingTask.TaskName}' (ID #{prod.ProducingTask.ID}) as part of pipeline run #{pipeline.ID} ({pipeline.PipelineName} v{pipeline.PipelineVersion})")

    lines.append(f"Created {prod.creation_dt}")
    lines.append(f"{prod.product_location}")
    lines.append("")
    
    # precursors and derivatives
    lines.append(section_sep)
    lines.append("Precursors and Derivatives")
    lines.append(section_sep)
    if len(prod.precursors):
        provenances = {"User Input":0}
        types = {}
        for p in prod.precursors:
            dtype = p.data_type + (f".{p.data_subtype}" if p.data_subtype else '')
            if dtype not in types:
                types[dtype] = 0
            types[dtype] += 1

            if p.is_input:
                provenances["User Input"]+=1
            else:
                pipe_id = f"Run {p.producing_pipeline_run_id}"
                if pipe_id not in provenances:
                    provenances[pipe_id] = 0
                provenances[pipe_id] += 1
            
        lines.append("Precursor Types:")
        lines.extend([f"    {key}: {val}" for key,val in types.items()])
        lines.append("Precursor Provenances:")
        lines.extend([f"    {key}: {val}" for key,val in provenances.items()])
    else:
        lines.append("No direct precursors.")

    lines.append('')

    producers = {}
    output_types = {}
    for d in derivatives:
        dtype = d.data_type + (f".{d.data_subtype}" if d.data_subtype else '')
        if dtype not in output_types:
            output_types[dtype] = 0
        output_types[dtype] += 1

        producer = d.producing_pipeline_run_id
        if producer not in producers:
            producers[producer] = 0
        producers[producer] += 1
        
    lines.append("Derivative Types:")
    lines.extend([f"    {key}: {val}" for key,val in output_types.items()])
    lines.append("Derivative-Producing Pipelines:")
    lines.extend([f"    {key}: {val}" for key,val in producers.items()])
    lines.append('')

    return("\n".join(lines))

if __name__ == "__main__":
    filepath = sys.argv[1]

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

    print(product_info(session, filepath))
