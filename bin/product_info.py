import sys, os
args = None
import argparse
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Show summary of pipeline product')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-f','--filepath', action="store", type=str, help="Filepath of the product to inspect")
    group.add_argument('-i', '--id', action="store", type=int, help="ID of the product to inspect")
    parser.add_argument('-v','--visualize',action="store_true",default=False)
    parser.add_argument('-d', '--database', type=str, help='optional path to database to use for lookup.')
    args = parser.parse_args()

import logging
sys.path.append(os.path.join(os.path.dirname(__file__),os.path.pardir,os.path.pardir))
from sagelib import Product, utils, configure_db


def product_info(session, filepath:str|None, prod_id:str|None=None):
    if filepath:
        prod = session.query(Product).filter(Product.product_location==filepath).first()
        if not prod:
            filepath = os.path.abspath(filepath)
            prod = session.query(Product).filter(Product.product_location==filepath).first()
            if not prod:
                raise ValueError(f"Couldn't find a product with filepath '{filepath}'")
    else:
        prod = session.query(Product).filter(Product.ID==prod_id).first()
        if not prod:
            raise ValueError(f"Couldn't find a product with ID '{prod_id}'")
    
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
    if output_types:
        lines.append("Derivative Types:")
        lines.extend([f"    {key}: {val}" for key,val in output_types.items()])
        lines.append("Derivative-Producing Pipelines:")
        lines.extend([f"    {key}: {val}" for key,val in producers.items()])
    else:
        lines.append("No derivatives.")
    lines.append('')
    lines.append(section_sep)
    lines.append("Metadata")
    lines.append(section_sep)
    meta_dict = prod.metadata_dict()
    if meta_dict:
        lines.extend([f"    {key}: {val}" for key,val in meta_dict.items()])
    else:
        lines.append("(No Metadata)")
    lines.append(" ")
        

    return("\n".join(lines)), prod

if __name__ == "__main__":
    filepath = args.filepath
    prod_id = args.id
    visualize = args.visualize
    database_path = args.database


    if not database_path:
        try:
            cfg_path = os.getenv("PIPELINE_DEFAULTS_PATH")
            cfg = utils._read_config(cfg_path)
            database_path = cfg["DB_PATH"]
        except Exception as e:
            raise ValueError("Either the environment variable 'PIPELINE_DEFAULTS_PATH' must point to a config file containing the key 'DB_PATH' or a database path must be provided with -d.") from e 

    logging.basicConfig(level=logging.ERROR)

    session, _ = configure_db(database_path)
    info, product = product_info(session, filepath, prod_id)
    print(info)
    if visualize:
        import matplotlib.pyplot as plt
        fig, (ax1,ax2) = plt.subplots(1,2)
        product.visualize_precursors(fig=fig,ax=ax1)
        product.visualize_derivatives(fig=fig,ax=ax2)
        plt.show()
