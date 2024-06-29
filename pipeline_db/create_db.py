# Sage Santomenna 2024
import os, sys
from os.path import abspath, join, dirname, pardir


sys.path.append(dirname(__file__))
from db_config import configure_db
from models import PipelineInputAssociation, PrecursorProductAssociation, PipelineRun, Product, TaskRun

from sqlalchemy.schema import CreateTable
from sqlalchemy import text

parent_dir = abspath(join(dirname(__file__), pardir))
sys.path.append(parent_dir)

from pipeline_utils import configure_logger

sys.path.remove(parent_dir)
sys.path.remove(dirname(__file__))

def create_db(dbpath):
    logger = configure_logger("DB Creation", join(dirname(dbpath),"db_config.log"))
    
    pipeline_db_session, pipeline_engine = configure_db(dbpath)

    pipeline_stmt = CreateTable(PipelineRun.__table__, if_not_exists=True).compile(pipeline_engine)
    product_stmt = CreateTable(Product.__table__, if_not_exists=True).compile(pipeline_engine)
    taskruns_stmt = CreateTable(TaskRun.__table__, if_not_exists=True).compile(pipeline_engine)

    pipeline_db_session.execute(text(str(pipeline_stmt)))
    logger.info("Configured PipelineRun")
    pipeline_db_session.execute(text(str(product_stmt)))
    logger.info("Configured Product Table")
    pipeline_db_session.execute(text(str(taskruns_stmt)))
    logger.info("Configured TaskRun Table")
    pipeline_db_session.commit()

    pipeline_association_stmt = CreateTable(PipelineInputAssociation, if_not_exists=True).compile(pipeline_engine)
    pipeline_db_session.execute(text(str(pipeline_association_stmt)))
    pipeline_db_session.commit()
    logger.info("Configured Pipeline Association Table")

    precursor_association_stmt = CreateTable(PrecursorProductAssociation, if_not_exists=True).compile(pipeline_engine)
    pipeline_db_session.execute(text(str(precursor_association_stmt)))
    pipeline_db_session.commit()
    logger.info("Configured Precursor Association Table")

    logger.info("Done configuring database")
    logger.info("")
    return pipeline_db_session, pipeline_engine

if __name__ == "__main__":
    create_db(sys.argv[1])