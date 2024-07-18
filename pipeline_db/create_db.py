# Sage Santomenna 2024
import os, sys
from os.path import abspath, join, dirname, pardir

sys.path.append(dirname(__file__))


from sqlalchemy.schema import CreateTable
from sqlalchemy import text

parent_dir = abspath(join(dirname(__file__), pardir))
sys.path.append(parent_dir)

from pipeline_utils import configure_logger


def create_db(dbpath):
    from db_config import configure_db
    from models import PipelineInputAssociation, PrecursorProductAssociation, ProductProductGroupAssociation, SupersessorAssociation, PipelineRun, Product, TaskRun, Metadata, ProductGroup
    logger = configure_logger("DB Creation", join(dirname(dbpath),"db_config.log"))
    
    if os.path.exists(dbpath):
        logger.info(f"Removing existing {dbpath}")
        os.remove(dbpath)

    pipeline_db_session, pipeline_engine = configure_db(dbpath)

    pipeline_stmt = CreateTable(PipelineRun.__table__, if_not_exists=True).compile(pipeline_engine)
    product_stmt = CreateTable(Product.__table__, if_not_exists=True).compile(pipeline_engine)
    taskruns_stmt = CreateTable(TaskRun.__table__, if_not_exists=True).compile(pipeline_engine)
    metadata_stmt = CreateTable(Metadata.__table__, if_not_exists=True).compile(pipeline_engine)
    product_group_stmt = CreateTable(ProductGroup.__table__, if_not_exists=True).compile(pipeline_engine)
    precursor_stmt = CreateTable(PrecursorProductAssociation.__table__, if_not_exists=True).compile(pipeline_engine)
    supersessor_stmt = CreateTable(SupersessorAssociation.__table__, if_not_exists=True).compile(pipeline_engine)

    pipeline_db_session.execute(text(str(pipeline_stmt)))
    logger.info("Configured PipelineRun")
    pipeline_db_session.execute(text(str(product_stmt)))
    logger.info("Configured Product Table")
    pipeline_db_session.execute(text(str(taskruns_stmt)))
    logger.info("Configured TaskRun Table")
    pipeline_db_session.execute(text(str(metadata_stmt)))
    logger.info("Configured Metadata Table")
    pipeline_db_session.execute(text(str(product_group_stmt)))
    logger.info("Configured ProductGroup Table")
    pipeline_db_session.execute(text(str(precursor_stmt)))
    logger.info("Configured Precursor Table")
    pipeline_db_session.execute(text(str(supersessor_stmt)))
    logger.info("Configured Supersession Table")
    pipeline_db_session.commit()

    pipeline_association_stmt = CreateTable(PipelineInputAssociation, if_not_exists=True).compile(pipeline_engine)
    pipeline_db_session.execute(text(str(pipeline_association_stmt)))
    logger.info("Configured Pipeline Association Table")    
    p_group_association_stmt = CreateTable(ProductProductGroupAssociation, if_not_exists=True).compile(pipeline_engine)
    pipeline_db_session.execute(text(str(p_group_association_stmt)))
    logger.info("Configured Product-ProductGroup Association Table")


    pipeline_db_session.commit()

    # precursor_association_stmt = CreateTable(PrecursorProductAssociation, if_not_exists=True).compile(pipeline_engine)
    # pipeline_db_session.execute(text(str(precursor_association_stmt)))
    # pipeline_db_session.commit()
    # logger.info("Configured Precursor Association Table")

    logger.info("Done configuring database")
    return pipeline_db_session, pipeline_engine

if __name__ == "__main__":
    create_db(sys.argv[1])