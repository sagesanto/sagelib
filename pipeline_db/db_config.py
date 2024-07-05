# Sage Santomenna 2023, 2024
# SQLAlchemy database connection and configuration
import sys,os
import json
import logging
from os.path import abspath, join, dirname, pardir, exists
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker, registry

grandparent_dir = abspath(join(dirname(__file__), pardir))
sys.path.append(grandparent_dir)
sys.path.append(dirname(__file__))

# import utils
# from utils import Config
from pipeline_utils import configure_logger
import logging

sys.path.remove(grandparent_dir)
sys.path.remove(dirname(__file__))

DB_PATH = None

_logger = logging.getLogger(__name__)

# dotenv_path = join(dirname(__file__),".env")
# if exists(dotenv_path):
#     from dotenv import load_dotenv
#     load_dotenv(dotenv_path)

# # db path can only be set in the defaults file!
# cfg_path = os.getenv("PIPELINE_DEFAULTS_PATH")
# cfg = Config(cfg_path)

# DB_PATH = cfg("DB_PATH")

# @event.listens_for(Engine, "before_cursor_execute")
# def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
#     conn.info.setdefault("query_start_time", []).append(time.time())
#     print("Start Query: %s", statement)


# @event.listens_for(Engine, "after_cursor_execute")
# def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
#     total = time.time() - conn.info["query_start_time"].pop(-1)
#     print("Query Complete!")
#     print("Total Time: %f", total)


# @event.listens_for(Engine, 'close')
# def receiveClose(dbapi_connection, connection_record):
#     cursor = dbapi_connection.cursor()
#     # cursor.execute("PRAGMA analysis_limit=400")
#     # cursor.execute("PRAGMA optimize")


@event.listens_for(Engine, "connect")
def setSQLitePragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode = MEMORY")
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA temp_store = MEMORY")
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


# def renew_pipeline_db_session():
#     global pipeline_db_session  # man this is bad
#     pipeline_db_session.expire_all()
#     pipeline_db_session.close()
#     pipeline_db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=pipeline_engine, ))

mapper_registry = registry()
pipeline_base = mapper_registry.generate_base()
# pipeline_base = declarative_base()

def configure_db(dbpath):
    logger = configure_logger('DB Config', join(dirname(dbpath),"db_config.log"))

    logger.info("Db Configuration Started")
    SQLALCHEMY_DATABASE_URL = f'sqlite:///{dbpath}'

    pipeline_engine = create_engine(SQLALCHEMY_DATABASE_URL)  # , echo="debug")
    pipeline_autocommit_engine = pipeline_engine.execution_options(isolation_level="AUTOCOMMIT")

    pipeline_db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=pipeline_engine))
    pipeline_read_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=pipeline_engine))
    pipeline_base.query = pipeline_db_session.query_property()

    logger.info("Pipeline Db Session Created")
    return pipeline_db_session, pipeline_engine