import sys, os
from abc import ABC, abstractmethod
import sqlite3
import logging
try:
    from . import pipeline_utils, utils
except:
    import pipeline_utils, utils


MODULE_PATH = os.path.abspath(os.path.dirname(__file__))

def mod(path): return os.path.join(MODULE_PATH,path)


class PipelineDB:
    def __init__(self, dbpath:str, logger):
        self.dbpath = dbpath
        self.logger = logger
        self.connect()
        self.make_tables()

    def connect(self):
        self.conn = sqlite3.connect(database=self.dbpath, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()
    
    def execute(self, sql_statement:str, vals=None):
        if vals:
            self.cur.execute(sql_statement, vals)
        else:
            self.cur.execute(sql_statement)

    def get_next_pipeline_id(self):
        self.execute("SELECT ID FROM PipelineRuns ORDER BY ID DESC; LIMIT 1")
        return self.cur.fetchone()
    
    def make_tables(self):
        task_run_stmnt = """
        CREATE TABLE IF NOT EXISTS "TaskRuns" (
        "TaskName"    TEXT NOT NULL,
        "StartTimeUTC"    TEXT NOT NULL,
        "EndTimeUTC"    TEXT NOT NULL,
        "StatusCodes"   INT NOT NULL,
        "PipelineRunID"     INT NOT NULL,
        "ID"    INT NOT NULL,
        PRIMARY KEY ("ID"), 
        FOREIGN KEY("PipelineRunID") REFERENCES "PipelineRuns" ("ID")
        )"""
        self.execute(task_run_stmnt)

        pipeline_run_stmnt = """
        CREATE TABLE IF NOT EXISTS "PipelineRuns" (
        "PipelineName"    TEXT NOT NULL,
        "PipelineVersion"    TEXT NOT NULL,
        "StartTimeUTC"    TEXT NOT NULL,
        "EndTimeUTC"    TEXT,
        "Config"    TEXT NOT NULL,
        "InputFITS"     TEXT NOT NULL,
        "LogFilepath"   TEXT,
        "ID"    INT NOT NULL,
        PRIMARY KEY ("ID") 
        )"""
        self.execute(pipeline_run_stmnt)

    def record_task(self, taskname, start_dt, end_dt, status_codes, pipeline_run_id):
        start_str = utils.tts(utils.dt_to_utc(start_dt))
        end_str = utils.tts(utils.dt_to_utc(end_dt))

        insert_stmt = "INSERT INTO TaskRuns (TaskName,StartTimeUTC,EndTimeUTC,StatusCodes,PipelineRunID) VALUES (?,?,?,?,?)"
        self.execute(insert_stmt,vals=(taskname,start_str,end_str,status_codes,pipeline_run_id))
    
    def record_pipeline_start(self, pipeline_name, pipeline_version, start_dt, config_str, input_fits_str, log_filepath=None):
        start_str = utils.tts(utils.dt_to_utc(start_dt))
        pipeline_id = self.get_next_pipeline_id()
        if log_filepath:
            insert_stmt = "INSERT INTO TaskRuns (PipelineName,PipelineVersion,StartTimeUTC,Config,InputFITS,LogFilepath,ID) VALUES (?,?,?,?,?,?,?)"
            self.execute(insert_stmt,(pipeline_name,pipeline_version,start_dt,config_str,input_fits_str,log_filepath, pipeline_id))
            return pipeline_id
        insert_stmt = "INSERT INTO TaskRuns (PipelineName,PipelineVersion,StartTimeUTC,Config,InputFITS,ID) VALUES (?,?,?,?,?,?)"
        self.execute(insert_stmt,(pipeline_name,pipeline_version,start_dt,config_str,input_fits_str, pipeline_id))
        return pipeline_id
    
    def record_pipeline_end(self,pipeline_run_id,end_dt):
        end_str = utils.tts(utils.dt_to_utc(end_dt))
        update_stmt = "UPDATE PipelineRuns AS p SET P.EndTimeUTC = ? FROM PipelineRuns AS Pipeline WHERE Pipeline.ID = ?"
        self.execute(update_stmt,(end_str,pipeline_run_id))
    
    def close(self):
        self.conn.close()
    
    def refresh(self):
        self.close()
        self.connect()


class Task(ABC):
    def __init__(self, name, profile, outdir):
        self.name = name
        self.profile = profile
        self.outdir = outdir
        self.logger = None
    
    @abstractmethod
    def __call__(self, fitslist, logfile):
        self.logger = pipeline_utils.configure_logger(self.name, logfile)

    @property
    @abstractmethod
    def required_profile_keys(self):
        pass

    @property
    @abstractmethod
    def description(self):
        pass


class Pipeline:
    def __init__(self, pipeline_name, tasks, fitslist, outdir, profile_name, config_path, dbpath, version):
        self.name = pipeline_name
        self.tasks = tasks
        self.fitslist = fitslist
        self.outdir = outdir
        self.profile_name = profile_name
        self.config = utils.read_config(config_path)[profile_name]
        self.dbpath = dbpath
        self.logfile = os.path.abspath(os.path.join(outdir,f"{self.name}.log"))
        self.logger = pipeline_utils.configure_logger(self.name,self.logfile)
        self.db = PipelineDB(dbpath, self.logger)
        self.version = version
        self.pipeline_id = None
    
    def run(self):
        self.pipeline_id = self.db.record_pipeline_start(self.name,self.version,utils.current_dt_utc(),f"{self.profile_name}: " + str(self.config),str(self.fitslist),self.logfile)
        self.logger.info(f"Beginning run {self.pipeline_id} (pipeline {self.name} v{self.version})")
        for i, task in enumerate(self.tasks):
            start_dt = utils.current_dt_utc()
            self.logger.info(f"Began task {task.name} ({i+1}/{len(self.tasks)}) at {utils.tts(start_dt)} UTC")
            codes = -1
            try:
                codes = task(self.fitslist, self.logfile)
            except Exception as e:
                self.logger.exception(f"Uh oh. Got exception running task {task.name}")
            end_dt = utils.current_dt_utc()
            self.logger.info(f"Finished task {task.name} ({i+1}/{len(self.tasks)}) at {utils.tts(end_dt)} UTC with codes {codes}")
            self.db.record_task(task.name,start_dt,end_dt,codes,self.pipeline_id)
        self.logger.info(f"Finished run {self.pipeline_id} (pipeline {self.name} v{self.version}) at {utils.tts(utils.current_dt_utc())}")
        self.db.record_pipeline_end(self.pipeline_id,utils.current_dt_utc())

if __name__ == "__main__":
    pipeline = Pipeline("test",["test"],"test",".","Subaru","test_config.toml","test.db",0)
    # db = PipelineDB()