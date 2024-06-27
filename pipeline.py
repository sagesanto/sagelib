import sys, os
from abc import ABC, abstractmethod
import sqlite3
import logging
import logging.config

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
        self.execute("SELECT ID FROM PipelineRuns ORDER BY ID DESC LIMIT 1")
        res = self.cur.fetchone()
        if res is None:
            res = -1
        else:
            res = res["ID"]
        return res + 1
    
    def make_tables(self):
        task_run_stmnt = """
        CREATE TABLE IF NOT EXISTS "TaskRuns" (
        "TaskName"    TEXT NOT NULL,
        "StartTimeUTC"    TEXT NOT NULL,
        "EndTimeUTC"    TEXT NOT NULL,
        "StatusCodes"   INT NOT NULL,
        "PipelineRunID"     INT NOT NULL,
        "ID"    INTEGER PRIMARY KEY,
        FOREIGN KEY("PipelineRunID") REFERENCES "PipelineRuns" ("ID")
        )"""
        self.execute(task_run_stmnt)

        pipeline_run_stmnt = """
        CREATE TABLE IF NOT EXISTS "PipelineRuns" (
        "PipelineName"    TEXT NOT NULL,
        "StartTimeUTC"    TEXT NOT NULL,
        "EndTimeUTC"      TEXT,
        "Success"         INT,
        "FailedTasks"     TEXT,
        "CrashedTasks"     TEXT,
        "PipelineVersion"    TEXT NOT NULL,
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
        self.conn.commit()

    def record_pipeline_start(self, pipeline_name, pipeline_version, start_dt, config, input_fits, log_filepath=None):
        start_str = utils.tts(utils.dt_to_utc(start_dt))
        config_str = str(config)
        input_fits_str = str(input_fits)
        pipeline_id = self.get_next_pipeline_id()
        if log_filepath:
            insert_stmt = "INSERT INTO PipelineRuns (PipelineName,PipelineVersion,StartTimeUTC,Config,InputFITS,LogFilepath,ID) VALUES (?,?,?,?,?,?,?)"
            self.execute(insert_stmt,(pipeline_name,pipeline_version,start_str,config_str,input_fits_str,log_filepath,pipeline_id))
            return pipeline_id
        insert_stmt = "INSERT INTO PipelineRuns (PipelineName,PipelineVersion,StartTimeUTC,Config,InputFITS,ID) VALUES (?,?,?,?,?,?)"
        self.execute(insert_stmt,(pipeline_name,pipeline_version,start_str,config_str,input_fits_str, pipeline_id))
        self.conn.commit()
        return pipeline_id
    
    def record_pipeline_end(self,pipeline_run_id,end_dt, success, failed, crashed):
        end_str = utils.tts(utils.dt_to_utc(end_dt))
        failed = ",".join(failed)
        crashed = ",".join(crashed)
        update_stmt = "UPDATE PipelineRuns SET EndTimeUTC = ?, Success = ?, FailedTasks = ?, CrashedTasks = ? WHERE ID = ?"
        self.execute(update_stmt, (end_str, success, failed, crashed, pipeline_run_id))
        self.conn.commit()
    
    def close(self):
        self.conn.close()
    
    def refresh(self):
        self.close()
        self.connect()


class Task(ABC):
    def __init__(self, name):
        self.name = name
        self.config = None
        self.outdir = None
        self.logger = None

    @abstractmethod
    def __call__(self, fitslist, outdir, config, logfile, pipeline_run_id) -> int:
        self.logfile = logfile
        self.logger = pipeline_utils.configure_logger(self.name, self.logfile)
        self.fitslist, self.outdir, self.config, self.pipeline_run_id = fitslist, outdir, config, pipeline_run_id

    def outpath(self, file): return os.path.join(self.outdir, file)

    @property
    @abstractmethod
    def required_params(self):
        """Return a list of keys that must be in the config or set by a prior pipeline task for this one to work"""
        pass

    @property
    @abstractmethod
    def description(self):
        pass

    @property
    @abstractmethod
    def will_set(self):
        """Return a list of keys in the config that this task will set while running"""
        pass



class Pipeline:
    def __init__(self, pipeline_name, tasks, fitslist, outdir, profile_name, config_path, version, default_cfg_path=None):
        self.name = pipeline_name
        self.tasks = tasks
        self.fitslist = fitslist
        self.outdir = outdir
        os.makedirs(outdir,exist_ok=True)
        self.profile_name = profile_name
        self.config = utils.Config(config_path, "PIPELINE_DEFAULTS_PATH") # this is the global config in the file
        self.config.choose_profile(profile_name) # this is the scoped config in the file
        self.logfile = os.path.abspath(os.path.join(outdir,f"{self.name}.log"))
        self.logger = pipeline_utils.configure_logger(self.name,self.logfile)
        self.dbpath = self.config("DB_PATH")
        self.db = PipelineDB(self.dbpath, self.logger)
        self.version = version
        self.failed = []
        self.crashed = []
        self.pipeline_id = None
        self.success = None
        if default_cfg_path:
            self.config.load_defaults(default_cfg_path)

        self.validate_pipeline()
    
    def validate_pipeline(self):
        # check configuration keys
        missing = {}
        req = self.get_required_keys()
        set_by_tasks = []
        for task, keys in self.get_required_keys().items():
            for key in keys:
                try:
                    self.config[key]
                except Exception:
                    # this means we didn't find the key in the config. 
                    # this is ok if the key will instead be set by a task that runs before this one
                    if key not in set_by_tasks:
                        if missing.get(task.name):
                            missing[task.name].append(key)
                        else:
                            missing[task.name] = [key]
            will_be_set = task.will_set
            if will_be_set:
                if isinstance(will_be_set,str):
                    set_by_tasks.append(will_be_set)
                else:
                    set_by_tasks.extend(will_be_set)

        if missing:
            raise AttributeError(f"Tasks are missing config keys: {missing}")

    def get_required_keys(self):
        keywords = {}
        for task in self.tasks:
            keywords[task] = task.required_params
        return keywords

    
    def run(self):
        self.succeeded = []
        self.failed = []
        self.crashed = []
        self.pipeline_id = None
        self.success = None
        self.pipeline_id = self.db.record_pipeline_start(self.name,self.version,utils.current_dt_utc(),self.config,self.fitslist,self.logfile)
        self.logger.info(f"Beginning run {self.pipeline_id} (pipeline {self.name} v{self.version})")
        for i, task in enumerate(self.tasks):
            start_dt = utils.current_dt_utc()
            self.logger.info(f"Began task '{task.name}' ({i+1}/{len(self.tasks)})")
            codes = -1
            try:
                codes = task(self.fitslist, self.outdir, self.config, self.logfile, self.pipeline_id)
                if codes != 0:
                    self.logger.error(f"Got nonzero exit code from task {task.name}: {codes}! Exiting.")
                    self.failed.append(task.name)
                    break
                self.succeeded.append(task.name)
            except Exception as e:
                self.logger.exception(f"Uh oh. Got exception running task {task.name}")
                self.failed.append(task.name)
                self.crashed.append(task.name)
            end_dt = utils.current_dt_utc()
            if codes != 0:
                self.logger.warning(f"Failed task {task.name} ({i+1}/{len(self.tasks)}) (duration: {end_dt-start_dt}) with code(s) {codes}")
            else:
                self.logger.info(f"Finished task {task.name} ({i+1}/{len(self.tasks)}) (duration: {end_dt-start_dt}) with code(s) {codes}")
            self.db.record_task(task.name,start_dt,end_dt,codes,self.pipeline_id)
        self.success = len(self.failed)==0
        if self.success:
            self.logger.info(f"Successfully finished pipeline run {self.pipeline_id} (pipeline {self.name} v{self.version})")
        else:
            self.logger.error(f"Unsuccessfully finished pipeline run {self.pipeline_id} (pipeline {self.name} v{self.version})")
            self.logger.warning(f"Failed: {', '.join(self.failed)}")
            if self.crashed:
                self.logger.warning(f"Crashed: {', '.join(self.crashed)}")
            else:
                self.logger.info("No crashes.")
        self.logger.info(f"Succeeded: {self.succeeded}")
        self.db.record_pipeline_end(self.pipeline_id,utils.current_dt_utc(),self.success,self.failed,self.crashed)
        return self.success

if __name__ == "__main__":
    class TestTaskOne(Task):
        def __call__(self, fitslist, outdir, config, logfile, pipeline_run_id):
            super().__call__(fitslist, outdir, config, logfile, pipeline_run_id)
            self.logger.info("hi")
            self.logger.info(self.config)
            self.logger.info(repr(self.config))
            self.logger.info(self.config("TEST_GLOBAL")) 
            self.logger.info(self.config("TEST_TEST")) 
            self.logger.info(self.config("TEST_DEFAULT"))
            self.config.set("TEST_SET_ONE","test task one set this!")
            # raise RuntimeError("I'm going to crash now!")
            return 0
        
        @property
        def required_params(self):
            return ["TEST_GLOBAL","TEST_TEST", "TEST_DEFAULT"]
        
        @property
        def will_set(self):
            return ["TEST_SET_ONE"]

        @property
        def description(self):
            return "A test task"
    

    class TestTaskTwo(Task):
        def __call__(self, fitslist, outdir, config, logfile, pipeline_run_id):
            super().__call__(fitslist, outdir, config, logfile, pipeline_run_id)
            self.logger.info(self.config("TEST_SET_ONE"))
            return 0
        
        @property
        def required_params(self):
            return ["TEST_SET_ONE"]

        @property
        def will_set(self):
            return None

        @property
        def description(self):
            return "A test task"


    test_task_one = TestTaskOne("test task one")
    test_task_two = TestTaskTwo("test task two")
    pipeline = Pipeline("test_pipline",[test_task_one, test_task_two],"no fits files","./test/pipeline","Test","./test/pipeline/test_config.toml",0, default_cfg_path="./test/pipeline/defaults.toml")
    success = pipeline.run()
