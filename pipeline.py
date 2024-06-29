import sys, os
from os.path import abspath, join, dirname, exists, basename
from abc import ABC, abstractmethod
import sqlite3
import logging
import logging.config
from typing import List

MODULE_PATH = abspath(dirname(__file__))
sys.path.append(MODULE_PATH)

import pipeline_utils, utils

from pipeline_db.db_config import configure_db
from pipeline_db.models import PipelineRun, Product, TaskRun
sys.path.append(MODULE_PATH)

def mod(path): return join(MODULE_PATH,path)

# storing pipeline products
    # input files in the fitslist (should probably make it more general) should be entered into the db at beginning (if they're new)
    # steps of the pipeline should create records of products that point to any precursor frames 
    # i.e. sextractor catalogs each point to a fits file, scamp headers each point to a sextractor product, 
    # products should store product type(s), pipeline run id, precise timestamps, and filepaths    

# class Product:
#     def __init__(self, data_type, pipeline_run_id, task_name, precursor_id, creation_dt, product_location, ID=None, flags=None, is_input=0, data_subtype=None):
#         self.data_type, self.pipeline_run_id, self.task_name, self.precursor_id = data_type, pipeline_run_id, task_name, precursor_id
#         self.creation_dt = utils.dt_to_utc(creation_dt)
#         self.product_location, self.ID, self.flags, self.is_input, self.data_subtype = product_location, ID, flags, is_input, data_subtype

class PipelineDB:
    def __init__(self, dbpath, logger):
        self.logger = logger
        if not exists(dbpath):
            raise FileNotFoundError(f"Sagelib: No database found at {dbpath}. Try running 'pipeline_db/create_db.py' with a path to create one, or check that this path is correct.")
        self.dbpath = dbpath

        self.connect()

    def connect(self):
        self.session, _ = configure_db(self.dbpath)

    def find_product(self, condition):
        return self.session.query(Product).filter(condition).all()

    def record_task(self, taskname, start_dt, end_dt, status_codes, pipeline_run_id):
        start_str = utils.tts(utils.dt_to_utc(start_dt))
        end_str = utils.tts(utils.dt_to_utc(end_dt))
        task_record = TaskRun(TaskName=taskname,StartTimeUTC=start_str,EndTimeUTC=end_str,StatusCodes=status_codes,PipelineRunID=pipeline_run_id)
        self.session.add(task_record)
        self.commit()
        return task_record.ID

    def commit(self):
        self.session.commit()

    def record_pipeline_start(self, pipeline_name, pipeline_version, start_dt, config, log_filepath=None):
        start_str = utils.tts(utils.dt_to_utc(start_dt))
        config_str = str(config)
        run = PipelineRun(PipelineName=pipeline_name,PipelineVersion=pipeline_version,StartTimeUTC=start_str,Config=config_str,LogFilepath=log_filepath)
        self.session.add(run)
        self.commit()
        return run
    
    def record_pipeline_end(self, pipeline_run_id, end_dt, success, failed, crashed):
        end_str = utils.tts(utils.dt_to_utc(end_dt))
        failed = ",".join(failed)
        crashed = ",".join(crashed)
        run = self.session.query(PipelineRun).filter(PipelineRun.ID==pipeline_run_id).first()
        run.EndTimeUTC = end_str
        run.FailedTasks = failed
        run.CrashedTasks = crashed
        run.Success = success
        # self.session.add(run) # do i need this?
        self.commit()
    
    def record_product(self,product:Product):
        self.session.add(product)
        self.commit()

    def record_input_data(self,product:Product,pipeline_run:PipelineRun):
        # create records to indicate what the inputs to a pipeline are, returns product
        existing_product = self.session.query(Product).filter(Product.product_location==product.input_location & Product.data_type==product.data_type & Product.flags==product.flags & Product.data_subtype==product.data_subtype).first()
        if existing_product:
            product = existing_product
            self.logger.info(f"Found product {existing_product.ID}")
        else:
            # if this product doesn't already exist in the db, it should be because its new and therefore does not yet have a producing_product_id
            assert product.producing_pipeline_run_id is None
            product.producing_pipeline_run_id = pipeline_run.ID
            self.session.add(product)
            self.logger.info(f"Made product {product.ID}")

        pipeline_run.Inputs.append(product)
        self.session.commit()
        return product

    def close(self):
        self.session.close()
    
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

    def outpath(self, file): return join(self.outdir, file)

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
    def __init__(self, pipeline_name, tasks:List[Task], inputs:List[str], outdir, profile_name, config_path, version, default_cfg_path=None):
        self.name = pipeline_name
        # list of *constructed* task objects, not just classes
        self.tasks = tasks
        self.inputs = [abspath(f) for f in inputs]
        self.outdir = outdir
        os.makedirs(outdir,exist_ok=True)
        self.profile_name = profile_name
        self.config = utils.Config(config_path, "PIPELINE_DEFAULTS_PATH")
        self.config.choose_profile(profile_name) # this is the scoped config in the file
        self.logfile = abspath(join(outdir,f"{self.name}.log"))
        self.logger = pipeline_utils.configure_logger(self.name,self.logfile)
        # db can ONLY be set in the default cfg
        self.dbpath = self.config._get_default("DB_PATH")
        print(self.dbpath)
        self.db = PipelineDB(self.dbpath, self.logger)
        self.version = version
        self.failed = []
        self.crashed = []
        self.pipeline_run = None
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
                except Exception: # this means we didn't find the key in the config. this is ok if the key will instead be set by a task that runs before this one
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
        self.pipeline_run = None
        self.success = None
        # get the pipeline_run object that identifies us
        # inputs are NOT passed here (or we get a chicken-and-egg situation bc inputs need to be associated with our id, which doesn't exist until after this)
        self.pipeline_run = self.db.record_pipeline_start(self.name,self.version,utils.current_dt_utc(),self.config,self.logfile)
        
        # register the inputs. they'll be added to the db if they dont already exist. 
        self.inputs = [self.db.record_input_data(i, self.pipeline_run) for i in self.inputs]

        self.logger.info(f"Beginning run {self.pipeline_run} (pipeline {self.name} v{self.version})")
        for i, task in enumerate(self.tasks):
            start_dt = utils.current_dt_utc()
            self.logger.info(f"Began task '{task.name}' ({i+1}/{len(self.tasks)})")
            codes = -1
            try:
                # this is using the task's __call__, not constructing it:
                codes = task(self.inputs, self.outdir, self.config, self.logfile, self.pipeline_run)
                if codes != 0:
                    self.logger.error(f"Got nonzero exit code from task {task.name}: {codes}! Exiting.")
                    self.failed.append(task.name)
                    break
                self.succeeded.append(task.name)
            except Exception as e:
                self.logger.exception(f"Uh oh. Got exception while running task {task.name}")
                self.failed.append(task.name)
                self.crashed.append(task.name)
                print(repr(e))
                print(e)
            end_dt = utils.current_dt_utc()
            if codes != 0:
                self.logger.warning(f"Failed task {task.name} ({i+1}/{len(self.tasks)}) (duration: {end_dt-start_dt}) with code(s) {codes}")
            else:
                self.logger.info(f"Finished task {task.name} ({i+1}/{len(self.tasks)}) (duration: {end_dt-start_dt}) with code(s) {codes}")
            self.db.record_task(task.name,start_dt,end_dt,codes,self.pipeline_run)
        self.success = len(self.failed)==0
        if self.success:
            self.logger.info(f"Successfully finished pipeline run {self.pipeline_run} (pipeline {self.name} v{self.version})")
        else:
            self.logger.error(f"Unsuccessfully finished pipeline run {self.pipeline_run} (pipeline {self.name} v{self.version})")
            self.logger.warning(f"Failed: {', '.join(self.failed)}")
            if self.crashed:
                self.logger.warning(f"Crashed: {', '.join(self.crashed)}")
            else:
                self.logger.info("No crashes.")
        self.logger.info(f"Succeeded: {self.succeeded}")
        self.db.record_pipeline_end(self.pipeline_run,utils.current_dt_utc(),self.success,self.failed,self.crashed)
        return self.success

if __name__ == "__main__":
    if os.path.exists(r"pipeline_db\.env"):
        from dotenv import load_dotenv
        load_dotenv(r"pipeline_db\.env")

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
