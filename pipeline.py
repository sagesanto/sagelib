import sys, os
from os.path import abspath, join, dirname, exists, basename
from abc import ABC, abstractmethod
import logging
import logging.config
from typing import List, Mapping, Any
from sqlalchemy import inspect
MODULE_PATH = abspath(dirname(__file__))
sys.path.append(join(MODULE_PATH,os.path.pardir))
try:
    from . import PipelineRun, Product, TaskRun, pipeline_utils, utils, configure_db
    from utils import now_stamp, tts, stt, dt_to_utc, current_dt_utc    
except:
    from sagelib import PipelineRun, Product, TaskRun, pipeline_utils, utils, configure_db
    from sagelib.utils import now_stamp, tts, stt, dt_to_utc, current_dt_utc    

sys.path.remove(join(MODULE_PATH,os.path.pardir))

# from pipeline_db.models import PipelineRun, Product, TaskRun

def mod(path): return join(MODULE_PATH,path)

# storing pipeline products
    # input files in the fitslist (should probably make it more general) should be entered into the db at beginning (if they're new)
    # steps of the pipeline should create records of products that point to any precursor frames 
    # i.e. sextractor catalogs each point to a fits file, scamp headers each point to a sextractor product, 
    # products should store product type(s), pipeline run id, precise timestamps, and filepaths    

# class Product:
#     def __init__(self, data_type, pipeline_run_id, task_name, precursor_id, creation_dt, product_location, ID=None, flags=None, is_input=0, data_subtype=None):
#         self.data_type, self.pipeline_run_id, self.task_name, self.precursor_id = data_type, pipeline_run_id, task_name, precursor_id
#         self.creation_dt = dt_to_utc(creation_dt)
#         self.product_location, self.ID, self.flags, self.is_input, self.data_subtype = product_location, ID, flags, is_input, data_subtype

class PipelineDB:
    """test str"""
    def __init__(self, dbpath, logger):
        self.logger = logger
        if not exists(dbpath):
            raise FileNotFoundError(f"Sagelib: No database found at {dbpath}. Try running 'pipeline_db/create_db.py' with a path to create one, or check that this path is correct.")
        self.dbpath = dbpath

        self.connect()

    def connect(self):
        self.session, _ = configure_db(self.dbpath)

    def query(self,*args,**kwargs):
        return self.session.query(*args,**kwargs)
    
    def add(self,*args,**kwargs):
        self.session.add(*args,**kwargs)

    def find_product(self, condition):
        return self.session.query(Product).filter(condition).all()
    
    def record_task_start(self, taskname, start_dt, pipeline_run_id):
        start_str = tts(dt_to_utc(start_dt))
        task_record = TaskRun(TaskName=taskname,StartTimeUTC=start_str,PipelineRunID=pipeline_run_id)
        self.session.add(task_record)
        self.commit()
        return task_record

    def record_task_end(self,task_run,end_dt,status_codes):
        end_str = tts(dt_to_utc(end_dt))
        task_run.EndTimeUTC = end_str
        task_run.StatusCodes = status_codes
        self.commit()

    def commit(self):
        self.session.commit()

    def record_pipeline_start(self, pipeline_name, pipeline_version, start_dt, config, log_filepath=None):
        start_str = tts(dt_to_utc(start_dt))
        config_str = str(config)
        run = PipelineRun(PipelineName=pipeline_name,PipelineVersion=pipeline_version,StartTimeUTC=start_str,Config=config_str,LogFilepath=log_filepath)
        self.session.add(run)
        self.commit()
        return run
    
    def record_pipeline_end(self, pipeline_run, end_dt, success, failed, crashed):
        end_str = tts(dt_to_utc(end_dt))
        failed = ",".join(failed)
        crashed = ",".join(crashed)
        pipeline_run.EndTimeUTC = end_str
        pipeline_run.FailedTasks = failed
        pipeline_run.CrashedTasks = crashed
        pipeline_run.Success = success
        # self.session.add(run) # do i need this?
        self.commit()
    
    def record_product(self,product:Product):
        self.session.add(product)
        self.commit()

    def record_input_data(self,product:Product,pipeline_run:PipelineRun):
        # create records to indicate what the inputs to a pipeline are, returns product
        existing_product = self.session.query(Product).filter((Product.product_location==product.product_location) & (Product.data_type==product.data_type) & (Product.flags==product.flags) & (Product.data_subtype==product.data_subtype)).first()
        if existing_product:
            product = existing_product
            # self.logger.info(f"Found product {existing_product} ({existing_product.product_location})")
        else:
            # if this product doesn't already exist in the db, it should be because its new and therefore does not yet have a producing_product_id

            assert product.producing_pipeline_run_id is None
            product.producing_pipeline_run_id = pipeline_run.ID
            product.creation_dt = now_stamp()
            self.session.add(product)
            # self.logger.info(f"Made product {product}")

        pipeline_run.Inputs.append(product)
        self.session.commit()
        return product

    def close(self):
        self.session.close()

    def __del__(self):
        self.close()

class Task(ABC):
    def __init__(self, name:str, filters:dict[str,str] | None=None, cfg_profile_name:str | None=None):
        """One step of a pipeline process

        :param name: the name of this task. ideally, the name alone gives a fairly good idea of what this task does
        :type name: str
        :param filters: a dictionary of key, value pairs that restricts :class:`Product` searches to only returning those where all of their `key` properties have the value `value` , defaults to None
        :type filters: dict[str,str] | None, optional
        :param cfg_profile_name: name of a profile to load from the config for the duration of this task's run. options in the profile will override global and default settings, defaults to None
        :type cfg_profile_name: str | None, optional
        """
        self.name = name
        self.outdir = None
        self.cfg_profile_name = cfg_profile_name
        # logical expressions that will be applied to all product queries that use self.find_products
        self.filters= filters or {}

    def __call__(self, inputs:List[Product], outdir:str, config:utils.Config, logfile:str, pipeline_run:PipelineRun, db:PipelineDB, task_run:TaskRun) -> int:
        """
        Called by :func:`Pipeline.run`. Does important setup, then calls :func:`Task.run()`
        """
        self.logfile = logfile
        self.logger = pipeline_utils.configure_logger(self.name, self.logfile)
        self.inputs, self.outdir, self.config = inputs, outdir, config,
        self.pipeline_run, self.db, self.task_run = pipeline_run, db, task_run
        
        # choose config profile if given
        if self.cfg_profile_name:
            self.config.choose_profile(self.cfg_profile_name)

        # add filters from config profile, if they exist
        filters_from_cfg = self.config.get("filters")
        if filters_from_cfg:
            for k,v in filters_from_cfg.items():
                self.filters[k] = v
        
        return self.run()

    @abstractmethod
    def run(self) -> int:
        """Run the pipeline task. Should take no arguments. Not invoked directly! Pipeline invokes through __call__ and does important setup in the process"""
        pass

    @property
    def ID(self):
        return self.task_run.ID

    def outpath(self, file:str): return join(self.outdir, file)

    def publish_output(self, data_type:str, product_location:str, flags:int | None=None, data_subtype: str | None=None,**kwargs:Mapping[str,Any]) -> Product:
        """Make a Product object with this task's ID, pipeline run ID, etc, add it to the database, and return it
        
        For example, to add a newly-created WCS header with path ``outpath`` derived from some ``image_product`` to the database:

        >>> wcs_product = self.publish_output("Header",outpath,flags=None,data_subtype="WCS",precursors=[image_product]])

        After this, the product is in the database and correctly reflects its origin. Tasks should use :func:`publish_output()` as their preferred method of creating and recording products. 

        :param data_type: The data type of the output. Can be anything, but is used by :func:Task.find_products() to filter for certain types of products
        :type data_type: str
        :param product_location: the path / locator of the product that can be used to access it
        :type product_location: str
        :param flags: optional integer used to denote flags. interpretation of flags is left to individual tasks, defaults to None
        :type flags: int | None, optional
        :param data_subtype: the subtype of the data. same idea as `data_type`, defaults to None
        :type data_subtype: str | None, optional
        :return: a newly created Product that has just been added to the database.
        :rtype: Product
        """
        product_location = abspath(product_location)
        product = Product(data_type, self.name, current_dt_utc(), product_location, is_input=0, 
                          producing_pipeline_run_id=self.pipeline_run.ID, producing_task_run_id=self.task_run.ID, flags=flags, data_subtype=data_subtype, **kwargs)
        self.db.record_product(product)
        return product

    def find_products(self, data_type: str, **filters: Mapping[str,Any]) -> List[Product]:
        """ Finds products from the current pipeline run (inputs and previous outputs). Filters are keyword pairs. '%' is the wildcard operator.
        
        For example, to find Headers of subtype 'WCS'::

        >> scamp_headers = self.find_products(data_type="Header",data_subtype="WCS")

        Headers of any subtype::

        >> scamp_headers = self.find_products(data_type="Header")

        Headers with any non-None subtype::

        >> scamp_headers = self.find_products(data_type="Header",data_subtype="%")
        
        """
        # if all_runs:
                # q = self.db.query(Product).filter((Product.data_type==data_type) & (Product.data_subtype.like(data_subtype))).all()
        return self.pipeline_run.get_related_products(self.db.session, data_type=data_type, **self.filters, **filters)

    @property
    @abstractmethod
    def required_params(self) -> List[str]:
        """Return a list of keys that must be in the config or set by a prior pipeline task for this one to work"""
        return []

    @property
    @abstractmethod
    def will_set(self) -> List[str]:
        """Return a list of keys in the config that this task will set while running"""
        return []

    @property
    @abstractmethod
    def required_product_types(self) -> List[str]:
        """Return a list of datatypes that must be passed as input or produced by a prior pipeline task for this one to work. Datatypes can be denoted as 'datatype' or 'datatype.subtype' """
        return []
    
    @property 
    @abstractmethod
    def product_types_produced(self) -> List[str]:
        """Return a list of datatypes that this pipeline step will produce. Datatypes can be denoted as 'datatype' or 'datatype.subtype' """
        return []

    @property
    @abstractmethod
    def description(self) -> str:
        """Return a brief description of this task
        :rtype: str
        """
        pass


class Pipeline:
    def __init__(self, pipeline_name: str, tasks:List[Task], inputs:List[Product], outdir:str, config_path:str, version:str, default_cfg_path:str | None = None):
        self.name = pipeline_name
        # list of *constructed* task objects, not just classes
        self.tasks = tasks
        self.inputs = inputs
        # self.inputs = [abspath(f) for f in inputs]
        self.outdir = outdir
        os.makedirs(outdir,exist_ok=True)
        # self.profile_name = profile_name
        self.config = utils.Config(config_path, "PIPELINE_DEFAULTS_PATH")
        # self.config.choose_profile(profile_name) # this is the scoped config in the file
        self.logfile = abspath(join(outdir,f"{self.name}.log"))
        self.logger = pipeline_utils.configure_logger(self.name,self.logfile)
        # db can ONLY be set in the default cfg
        self.dbpath = self.config._get_default("DB_PATH")
        print(self.dbpath)
        self.db = PipelineDB(self.dbpath, self.logger)
        self.version = version
        self.failed = []
        self.crashed = []
        self.succeeded = []
        self.failed_task_runs = []
        self.crashed_task_runs = []
        self.succeeded_task_runs = []
        self.pipeline_run = None
        self.success = None
        if default_cfg_path:
            self.config.load_defaults(default_cfg_path)
        self.task_runs = []
        self.validate_pipeline()
    
    def validate_pipeline(self):
        # check configuration keys
        missing = {}
        req = self.get_required_keys()
        set_by_tasks = []
        for task, keys in self.get_required_keys().items():
            self.config.clear_profile()
            if task.cfg_profile_name:
                self.config.choose_profile(task.cfg_profile_name)
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
        
        # check products produced in pipeline
        missing = {}
        datatypes_supplied = []
        for p in self.inputs:
            datatypes_supplied.append("*")
            datatypes_supplied.append(p.data_type)
            datatypes_supplied.append(f"{p.data_type}.{p.data_subtype}")
        datatypes_supplied = list(set(datatypes_supplied))
        for task in self.tasks:
            for dtype in task.required_product_types:
                if dtype not in datatypes_supplied:
                    if missing.get(task.name):
                        missing[task.name].append(dtype)
                    else:
                        missing[task.name] = [dtype]
            datatypes_supplied.extend(task.product_types_produced)
            datatypes_supplied = list(set(datatypes_supplied))
        if missing:
            raise AttributeError(f"Tasks are missing the following data products: {missing}")

    def check_task_honesty(self,task:Task,taskrun:TaskRun):
        missing_keys = []
        for key in task.will_set:
            if self.config.get(key) is None:
                missing_keys.append(key)
        produced_by_task = self.pipeline_run.get_related_products(self.db.session,producing_task_run_id=taskrun.ID)
        types_produced_by_task = []
        for p in produced_by_task:
            types_produced_by_task.append("*")
            types_produced_by_task.append(p.data_type)
            types_produced_by_task.append(f"{p.data_type}.{p.data_subtype}")
        types_produced_by_task = set(types_produced_by_task)
        missing_product_types = []
        for t in task.product_types_produced:
            if t not in types_produced_by_task:
                missing_product_types.append(t)
        
        if missing_keys:
            self.logger.warning(f"It looks like task '{task.name}' (#{taskrun.ID}) failed to set the following config keys despite promising to do so: {missing_keys}. This is probably a programming error. The pipeline run will continue, but this could cause serious problems.")
        if missing_product_types:
            self.logger.warning(f"It looks like task '{task.name}' (#{taskrun.ID}) failed to produce data products of the following types, despite promising to do so: {missing_product_types}. This is probably a programming error. The pipeline run will continue, but this could cause serious problems.")
        

    def get_required_keys(self) -> dict[Task,str]:
        keywords = {}
        for task in self.tasks:
            keywords[task] = task.required_params
        return keywords
    
    def run(self) -> int:
        self.succeeded = []
        self.failed = []
        self.crashed = []
        self.succeeded_task_runs = []
        self.failed_task_runs = []
        self.crashed_task_runs = []

        self.pipeline_run = None
        self.success = None

        # get the pipeline_run object that identifies us
        # inputs are NOT passed here (or we get a chicken-and-egg situation bc inputs need to be associated with our id, which doesn't exist until after this)
        pipeline_start = current_dt_utc()
        self.pipeline_run = self.db.record_pipeline_start(self.name,self.version,pipeline_start,self.config,self.logfile)
        # register the inputs. they'll be added to the db if they dont already exist. 
        self.inputs = [self.db.record_input_data(i, self.pipeline_run) for i in self.inputs]
        print("Inputs after registration:")
        print([inspect(i).dict for i in self.inputs])
        print("Input sessions after registration:")
        print([inspect(i).session for i in self.inputs])
        self.logger.info(f"Beginning run {self.pipeline_run.ID} (pipeline {self.name} v{self.version})")
        for i, task in enumerate(self.tasks):
            start_dt = current_dt_utc()
            self.logger.info(f"Began task '{task.name}' ({i+1}/{len(self.tasks)})")
            code = -1
            task_run = self.db.record_task_start(task.name,start_dt,self.pipeline_run.ID)
            self.db.session.expire_all()
            try:
                self.config.clear_profile()
                # this is using the task's __call__, not constructing it:
                code = task(self.inputs, self.outdir, self.config, self.logfile, self.pipeline_run, self.db, task_run)
                # we need tasks to return integer codes. if this isn't an int, the task was written wrong
                if not isinstance(code, int):
                    raise ValueError(f"Task \'{task.name}\' returned \'{code}\' instead of an integer return code. Tasks must return an integer code (0=success) if they do not crash.")
                end_dt = current_dt_utc()
                self.db.record_task_end(task_run,end_dt=end_dt,status_codes=code)
                if code != 0:
                    self.logger.error(f"Got nonzero exit code from task {task.name}: {code}! Ending pipeline run.")
                    self.failed.append(task.name)
                    self.failed_task_runs.append(task_run)
                    break
                self.check_task_honesty(task,task_run)
                self.succeeded_task_runs.append(task_run)
                self.succeeded.append(task.name)
            except Exception as e:
                end_dt = current_dt_utc()
                self.db.record_task_end(task_run,end_dt=end_dt,status_codes=code)
                self.logger.exception(f"CRASH! Uh oh. Got exception while running task {task.name}")
                self.crashed.append(task.name)
                self.crashed_task_runs.append(task_run)
                print(repr(e))
                print(e)
                break
            if code != 0:
                self.logger.warning(f"Failed task {task.name} ({i+1}/{len(self.tasks)}) (duration: {end_dt-start_dt}) with code {code}")
            else:
                self.logger.info(f"Finished task {task.name} ({i+1}/{len(self.tasks)}) (duration: {end_dt-start_dt}) with code {code}")
        self.config.clear_profile()
        self.success = len(self.failed)==0 and len(self.crashed)==0
        pipeline_end = current_dt_utc()
        if self.success:
            self.logger.info(f"Successfully finished pipeline run {self.pipeline_run.ID} (pipeline {self.name} v{self.version}) (duration: {pipeline_end-pipeline_start})")
        else:
            self.logger.error(f"Unsuccessfully finished pipeline run {self.pipeline_run.ID} (pipeline {self.name} v{self.version}) (duration: {pipeline_end-pipeline_start})")
            self.logger.warning(f"Failed: {', '.join(self.failed)}")
            if self.crashed:
                self.logger.warning(f"Crashed: {', '.join(self.crashed)}")
            else:
                self.logger.info("No crashes.")
        self.logger.info(f"Succeeded: {self.succeeded}")
        self.db.record_pipeline_end(self.pipeline_run,current_dt_utc(),self.success,self.failed,self.crashed)
        self.db.session.expire_all()
        return self.success

if __name__ == "__main__":
    if os.path.exists(r"pipeline_db\.env"):
        from dotenv import load_dotenv
        load_dotenv(r"pipeline_db\.env")

    class TestTaskOne(Task):
        def run(self):
            self.logger.info("hi")
            self.logger.info(self.config)
            self.logger.info(repr(self.config))
            self.logger.info(self.config("TEST_GLOBAL")) 
            self.logger.info(self.config("TEST_TEST")) 
            self.logger.info(self.config("TEST_DEFAULT"))
            self.config.set("TEST_SET_ONE","test task one set this!")
            outproduct = self.publish_output("test_one","test one output loc",precursors=self.inputs)
            # raise RuntimeError("I'm going to crash now!")
            return 0

        @property
        def required_params(self):
            return ["TEST_GLOBAL","TEST_TEST", "TEST_DEFAULT"]
        
        @property
        def required_product_types(self):
            return ["test_input"]
        
        @property
        def product_types_produced(self):
            return ["test_one"]

        @property
        def will_set(self):
            return ["TEST_SET_ONE"]

        @property
        def description(self):
            return "A test task"
    

    class TestTaskTwo(Task):
        def run(self):
            self.logger.info(self.config("TEST_SET_ONE"))
            task_one_out = self.find_products("test_one")[0]
            # test making a Product whose precursors are the inputs + the task one output
            precursors = [task_one_out]
            # precursors.extend(inputs)
            task_two_out_1 = self.publish_output("test_two","test_two_1 output loc",precursors=precursors)
            task_two_out_2 = self.publish_output("test_two","test_two_2 output loc",precursors=precursors)
            task_two_1_sub = self.publish_output("test_two","test_two_3 output loc",precursors=[task_two_out_1])
            self.logger.info(f"Input: \n{str(self.inputs[0])}")
            self.logger.info(f"Task one's product: \n{str(task_one_out)}")
            self.logger.info(f"Task two product 1: \n{str(task_two_out_1)}")
            self.logger.info(f"Task two product 2: \n{str(task_two_out_2)}")
            self.logger.info(f"Task two 1 sub:product: \n{str(task_two_1_sub)}")
            self.logger.info(f"All products from this run: {self.find_products('%')}")
            self.logger.info(f"traversal: {task_one_out.traverse_derivatives(lambda p: p.product_location)}")
            self.logger.info(f"task_one_out all derivatives: {task_one_out.all_derivatives()}")
            self.logger.info(f"Inputs[0] all derivatives: {self.inputs[0].all_derivatives()}")
            self.logger.info(f"Inputs[0] all derivatives only this run: {self.inputs[0].all_derivatives(pipeline_run_id=self.pipeline_run.ID)}")

            return 0
        
        @property
        def required_params(self):
            return ["TEST_SET_ONE"]

        @property
        def required_product_types(self):
            return ["test_one"]
        
        @property
        def product_types_produced(self):
            return ["test_two","i'm lying about producing this type"]

        @property
        def will_set(self):
            return ["i'm lying about setting this key"]

        @property
        def description(self):
            return "A test task"


    test_task_one = TestTaskOne("test task one",cfg_profile_name="Test")
    test_task_two = TestTaskTwo("test task two",cfg_profile_name="Test")

    test_input = Product("test_input","INPUT", current_dt_utc(),"test_input loc",is_input=1)
    test_input_2 = Product("test_input","INPUT", current_dt_utc(),"test_input 2 loc",is_input=1)

    pipeline = Pipeline("test_pipline",[test_task_one, test_task_two],[test_input, test_input_2],"./test/pipeline","./test/pipeline/test_config.toml","0.0", default_cfg_path="./test/pipeline/defaults.toml")
    success = pipeline.run()
    # insp = inspect(test_input)
    # print("Session:",insp.session)
    # print("Info:",insp.dict)
    # print("deleted:",insp.deleted)
    # print("detached:",insp.detached)
    # print("pending:",insp.pending)
    # print("persistent:",insp.persistent)
    # print("transient:",insp.transient)
    # print("unloaded:",insp.unloaded)
    # pipeline.db.session.refresh(test_input)
    # print("traversal:")
    # print(test_input.traverse_derivatives(lambda p: p.product_location))
