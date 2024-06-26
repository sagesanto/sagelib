# Sage Santomenna 2023
# models used by sqlalchemy to understand the database
from typing import List

import os, sys
from os.path import abspath, join, dirname, pardir
import sqlalchemy
from sqlalchemy import select, insert, and_, or_
from sqlalchemy import Column, Integer, String, BLOB, ForeignKey, Table, PrimaryKeyConstraint
from sqlalchemy.orm import relationship, Mapped, mapped_column

from sqlalchemy import create_engine, Column, Integer, String, Numeric, Text, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

sys.path.append(dirname(__file__))

parent_dir = abspath(join(dirname(__file__), pardir))
sys.path.append(parent_dir)

from pipeline_db.db_config import pipeline_base

sys.path.remove(parent_dir)
sys.path.remove(dirname(__file__))

PipelineInputAssociation = None

if not PipelineInputAssociation:
    # table to match observations with obs codes
    PipelineInputAssociation = Table(
        'PipelineInputAssociation',
        pipeline_base.metadata,
        Column('PipelineRunID', Integer, ForeignKey('PipelineRun.ID'), nullable=False,primary_key=True, index=True),
        Column('ProductID', Integer, ForeignKey('Product.ID'), nullable=False,primary_key=True),
        # UniqueConstraint('PipelineRunID','ProductID',name="UniqueProducts")
        extend_existing=True
    )

class PrecursorProductAssociation(pipeline_base):
    __tablename__ = 'PrecursorProductAssociation'

    PrecursorID = Column(Integer, ForeignKey('Product.ID'), primary_key=True)
    ProductID = Column(Integer, ForeignKey('Product.ID'), primary_key=True)

    # Define the relationship to Product table (not needed for direct querying)
    precursor = relationship('Product', foreign_keys=[PrecursorID])
    product = relationship('Product', foreign_keys=[ProductID])


class PipelineRun(pipeline_base):
    """ A permanent record that stores information about a past or ongoing Pipeline run.
    
    :class:`PipelineRun` objects are created and recorded each time any :class:`Pipeline` is run. they make a permanent record of important information about who, what, when, and where. On run, a :class:`sagelib.pipeline.Pipeline` will generate a :class:`PipelineRun` record, and update it when they finish. To find products on which to operate, a :class:`sagelib.pipeline.Task` should use its own :func:`sagelib.pipeline.Task.find_products()` method, which will in turn reference the :class:`PipelineRun` .
    """
    __tablename__ = 'PipelineRun'

    ID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    PipelineName = Column(String, nullable=False)
    StartTimeUTC = Column(String, nullable=False)
    EndTimeUTC = Column(String, nullable=True)
    Success = Column(Integer, nullable=True)
    FailedTasks = Column(String, nullable=True)
    CrashedTasks = Column(String, nullable=True)
    PipelineVersion = Column(String, nullable=False)
    Config = Column(String, nullable=False)
    # InputFITS = Column(String, nullable=False)
    LogFilepath = Column(String, nullable=True)
    OutputProducts: Mapped[List["Product"]] = relationship("Product")
    Inputs: Mapped[List["Product"]] = relationship("Product", secondary='PipelineInputAssociation', back_populates="UsedByRunsAsInput")
    TaskRuns: Mapped[List["TaskRun"]] = relationship("TaskRun")

    def __repr__(self):
        return f"'{self.PipelineName}' v{self.PipelineVersion} (run #{self.ID})"
    
    def get_related_products(self, dbsession, **filters):
        """Query for products among this PipelineRun's inputs an outputs. optionally, add keyword arguments to filter Products
        
        Query this pipeline run's inputs and the outputs of previous task runs in this pipeline run for Products. 

        :param dbsession: sqlalchemy database session with which to query
        :param **filters: keyword argument filters to apply to the query. Keys must be columns of the PipelineRun table. supports wildcarding with %

        :returns: list of products 
        """
        query = dbsession.query(Product).filter(
            (Product.producing_pipeline_run_id == self.ID) | 
            (Product.UsedByRunsAsInput.any(PipelineRun.ID == self.ID))
        )
        if filters:
            for colname, cond_val in filters.items():
                col = getattr(Product,colname)
                if col is None:
                    raise AttributeError(f"Product table has no column {colname}")
                query = query.filter(col.like(cond_val))
        related_products = query.all()
        return related_products

class Product(pipeline_base):
    """Inputs and outputs of Pipelines, represent pipeline products 
    
    Products represent data files that are the inputs or outputs to Pipelines. Products that come from external sources (ex FITS files downloaded from the internet) and not from another PipelineRun are considered 'Inputs' (but they are still of class :class:`Product` - no Input class exists). The :py:attr:`task_name` field of Inputs should be ``INPUT``. The first :class:`PipelineRun` to use a given Input is considered its producing pipeline. During this first run, the Input will be inserted into the :class:`sagelib.pipeline.PipelineDB` and marked with the ID of the :class:`PipelineRun` . 

    When creating inputs to a Pipeline (presumably from passed-in filenames or the like), you should **not** add them to the database. :class:`sagelib.pipeline.Pipeline.run` will do that. 

    Products are created in two ways: before a pipeline runs (to then be passed as Inputs) or by Tasks during a pipeline run. 

    **IMPORTANT:** When a :class:`sagelib.pipeline.Task` creates any sort of output product (table, mask, fits file, etc), it *must* create a corresponding Product, set its :py:attr:`producing_task_run_id` to be the Tasks's TaskRun.ID, and add it to the :class:`sagelib.pipeline.PipelineDB` . This is **very important,** as future Tasks in the pipeline can only use products that are in the ``PipelineDB``. This can be done easily with :func:`sagelib.pipeline.Task.publish_output()` like so::

    >>> product = self.publish_output("FitsImage",outpath,flags=None,data_subtype="Coadd",precursors=[image_product]])

    After this, the product is in the database and correctly reflects its origin. Tasks should use :func:`sagelib.pipeline.Task.publish_output()` as their preferred method of creating and recording products.

    :func:`sagelib.pipeline.PipelineDB.record_product()` can also be used to add a :class:`Product` to the database.

    :param data_type: The type of the data that this product represents, ex "FitsImage"
    :param task_name: The name of the task that created this product 
    :param creation_dt: A datetime string in module format indicating the time at which this product was created
    :param product_location: A string representing the full path to the data that this product represents 
    :param is_input: Integer. 1 if this product was not output by any Pipeline, ever. 0 otherwise
    :param producing_pipeline_run_id=None: The ID of the Pipeline that produced this output, if any
    :param producing_task_run_id=None: The ID of the Task that produced this output, if any
    :param flags=None: An integer to store data with. Interpretation of flags is left entirely to individual Tasks.
    :param data_subtype=None: A string that gives more detail as to the type of data being stored. Ex. 'Coadd'
    :param **kwargs: Additional kwargs. Most relevant is ``prescursor``, a collection of :class:`Product` objects from which this one was derived.
    """
    __tablename__ = 'Product'

    ID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    data_type = Column(String, nullable=False)
    producing_pipeline_run_id = Column(Integer, ForeignKey('PipelineRun.ID'), nullable=True)
    task_name = Column(String, nullable=False)
    producing_task_run_id = Column(Integer, ForeignKey('TaskRun.ID'), nullable=True)
    creation_dt = Column(String, nullable=False)
    product_location = Column(String, nullable=False)
    flags = Column(Integer, nullable=True)
    is_input = Column(Integer, nullable=False) 
    data_subtype = Column(String, nullable=True)
    precursors = relationship('Product',
                              secondary='PrecursorProductAssociation',
                              primaryjoin='Product.ID == PrecursorProductAssociation.ProductID',
                              secondaryjoin='Product.ID == PrecursorProductAssociation.PrecursorID',
                              backref='derivatives',
                              overlaps="product")
    ProducingPipeline = relationship("PipelineRun", back_populates="OutputProducts")
    UsedByRunsAsInput: Mapped[List["PipelineRun"]] = relationship("PipelineRun", secondary='PipelineInputAssociation', back_populates="Inputs")
    ProducingTask = relationship("TaskRun", back_populates="Outputs")

    def __init__(self, data_type, task_name, creation_dt, product_location, is_input, 
                 producing_pipeline_run_id=None, producing_task_run_id=None, flags=None, data_subtype=None, **kwargs):
        """hi"""
        super().__init__(data_type=data_type, producing_pipeline_run_id=producing_pipeline_run_id,
                         task_name=task_name, producing_task_run_id=producing_task_run_id,
                         creation_dt=creation_dt, product_location=product_location,
                         flags=flags, is_input=is_input, data_subtype=data_subtype, **kwargs)

    def __str__(self):
        endline, tab = '\n','\t'
        return f"{'Input ' if self.is_input else ''}Product of type '{self.data_type+(f'.{self.data_subtype}' if self.data_subtype else '')}' created at {self.creation_dt} UTC with {len(self.precursors)} precursors and {len(self.derivatives)} derivatives.\nProducers: Pipeline {self.ProducingPipeline}, Task {self.ProducingTask}.\nPrecursors:\n\t{(endline+tab).join([repr(p) for p in self.precursors])}\nDerivatives:\n\t{(endline+tab).join([repr(d) for d in self.derivatives])}\n"
    
    def __repr__(self):
        return f"#{self.ID}: {'Input ' if self.is_input else ''}Product of type '{self.data_type+(f'.{self.data_subtype}' if self.data_subtype else '')}'"
    
    def traverse_derivatives(self,func,*args,maxdepth=-1,**kwargs):
        """Recursively pply a function to each of the products in the derivative tree of this product, collecting and returning its result
        
        Does a tree-like walk of derivatives of this product, calling ``func`` on each and collecting the results in a dictionary of {:class:`Product` : returned result}.
        
        :param func: the function to apply to each derivative. must take the derivative as its first argument.
        :param maxdepth: integer. maximum depth to traverse. any negative number runs forever.
        :param *args: additional arguments to pass to ``func``
        :param **kwargs: additional keyword arguments to pass to ``func``

        :returns: a dictionary of {result of ``func``: list of results of traverse_derivatives on derivatives}
        """
        res = []
        if not self.derivatives or not maxdepth:
            return func(self)
        for d in self.derivatives:
            res.append(d.traverse_derivatives(func, maxdepth-1,*args,**kwargs))
        return {func(self):res}
    
    # what a horrendous mess. why did i do this to myself
    def all_derivatives(self,pipeline_run_id=None):
        """Traverses tree of derivatives, returning all as a flattened list."""

        deriv_tree = self.traverse_derivatives(lambda s: s)

        if not deriv_tree:
            return []

        def extract_derivs(tree):
            contents = []
            if isinstance(tree,Product):
                if not pipeline_run_id or tree.producing_pipeline_run_id==pipeline_run_id:
                    return [tree]
            if isinstance(tree,dict):
                for k, v in tree.items():
                    if not pipeline_run_id or k.producing_pipeline_run_id==pipeline_run_id or k==self:
                        contents.extend(extract_derivs(v))
                        if k != self:
                            contents.append(k)
            if isinstance(tree,list):
                for l in tree:
                    contents.extend(extract_derivs(l))
            return contents
        return extract_derivs(deriv_tree)


class TaskRun(pipeline_base):
    """A :class:`TaskRun` object represents one run of a :class:`sagelib.pipeline.Task` . Constructed by the Pipeline."""
    __tablename__ = 'TaskRun'

    TaskName = Column(String, nullable=False)
    StartTimeUTC = Column(String, nullable=False)
    EndTimeUTC = Column(String, nullable=True)
    StatusCodes = Column(Integer, nullable=True)
    PipelineRunID = Column(Integer, ForeignKey('PipelineRun.ID'))
    ID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    Outputs: Mapped[List["Product"]] = relationship("Product", back_populates="ProducingTask")
    Pipeline = relationship("PipelineRun",back_populates="TaskRuns")

    def __repr__(self):
        return f"'{self.TaskName}' (run #{self.ID})"