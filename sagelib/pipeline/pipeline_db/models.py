from __future__ import annotations
# Sage Santomenna 2024
# models used by sqlalchemy to understand the database
from typing import List, Callable, Tuple, Union, Any,Mapping
import sys
from os.path import abspath, join, dirname, pardir
from datetime import datetime
from matplotlib.figure import Figure
from matplotlib.axes import Axes

from sqlalchemy import Column, Integer, String, ForeignKey, Table, null, and_
from sqlalchemy.orm import relationship, Mapped, mapped_column, scoped_session, aliased
from sqlalchemy.sql.elements import BinaryExpression

sys.path.append(dirname(__file__))

parent_dir = abspath(join(dirname(__file__), pardir))
sys.path.append(parent_dir)

from pipeline_db.db_config import pipeline_base, mapper_registry
from sagelib.utils import dt_to_utc, tts, visualize_graph

sys.path.remove(parent_dir)
sys.path.remove(dirname(__file__))

# PipelineInputAssociation = None

# # mapper_registry.configure()

# if not PipelineInputAssociation:
PipelineInputAssociation = Table(
    'PipelineInputAssociation',
    pipeline_base.metadata,
    Column('PipelineRunID', Integer, ForeignKey('PipelineRun.ID'), nullable=False,primary_key=True, index=True),
    Column('ProductID', Integer, ForeignKey('Product.ID'), nullable=False,primary_key=True),
    # UniqueConstraint('PipelineRunID','ProductID',name="UniqueProducts")
    extend_existing=True
)

ProductProductGroupAssociation = Table(
    "ProductProductGroupAssociation",
    pipeline_base.metadata,
    Column('ProductGroupID', Integer, ForeignKey('ProductGroup.ID'), nullable=False,primary_key=True, index=True),
    Column('ProductID', Integer, ForeignKey('Product.ID'), nullable=False,primary_key=True),
)

ProductMetadataAssociation = Table(
    "ProductMetadataAssociation",
    pipeline_base.metadata,
    Column('ProductID', Integer, ForeignKey('Product.ID'), nullable=False,primary_key=True, index=True),
    Column('MetadataID', Integer, ForeignKey('Metadata.ID'), nullable=False,primary_key=True),
)

def product_query(dbsession:scoped_session, metadata:dict|None=None, exprs:None|List[BinaryExpression]=None, **filters:Mapping[str,Any]):
    query = dbsession.query(Product).order_by(Product.creation_dt.desc())

    to_apply = exprs or []
    
    if filters:
            for colname, cond_val in filters.items():
                col = getattr(Product,colname)
                if col is None:
                    raise AttributeError(f"Product table has no column {colname}")
                to_apply.append(col.like(cond_val))

    if metadata:
        for i, (k,v) in enumerate(metadata.items()):
            md_alias = aliased(Metadata, name=f"md_alias{i}")
            assoc_alias = aliased(ProductMetadataAssociation, name=f"assoc_alias{i}")
            # do the joins to populate these values
            query = query.join(assoc_alias,Product.ID==assoc_alias.c.ProductID).join(md_alias,assoc_alias.c.MetadataID==md_alias.ID)
            # add filters to filter on these values
            to_apply.append(and_(md_alias.Key==k, md_alias.Value==v))

    query = query.filter(and_(*to_apply))
    query = query.group_by(Product.ID)

    return query
              
class PipelineRun(pipeline_base):
    """ A permanent record that stores information about a past or ongoing Pipeline run.
    
    :class:`PipelineRun` objects are created and recorded each time any :class:`Pipeline` is run. they make a permanent record of important information about who, what, when, and where. On run, a :class:`sagelib.pipeline.Pipeline` will generate a :class:`PipelineRun` record, and update it when they finish. To find products on which to operate, a :class:`sagelib.pipeline.Task` should use its own :func:`sagelib.pipeline.Task.find_products()` method, which will in turn reference the :class:`PipelineRun` .
    """
    __tablename__ = 'PipelineRun'

    ID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True,index=True)
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
    
    def related_product_query(self, dbsession:scoped_session, use_superseded:bool=False, metadata:None|dict=None, **filters:Mapping[str,Any]):
        """Return a Query for products among this PipelineRun's inputs an outputs. optionally, add keyword arguments to filter Products
        
        This Query can be executed using :func:`PipelineRun.run_query` to find the pipeline run's inputs and the outputs of previous task runs in this pipeline run for Products. Ordered by creation datetime, newest first.

        :param dbsession: sqlalchemy database session with which to query
        :param metadata: optional argument of key:value pairs. products will be required to have associated metadata records for each key, each with the specified value
        :param **filters: keyword argument filters to apply to the query. Keys must be columns of the PipelineRun table. supports wildcarding with %

        :returns: list of products 
        """
        query = product_query(dbsession,metadata=metadata,**filters).\
                    filter((Product.producing_pipeline_run_id == self.ID) | (Product.UsedByRunsAsInput.any(PipelineRun.ID == self.ID))).\
                    order_by(Product.creation_dt.desc())
        
        if not use_superseded:
            subq = dbsession.query(SupersessorAssociation).\
                join(Product,SupersessorAssociation.SupersededID==Product.ID).\
                    join(PipelineRun,Product.producing_pipeline_run_id==PipelineRun.ID).\
                        filter(PipelineRun.ID == self.ID).subquery()
            query = query.outerjoin(subq,Product.ID==subq.c.SupersededID).filter(subq.c.SupersededID==null())
        
        return query
    
    def group_product_query(self, group_id:str, dbsession:scoped_session, metadata:None|dict=None, **filters:Mapping[str,Any]):
        """Query for products among this PipelineRun's outputs that were produced by a task with `GroupID` == `group_id`. optionally, add keyword arguments to filter Products.
        
        returns products ordered by creation dt, descending 
        :param group_id: the id of the group from which to look for products
        :param dbsession: sqlalchemy database session with which to query
        :param metadata: optional argument of key:value pairs. products will be required to have associated metadata records for each key, each with the specified value
        :param **filters: keyword argument filters to apply to the query. Keys must be columns of the PipelineRun table. supports wildcarding with %

        :returns: list of products
        """
        query = self.related_product_query(dbsession,metadata=metadata,**filters)

        query = dbsession.query(Product).filter((Product.ProducingTask.TaskGroupID == group_id))
        return query

    def get_related_products(self, dbsession:scoped_session, use_superseded:bool=False, metadata:None|dict=None, **filters:Mapping[str,Any]):
        """Query for products among this PipelineRun's inputs an outputs. optionally, add keyword arguments to filter Products
        
        Query this pipeline run's inputs and the outputs of previous task runs in this pipeline run for Products. Ordered by creation datetime, newest first.

        :param dbsession: sqlalchemy database session with which to query
        :param metadata: optional argument of key:value pairs. products will be required to have associated metadata records for each key, each with the specified value
        :param **filters: keyword argument filters to apply to the query. Keys must be columns of the PipelineRun table. supports wildcarding with %

        :returns: list of products 
        """
        query = self.related_product_query(dbsession,use_superseded,metadata=metadata, **filters)
        related_products = query.all()
        return related_products
    

    def get_group_products(self, group_id:str, dbsession:scoped_session, metadata:None|dict=None, **filters:Mapping[str,Any]):
        """Query for products among this PipelineRun's outputs that were produced by a task with `GroupID` == `group_id`. optionally, add keyword arguments to filter Products.
        
        returns products ordered by creation dt, descending 
        :param group_id: the id of the group from which to look for products
        :param dbsession: sqlalchemy database session with which to query
        :param metadata: optional argument of key:value pairs. products will be required to have associated metadata records for each key, each with the specified value
        :param **filters: keyword argument filters to apply to the query. Keys must be columns of the PipelineRun table. supports wildcarding with %

        :returns: list of products
        """
        query = self.group_product_query(group_id=group_id,dbsession=dbsession,metadata=metadata,**filters)
        related_products = query.all()
        return related_products


class Product(pipeline_base):
    """Inputs and outputs of Pipelines
    
    Products represent data files that are the inputs or outputs to Pipelines. Products that come from external sources (ex FITS files downloaded from the internet) and not from another PipelineRun are considered 'Inputs' (but they are still of class :class:`Product` - no Input class exists). The :py:attr:`task_name` field of Inputs should be ``INPUT``. The first :class:`PipelineRun` to use a given Input is considered its producing pipeline. During this first run, the Input will be inserted into the :class:`sagelib.pipeline.PipelineDB` and marked with the ID of the :class:`PipelineRun` . 

    When creating inputs to a Pipeline (presumably from passed-in filenames or the like), you should **not** add them to the database. :class:`sagelib.pipeline.Pipeline.run` will do that. 

    Products are created in two ways: before a pipeline runs (to then be passed as Inputs) or by Tasks during a pipeline run. 

    **IMPORTANT:** When a :class:`sagelib.pipeline.Task` creates any sort of output product (table, mask, fits file, etc), it *must* create a corresponding Product, set its :py:attr:`producing_task_run_id` to be the Tasks's TaskRun.ID, and add it to the :class:`sagelib.pipeline.PipelineDB` . This is **very important,** as future Tasks in the pipeline can only use products that are in the ``PipelineDB``. This can be done easily with :func:`sagelib.pipeline.Task.publish_output()` like so::

    >>> product = self.publish_output("FitsImage",outpath,flags=None,data_subtype="Coadd",precursors=[image_product]])

    After this, the product is in the database and correctly reflects its origin. Tasks should use :func:`sagelib.pipeline.Task.publish_output()` as their preferred method of creating and recording products.

    :func:`sagelib.pipeline.PipelineDB.record_product()` can also be used to add a :class:`Product` to the database.

    :param data_type: The type of the data that this product represents, ex "FitsImage"
    :param task_name: The name of the task that created this product 
    :param creation_dt: `datetime` object representing when this product was created 
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
                              overlaps="products, precursors")
    derivatives = relationship("Product",
                               secondary='PrecursorProductAssociation',
                               primaryjoin='Product.ID == PrecursorProductAssociation.PrecursorID',
                               secondaryjoin='Product.ID == PrecursorProductAssociation.ProductID',
                               overlaps="precursors, products") # this 'overlaps' seems questionable
    
    supersessors = relationship('Product',
                              secondary='SupersessorAssociation',
                              primaryjoin='Product.ID == SupersessorAssociation.SupersededID',
                              secondaryjoin='Product.ID == SupersessorAssociation.SupersessorID',
                              overlaps="superseded, supersessors")
    superseded = relationship("Product",
                               secondary='SupersessorAssociation',
                               primaryjoin='Product.ID == SupersessorAssociation.SupersessorID',
                               secondaryjoin='Product.ID == SupersessorAssociation.SupersededID',
                               overlaps="supersessors, superseded")

    ProducingPipeline = relationship("PipelineRun", back_populates="OutputProducts")
    # ProducingTaskGroup = relationship("TaskGroup",back_populates="ProductsProduced")
    UsedByRunsAsInput: Mapped[List["PipelineRun"]] = relationship("PipelineRun", secondary='PipelineInputAssociation', back_populates="Inputs")
    ProducingTask = relationship("TaskRun", back_populates="Outputs")
    ProductGroups = relationship("ProductGroup",secondary=ProductProductGroupAssociation,back_populates="Products")
    Metadata: Mapped[List["Metadata"]] = relationship("Metadata",secondary=ProductMetadataAssociation,back_populates="Products")
    # Metadata: Mapped[List["Metadata"]] = relationship("Metadata")



    def __init__(self, data_type: str, task_name: str, creation_dt:datetime, product_location:str, is_input:int, 
                 producing_pipeline_run_id:int | None=None, producing_task_run_id:int | None=None, flags:int | None=None, data_subtype: str | None=None, **kwargs:Mapping[str,Any]):
        date_str = tts(dt_to_utc(creation_dt))
        product_location = abspath(product_location)
        precs = []
        if "precursors" in kwargs:
            precs = kwargs.pop("precursors")
        
        derivs = []
        if "derivatives" in kwargs:
            derivs = kwargs.pop("derivatives")

        super().__init__(data_type=data_type, producing_pipeline_run_id=producing_pipeline_run_id,
                         task_name=task_name, producing_task_run_id=producing_task_run_id, 
                         creation_dt=date_str, product_location=product_location,
                         flags=flags, is_input=is_input, data_subtype=data_subtype, **kwargs)
        
        if precs:
            self.add_precursors(precs)
        
        if derivs:
            self.add_derivatives(derivs)

    def __getitem__(self, index:str) -> str:
        """Retrieve product metadata with key 'index'

        :param index: the key of the metadata
        :type index: str
        :return: metadata value
        :rtype: str
        """
        try:
            return self.metadata_dict()[index]
        except KeyError as e:
            raise KeyError(f"Product {repr(self)} has no metadata record with key '{index}'") from e
    
    def _mdkeys(self):
        return list(self.metadata_dict().keys())

    def getmd(self,key:str,default_val:str|None = None) -> str:
        """Retrieve product metadata with key 'key'. If no such metadata exists, return default_val instead (defaults to None).

        :type key: str
        :type default_val: str
        :return: the value stored in the metadata record, if found, or default_val
        :rtype: str
        """
        return self.metadata_dict().get(key,default_val)


    def __str__(self):
        endline, tab = '\n','\t'
        base_str = f"{'Input ' if self.is_input else ''}Product of type '{self.data_type+(f'.{self.data_subtype}' if self.data_subtype else '')}' created at {self.creation_dt} UTC with {len(self.precursors)} precursors and {len(self.derivatives)} derivatives.\nProducers: Pipeline {self.ProducingPipeline}, Task {self.ProducingTask}."
        if self.precursors:
            base_str += f"\nPrecursors:\n\t{(endline+tab).join([repr(p) for p in self.precursors])}"
        if self.derivatives:
            base_str += f"\nDerivatives:\n\t{(endline+tab).join([repr(d) for d in self.derivatives])}"
        return base_str

    def __repr__(self):
        return f"#{self.ID}: {'Input ' if self.is_input else ''}Product of type '{self.data_type+(f'.{self.data_subtype}' if self.data_subtype else '')}' with {len(self.precursors)} precursors and {len(self.derivatives)} derivatives"
    
    def add_derivative(self,derivative:Product):
        if derivative not in self.derivatives:
            self.derivatives.append(derivative)
            # add our metadata to our derivative
            mdkeys = derivative._mdkeys()
            for m in self.Metadata:
                if m.Key not in mdkeys:
                    derivative.Metadata.append(m)
                    mdkeys.append(m.Key)

    def add_precursor(self,precursor:Product):
        if precursor not in self.precursors:
            self.precursors.append(precursor)
            # copy precursor's metadata to us
            mdkeys = self._mdkeys()
            for m in precursor.Metadata:
                if m.Key not in mdkeys:
                    self.Metadata.append(m)
                    mdkeys.append(m.Key)

    def add_derivatives(self,derivatives:List[Product]):
        for derivative in derivatives:
            self.add_derivative(derivative)
            # if derivative not in self.derivatives:
                # self.derivatives.append(derivative)

    def add_precursors(self,precursors:Product):
        for precursor in precursors:
            self.add_precursor(precursor)
            # if precursor not in self.precursors:
                # self.precursors.append(precursor)

    def traverse_derivatives(self,func:Callable[[Product,Tuple[Any, ...]],dict[Any,Any]| Any],*args:Tuple[Any, ...],pipeline_run:PipelineRun | None=None,maxdepth:int=-1,**kwargs:Mapping[str,Any]):
        """Recursively apply a function to each of the products in the derivative tree of this product, collecting and returning its result
        
        Does a tree-like walk of derivatives of this product, calling ``func`` on each and collecting the results in a dictionary of 
        
        :param func: the function to apply to each derivative. must take the derivative as its first argument. RESULT MUST BE HASHABLE
        :param maxdepth: integer. maximum depth to traverse. any negative number runs forever.
        :param *args: additional arguments to pass to ``func``
        :param **kwargs: additional keyword arguments to pass to ``func``

        :returns: a dictionary of {result of ``func``: list of results of traverse_derivatives on derivatives}
        """
        res = {}
        if not self.derivatives or not maxdepth:
            return res
        for d in self.derivatives:
            if pipeline_run is None or d.ProducingPipeline==pipeline_run:
                res[func(d,*args)] = d.traverse_derivatives(func, *args, pipeline_run=pipeline_run, maxdepth=maxdepth-1,**kwargs)
        return res
    
    # what a horrendous mess. why did i do this to myself
    def all_derivatives(self,pipeline_run:PipelineRun | None=None)-> List[Product]:
        """Traverses tree of derivatives, returning all as a flattened list."""

        deriv_tree = self.traverse_derivatives(lambda s: s, pipeline_run=pipeline_run)
        # print("")
        # print("Deriv tree: ")
        # print(deriv_tree)
        # print("")

        if not deriv_tree:
            return []

        def extract_derivs(tree: dict[Product,Any]):
            contents = []
            if isinstance(tree,Product):
                return contents
                # if not pipeline_run_id or tree.producing_pipeline_run_id==pipeline_run_id:
                #     return [tree]
            if isinstance(tree,dict):
                for k, v in tree.items():
                        contents.extend(extract_derivs(v))
                        if k != self:
                            contents.append(k)
            # if isinstance(tree,list):
            #     for l in tree:
            #         contents.extend(extract_derivs(l))
            return contents
        return list(set(extract_derivs(deriv_tree)))
    
    def visualize_derivatives(self, pipeline_run: PipelineRun|None = None, title:str|None=None, fig:Figure|None=None,ax:Axes|None=None) -> Tuple[Figure,Axes]:
        if title is None:
            title = f"Derivatives of Product {self.ID}"
            if pipeline_run is not None:
                title += f" During Run {pipeline_run.ID}"
        
        id_traversal = self.traverse_derivatives(lambda s: s.ID,pipeline_run=pipeline_run)
        
        return visualize_graph({self.ID:id_traversal},title,fig,ax)
    

    def traverse_precursors(self,func:Callable[[Product,Tuple[Any, ...]],dict[Any,Any]| Any],*args:Tuple[Any, ...],pipeline_run:PipelineRun|None=None,maxdepth:int=-1,**kwargs:Mapping[str,Any]):
        """Recursively apply a function to each of the products in the precursor tree of this product, collecting and returning its result
        
        Does a tree-like walk of precursors of this product, calling ``func`` on each and collecting the results in a dictionary of 
        
        :param func: the function to apply to each precursor. must take the derivative as its first argument. RESULT MUST BE HASHABLE
        :param *args: additional arguments to pass to ``func``
        :param pipeline_run: if provided, will only traverse precursors that are products or inputs of this PipelineRun
        :type pipeline_run: :class:`PipelineRun`
        :param maxdepth: integer. maximum depth to traverse. any negative number runs forever.
        :param **kwargs: additional keyword arguments to pass to ``func``

        :returns: a dictionary of {result of ``func``: list of results of traverse_precursors on precursors}
        """
        res = {}
        if not self.precursors or not maxdepth:
            return res
        for p in self.precursors:
            if pipeline_run is None or p.ProducingPipeline==pipeline_run or pipeline_run in p.UsedByRunsAsInput:
                res[func(p,*args)] = p.traverse_precursors(func, *args, pipeline_run=pipeline_run, maxdepth=maxdepth-1,**kwargs)
        return res
    
    # what a horrendous mess. why did i do this to myself
    def all_precursors(self,pipeline_run:PipelineRun | None=None)-> List[Product]:
        """Traverses tree of precursors, returning all as a flattened list."""

        prec_tree = self.traverse_precursors(lambda s: s, pipeline_run=pipeline_run)

        if not prec_tree:
            return []

        def extract_precursors(tree: dict[Product,Any]):
            contents = []
            if isinstance(tree,Product):
                return contents
            if isinstance(tree,dict):
                for k, v in tree.items():
                        contents.extend(extract_precursors(v))
                        if k != self:
                            contents.append(k)
            return contents
        return list(set(extract_precursors(prec_tree)))
    
    def visualize_precursors(self, pipeline_run: PipelineRun|None = None, title:str|None=None, fig:Figure|None=None,ax:Axes|None=None) -> Tuple[Figure,Axes]:
        if title is None:
            title = f"Precursors of Product {self.ID}"
            if pipeline_run is not None:
                title += f" (Run {pipeline_run.ID})"
        
        id_traversal = self.traverse_precursors(lambda s: s.ID,pipeline_run=pipeline_run)
        
        return visualize_graph({self.ID:id_traversal},title,fig,ax)
    
    def add_metadata(self,task_id:int,**kwargs:Mapping[str,str]):
        """Add key, value pairs to a product as Metadata. **Does not commit to the database - you must do that after running this!**

        :param task_id: the ID of the task adding the metadata
        :type task_id: int
        """
        for k,v in kwargs.items():
            meta = Metadata(self.ID,str(k),str(v),task_id)
            self.Metadata.append(meta)
        
    def metadata_dict(self):
        return {m.Key:m.Value for m in self.Metadata}


class TaskRun(pipeline_base):
    """A :class:`TaskRun` object represents one run of a :class:`sagelib.pipeline.Task` . Constructed by the Pipeline."""
    __tablename__ = 'TaskRun'

    ID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    TaskName = Column(String, nullable=False)
    StartTimeUTC = Column(String, nullable=False)
    EndTimeUTC = Column(String, nullable=True)
    StatusCodes = Column(Integer, nullable=True)
    PipelineRunID = Column(Integer, ForeignKey('PipelineRun.ID'))
    # TaskGroupID = Column(Integer, ForeignKey('TaskGroup.ID'),nullable=True)

    Outputs: Mapped[List["Product"]] = relationship("Product", back_populates="ProducingTask")
    Pipeline = relationship("PipelineRun",back_populates="TaskRuns")
    # TaskGroup = relationship("TaskGroup")

    def __repr__(self):
        return f"'{self.TaskName}' (run #{self.ID})"
    

class ProductGroup(pipeline_base):
    __tablename__ = "ProductGroup"

    ID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    PipelineRunID = Column(Integer, ForeignKey('PipelineRun.ID'),nullable=True)
    ParentGroupID = Column(Integer, ForeignKey('ProductGroup.ID'),nullable=True)

    ParentGroup = relationship("ProductGroup", back_populates="ChildGroups")
    ChildGroups: Mapped[List["ProductGroup"]] = relationship("ProductGroup")

    Products = relationship("Product",secondary=ProductProductGroupAssociation)
    Pipeline = relationship("PipelineRun")

    def __init__(self,PipelineRunID:int|None = None, ParentGroupID: int | None = None, **kwargs:Mapping[str,Any]):
        super().__init__(PipelineRunID=PipelineRunID, ParentGroupID=ParentGroupID, **kwargs)

    def __getitem__(self,index):
        return self.Products[index]


class Metadata(pipeline_base):
    __tablename__ = 'Metadata'

    ID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    ProductID = Column(Integer, ForeignKey('Product.ID'), nullable=False)
    TaskID = Column(Integer, ForeignKey('TaskRun.ID'))
    Key = Column(String, nullable=False)
    Value = Column(String, nullable=False)

    Products: Mapped[List["Product"]] = relationship("Product",secondary=ProductMetadataAssociation,back_populates="Metadata")


    def __init__(self,ProductID:int,Key:str,Value:str,TaskID:int|None=None):
        super().__init__(ProductID=ProductID,TaskID=TaskID,Key=Key,Value=Value)

    def __str__(self):
        return f"Metadata[{self.Key}={self.Value}]"
    
    def __repr__(self):
        return str(self)



class PrecursorProductAssociation(pipeline_base):
    __tablename__ = 'PrecursorProductAssociation'

    PrecursorID = Column(Integer, ForeignKey('Product.ID'), primary_key=True)
    ProductID = Column(Integer, ForeignKey('Product.ID'), primary_key=True)
    
    precursor = relationship('Product', foreign_keys=[PrecursorID], overlaps="derivatives,precursors")
    product = relationship('Product', foreign_keys=[ProductID], overlaps="derivatives,precursors")


class SupersessorAssociation(pipeline_base):
    __tablename__ = 'SupersessorAssociation'

    SupersessorID = Column(Integer, ForeignKey('Product.ID'), primary_key=True)
    SupersededID = Column(Integer, ForeignKey('Product.ID'), primary_key=True)
        
    supersessor = relationship('Product', foreign_keys=[SupersessorID], overlaps="supersessors,supersedes,superseded")
    superseded = relationship('Product', foreign_keys=[SupersededID], overlaps="supersessors,supersedes")