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

class Product(pipeline_base):
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
                              backref='derivatives')
    ProducingPipeline = relationship("PipelineRun", back_populates="OutputProducts")
    UsedByRunsAsInput: Mapped[List["PipelineRun"]] = relationship("PipelineRun", secondary='PipelineInputAssociation', back_populates="Inputs")
    ProducingTask = relationship("TaskRun", back_populates="Outputs")

    def __init__(self, data_type, task_name, creation_dt, product_location, is_input, 
                 producing_pipeline_run_id=None, producing_task_run_id=None, flags=None, data_subtype=None, **kwargs):
        super().__init__(data_type=data_type, producing_pipeline_run_id=producing_pipeline_run_id,
                         task_name=task_name, producing_task_run_id=producing_task_run_id,
                         creation_dt=creation_dt, product_location=product_location,
                         flags=flags, is_input=is_input, data_subtype=data_subtype, **kwargs)


class TaskRun(pipeline_base):
    __tablename__ = 'TaskRun'

    TaskName = Column(String, nullable=False)
    StartTimeUTC = Column(String, nullable=False)
    EndTimeUTC = Column(String, nullable=True)
    StatusCodes = Column(Integer, nullable=True)
    PipelineRunID = Column(Integer, ForeignKey('PipelineRun.ID'))
    ID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    Outputs: Mapped[List["Product"]] = relationship("Product", back_populates="ProducingTask")