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

PrecursorProductAssociation = Table(
    'PrecursorProductAssociation',
    pipeline_base.metadata,
    Column('PrecursorID', Integer, ForeignKey('Product.ID'), nullable=False,primary_key=True),
    Column('ProductID', Integer, ForeignKey('Product.ID'), nullable=False,primary_key=True),
    # PrimaryKeyConstraint('PrecursorID', 'ProductID'),
    extend_existing=True
)


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
    # OutputProduct: Mapped[List["Product"]] = relationship("Product")
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
    Precursors: Mapped[List["Product"]] = relationship("Product", secondary='PrecursorProductAssociation', 
                                                       primaryjoin=ID==PrecursorProductAssociation.c.ProductID,
                                                       secondaryjoin=ID==PrecursorProductAssociation.c.PrecursorID)
    # Precursors: Mapped[List["Product"]] = relationship("Product", secondary='PrecursorProductAssociation', back_populates="Derivatives",remote_side=[ID])
    # Derivatives = relationship("Product")
    # ProducingPipeline: Mapped[List["PipelineRun"]] = relationship("Product", back_populates="OutputProduct",primaryjoin=pipeline_run_id==PipelineRun.ID)
    UsedByRunsAsInput: Mapped[List["PipelineRun"]] = relationship("PipelineRun", secondary='PipelineInputAssociation', back_populates="Inputs")
    ProducingTask = relationship("TaskRun", back_populates="Outputs")

class TaskRun(pipeline_base):
    __tablename__ = 'TaskRun'

    TaskName = Column(String, nullable=False)
    StartTimeUTC = Column(String, nullable=False)
    EndTimeUTC = Column(String, nullable=False)
    StatusCodes = Column(Integer, nullable=False)
    PipelineRunID = Column(Integer, ForeignKey('PipelineRun.ID'))
    ID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    Outputs: Mapped[List["Product"]] = relationship("TaskRun")


# # model for the candidate (target) object
# class CandidateModel(candidate_base):
#     __tablename__ = 'Candidates'

#     ID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
#     Author = Column(String, nullable=False)
#     DateAdded = Column(String, nullable=False)
#     DateLastEdited = Column(String)
#     CandidateName = Column(String, nullable=False)
#     Priority = Column(Integer, nullable=False)
#     CandidateType = Column(String, nullable=False)
#     Updated = Column(String)
#     StartObservability = Column(String)
#     EndObservability = Column(String)
#     TransitTime = Column(String)
#     RejectedReason = Column(String)
#     RemovedReason = Column(String)
#     RemovedDt = Column(String)
#     RA = Column(Numeric)
#     Dec = Column(Numeric)
#     dRA = Column(Numeric)
#     dDec = Column(Numeric)
#     Magnitude = Column(Numeric)
#     RMSE_RA = Column(Numeric)
#     RMSE_Dec = Column(Numeric)
#     nObs = Column(Integer)
#     Score = Column(Integer)
#     ApproachColor = Column(String)
#     ExposureTime = Column(Numeric)
#     NumExposures = Column(Integer)
#     Scheduled = Column(Integer, default=0)
#     Observed = Column(Integer, default=0)
#     Processed = Column(Numeric, default=0)
#     Submitted = Column(Integer, default=0)
#     Notes = Column(Text)
#     CVal1 = Column(Text)
#     CVal2 = Column(Text)
#     CVal3 = Column(Text)
#     CVal4 = Column(Text)
#     CVal5 = Column(Text)
#     CVal6 = Column(Text)
#     CVal7 = Column(Text)
#     CVal8 = Column(Text)
#     CVal9 = Column(Text)
#     CVal10 = Column(Text)
#     Filter = Column(String)
#     Observations: Mapped[List["Observation"]] = relationship("Observation", back_populates="candidate")

#     def as_dict(self):
#         return {c.name: getattr(self, c.name) for c in self.__table__.columns}

# # model for the observation object
# class Observation(candidate_base):
#     __tablename__ = 'Observation'

#     CandidateID = Column(Integer, ForeignKey('Candidates.ID'))
#     ObservationID = Column(Integer, primary_key=True, nullable=False)
#     RMSE_RA = Column(Numeric)
#     RMSE_Dec = Column(Numeric)
#     RA = Column(Numeric)
#     Dec = Column(Numeric)
#     ApproachColor = Column(String)
#     AstrometryStatus = Column(String)
#     ExposureTime = Column(Numeric)
#     EncoderRA = Column(Numeric)
#     EncoderDec = Column(Numeric)
#     SkyBackground = Column(Numeric)
#     Temperature = Column(Numeric)
#     Dataset = Column(String)
#     CaptureStartEpoch = Column(Numeric)
#     Focus = Column(Numeric)
#     RAOffset = Column(Numeric) # deg
#     DecOffset = Column(Numeric) # deg
#     SystemName = Column(String)
#     CameraName = Column(String)
#     # ProcessingCodesCol = Column(String)
#     Submitted = Column(Integer, nullable=False)
#     Comments = Column(Text)

#     candidate = relationship('CandidateModel', back_populates='Observations')
#     ProcessingCode: Mapped[List["ProcessingCode"]] = relationship("ProcessingCode", secondary='ObservationCodeAssociation', back_populates="Observations")

#     def as_dict(self):
#         return {c.name: getattr(self, c.name) for c in self.__table__.columns}

# # model for the processing code
# class ProcessingCode(candidate_base):
#     __tablename__ = 'ProcessingCode'

#     ID = Column(Integer, primary_key=True, autoincrement=True)
#     Code = Column(Integer, nullable=False, unique=True)
#     Description = Column(String, nullable=False)
#     Observations: Mapped[List["Observation"]] = relationship("Observation", secondary='ObservationCodeAssociation', back_populates="ProcessingCode")

#     def as_dict(self):
#         return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    

