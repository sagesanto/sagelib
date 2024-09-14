import sys,os

try:
    from .pipeline_db.db_config import configure_db
    from .pipeline_db.models import Product, PipelineRun, TaskRun, Metadata, ProductGroup, PipelineInputAssociation, PrecursorProductAssociation, ProductProductGroupAssociation, SupersessorAssociation, ProductMetadataAssociation, product_query
except ImportError:
    sys.path.append(os.path.dirname(__file__))
    from pipeline_db.db_config import configure_db
    from pipeline_db.models import Product, PipelineRun, TaskRun, Metadata, ProductGroup, PipelineInputAssociation, PrecursorProductAssociation, ProductProductGroupAssociation, SupersessorAssociation, ProductMetadataAssociation, product_query
    # sys.path.remove(os.path.dirname(__file__))

py_in_dir = [os.path.splitext(f)[0] for f in os.listdir(os.path.dirname(__file__)) if f.endswith('.py') and not f.startswith('_')]

from_db = ["Product","PipelineRun","TaskRun","Metadata","ProductGroup","configure_db", "PipelineInputAssociation", "PrecursorProductAssociation", "ProductProductGroupAssociation", "SupersessorAssociation", "ProductMetadataAssociation", "product_query"]

__all__ = ['pipeline_db'] + py_in_dir + from_db