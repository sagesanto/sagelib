import sys,os

try:
    from .pipeline_db.db_config import configure_db
    from .pipeline_db.models import Product, PipelineRun, TaskRun, Metadata, TaskGroup, ProductGroup
except ImportError:
    sys.path.append(os.path.dirname(__file__))
    from pipeline_db.db_config import configure_db
    from pipeline_db.models import Product, PipelineRun, TaskRun, Metadata, TaskGroup, ProductGroup
    sys.path.remove(os.path.dirname(__file__))


_all = [os.path.splitext(f)[0] for f in os.listdir(os.path.dirname(__file__)) if f.endswith('.py') and not f.startswith('_')]
_all.extend(["Product","PipelineRun","TaskRun","Metadata","TaskGroup","ProductGroup","configure_db"])
_all.append('pipeline_db')
__all__ = _all