import os
import sys

# Point Spark's Python workers at the interpreter running this code, so
# driver and workers agree on the environment. Runs on first import of any
# ingestion module, before a SparkSession is created; setdefault leaves an
# explicit override in the environment alone.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
