#import libraries
from google.colab import userdata
import os
import time
import logging
#set up logging
import logging

# Clear existing handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Reconfigure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

#to unzip imported data
import zipfile

#standard analytics libraries
import pandas as pd
import numpy as np
import string

#spark for parallelism
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark Example") \
    .getOrCreate()


#for market basket analysis
#from apyori import apriori
