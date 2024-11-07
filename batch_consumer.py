from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd

spark = (
    SparkSession.builder.getOrCreate()
)
# Not fully working yet.

# Idea
# The Idea is to retrieve data in batches from the consumer and do some data analyst with PySpark.

# What is functional right now?
# It is possible to recieve the batch data and create a spark dataframe.

# What is not functional
# It is not currently functional to operate on the spark dataframe

# Expectations of a working solution
# For a working solution, PySpark should read total amount of production amount in a batch, and comparing it to the next batch.
# In the comparison we should see either a % increase or decrease.

def show_batch(pd_df):
    dataframe = pd.DataFrame(pd_df)
    print('Batch data')
    print(dataframe)
    df = spark.createDataFrame(dataframe)
    #df.printSchema() # printSchema works

    # df.show() # Show not working, other spark operation also does not work