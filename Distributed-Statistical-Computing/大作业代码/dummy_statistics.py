#! /usr/bin/env python3.6
import findspark
findspark.init('/usr/lib/spark-current')
import pyspark
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
import pandas as pd
import numpy as np


def dummy_statistics(sdf,Y_column, dummy_columns):
    dummys_df = []  # The full dummy sets
    for string_col in dummy_columns:
        # Descending sorting with counts
        sdf_column_count = sdf.groupBy(string_col,Y_column).count().orderBy(
                string_col,Y_column)
        df = sdf_column_count.toPandas()
        dummys_df.append(df)

    return dummys_df 
