#! /usr/bin/env python3.6
import findspark
findspark.init('/usr/lib/spark-current')
import pyspark
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
import pandas as pd
import numpy as np



def sdummies(sdf, dummy_columns):

    total = sdf.count()

    factor_set = {}  # The full dummy sets
    for string_col in dummy_columns:
        # Descending sorting with counts
        sdf_column_count = sdf.groupBy(string_col).count().orderBy(
                'count', ascending=False)
        sdf_column_count = sdf_column_count.withColumn(
                "perc",col('count')/total)

        keep_list = dict(sdf_column_count.select(string_col,'perc').rdd.map(lambda x: tuple(x)).collect())

            # Save factor sets
        factor_set[string_col] = keep_list
    return factor_set 
