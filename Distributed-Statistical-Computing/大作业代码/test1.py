#! /usr/bin/env python3.6
import findspark
findspark.init('/usr/lib/spark-current')
import pyspark
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler

from pyspark.sql.window import Window
import sys


def sdummies(sdf,
                 dummy_columns,
                 keep_top,
                 threshold = 0.03,
                 replace_with='000_OTHERS'):

    total = sdf.count()
    column_i = 0

    factor_set = {}  # The full dummy sets
    factor_selected = {}  # Used dummy sets
    factor_dropped = {}  # Dropped dummy sets
    factor_selected_names = {}  # Final revised factors
    factor_pro = {} # The proporation of factors
    for string_col in dummy_columns:
        # Descending sorting with counts
        sdf_column_count = sdf.groupBy(string_col).count().orderBy(
                'count', ascending=False)
        
        sdf_column_count = sdf_column_count.withColumn(
                "cumsum",
                F.sum("count").over(Window.partitionBy("count")))
        '''
        sdf_column_count = sdf_column_count.withColumn(
                "cumperc",col('cumsum')/total)
        sdf_column_count = sdf_column_count.withColumn(
                "perc",col('count')/total)
            # Obtain top dummy factors
        
        sdf_column_top_dummies = sdf_column_count.filter((col('cumperc') <= keep_top[column_i])&(col('perc')>=threshold))
        keep_list = dict(sdf_column_top_dummies.select(string_col,'perc').rdd.map(lambda x: tuple(x)).collect())
        perc_list = list(keep_list.values())
        keep_list = list(keep_list.keys())

            # Save factor sets
        factor_set[string_col] = dict(sdf_column_count.select(
                string_col,'perc').rdd.map(lambda x: tuple(x)).collect())
        factor_selected[string_col] = keep_list
        factor_dropped[string_col] = list(set(factor_set[string_col]) - set(keep_list))
        factor_pro[string_col] = perc_list
            # factor_selected_names[string_col] = [string_col + '_' + str(x) for x in factor_new ]

            # Replace dropped dummies with indicators like `others`
        if len(factor_dropped[string_col]) == 0:
            factor_new = []
        else:
            factor_new = [replace_with]
        factor_new.extend(factor_selected[string_col])

        factor_selected_names[string_col] = [
                string_col + '_' + str(x) for x in factor_new
          ]
        ''' 
        break
    return sdf_column_count   
        
    dummy_info = {
            'factor_set': factor_set,
            'factor_selected': factor_selected,
            'factor_proportion': factor_pro,
            'factor_dropped': factor_dropped,
            'factor_selected_names': factor_selected_names
        }
    return  dummy_info,sdf_column_count,sdf_column_top_dummies
