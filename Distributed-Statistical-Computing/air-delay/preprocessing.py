#! /usr/bin/env python3.6
import findspark 
findspark.init('/usr/lib/spark-current')
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler


spark = SparkSession.builder.appName("air-delay-task").master('yarn').config('spark.port.maxRetries','32').getOrCreate()



schema_sdf = StructType([
        StructField('Year', IntegerType(), True),
        StructField('Month', IntegerType(), True),
        StructField('DayofMonth', IntegerType(), True),
        StructField('DayOfWeek', IntegerType(), True),
        StructField('DepTime', DoubleType(), True),
        StructField('CRSDepTime', DoubleType(), True),
        StructField('ArrTime', DoubleType(), True),
        StructField('CRSArrTime', DoubleType(), True),
        StructField('UniqueCarrier', StringType(), True),
        StructField('FlightNum', StringType(), True),
        StructField('TailNum', StringType(), True),
        StructField('ActualElapsedTime', DoubleType(), True),
        StructField('CRSElapsedTime', DoubleType(), True),
        StructField('AirTime', DoubleType(), True),
        StructField('ArrDelay', DoubleType(), True),
        StructField('DepDelay', DoubleType(), True),
        StructField('Origin', StringType(), True),
        StructField('Dest', StringType(), True),
        StructField('Distance', DoubleType(), True),
        StructField('TaxiIn', DoubleType(), True),
        StructField('TaxiOut', DoubleType(), True),
        StructField('Cancelled', IntegerType(), True),
        StructField('CancellationCode', StringType(), True),
        StructField('Diverted', IntegerType(), True),
        StructField('CarrierDelay', DoubleType(), True),
        StructField('WeatherDelay', DoubleType(), True),
        StructField('NASDelay', DoubleType(), True),
        StructField('SecurityDelay', DoubleType(), True),
        StructField('LateAircraftDelay', DoubleType(), True)
    ])
air = spark.read.options(header='true').schema(schema_sdf).csv('/user/devel/2020210990Renqiang/data/airdelay_small.csv')


usecols_x = [
        'Year',   
        'Month',
        'DayofMonth',
        'DayOfWeek',
        'DepDelay', 
        'CRSDepTime',
        'CRSArrTime',
        'UniqueCarrier',
        'ActualElapsedTime',  # 'AirTime',
        'Origin',
        'Dest',
        'Distance'
    ]
Y_name = ['ArrDelay']
var_colnames = Y_name + usecols_x
air = air.select(var_colnames)
air = air.na.drop()

nrow = air.count()

air = air.withColumn('CRSDepTime',((floor(air['CRSDepTime']/100)+round((air['CRSDepTime']-floor(air['CRSDepTime']/100)*100)/60)).cast('Int')))
air = air.withColumn('CRSArrTime',((floor(air['CRSArrTime']/100)+round((air['CRSArrTime']-floor(air['CRSArrTime']/100)*100)/60)).cast('Int')))

dummies_col1 = ['UniqueCarrier','Origin','Dest']

p=0.03
cum_p = 0.8
factor_set = {}
factor_dropped = {}
dummies_factor_dict = {}
for i in range(len(dummies_col1)):
    var_count =  air.groupby(dummies_col1[i]).count().sort('count',ascending=False)
    var_count = var_count.toPandas()
    var_count['count'] = var_count['count']/nrow
    var_cumsum = var_count['count'].cumsum()
    keep_list = var_count.iloc[:(np.where(var_cumsum>=cum_p)[0][0]+1)][var_count['count']>p].iloc[:,0].to_list()
    dummies_factor_dict[dummies_col1[i]] = keep_list
    factor_set[dummies_col1[i]] = var_count.iloc[:,0]
    factor_dropped[dummies_col1[i]] = list(set(factor_set[dummies_col1[i]]) - set(keep_list))

dummies_col2 = ['year','month','DayofMonth','DayofWeek','CRSDepTime','CRSArrTime']
for i in range(len(dummies_col2)):
    var_count = air.groupby(dummies_col2[i]).count().sort('count',ascending=False)
    factor_set[dummies_col2[i]] = var_count.select(dummies_col2[i]).rdd.flatMap(lambda x:x).collect()


year_v = list(range(1989,2008))
month_v = list(range(2,13))
DayofMonth_v = list(range(1,30))
DayofWeek_v = list(range(2,8))
CRSDepTime_v = list(range(7,22))
CRSArrTime_v = list(range(8,23))
v = [year_v,month_v,DayofMonth_v,DayofWeek_v,CRSDepTime_v,CRSArrTime_v]
for i in range(len(dummies_col2)):
    factor_dropped[dummies_col2[i]] = list(set(factor_set[dummies_col2[i]]) - set(v[i])) 
    temp = factor_set[dummies_col2[i]].copy()
    for x in  factor_dropped[dummies_col2[i]]:
        temp.remove(x)  
    dummies_factor_dict[dummies_col2[i]] = temp


dummy_info = {'factor_set':factor_set,'factor_selected':dummies_factor_dict,
'factor_dropped':factor_dropped}


dummies_col = dummies_col1 + dummies_col2
replacement = '9999'
for i in range(len(dummies_col)):
    pattern =   '^(?!'  + '|'.join(list(map(str,dummies_factor_dict[dummies_col[i]])))+').+'
    air =  air.withColumn(dummies_col[i],regexp_replace(air[dummies_col[i]],pattern,replacement))

air = air.withColumn(Y_name[0],(air[Y_name[0]]>0).cast('Int'))


# The index of string vlaues multiple columns
indexers = [StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c)) for c in dummies_col]
# The encode of indexed vlaues multiple columns
encoders = [OneHotEncoder(dropLast=True,inputCol=indexer.getOutputCol(),
            outputCol="{0}_encoded".format(indexer.getOutputCol())) 
    for indexer in indexers ]

# Vectorizing encoded values

assembler = VectorAssembler(inputCols=['ActualElapsedTime','Distance']+[encoder.getOutputCol() for encoder in encoders],outputCol="features")

pipeline = Pipeline(stages=indexers + encoders+[assembler])
model=pipeline.fit(air)
transformed = model.transform(air)
air2 = transformed[['ArrDelay','features']]

air2.show(5)


from pyspark.ml.classification import LogisticRegression

(trainingData,testData) = air2.randomSplit([0.7,0.3])
lr = LogisticRegression(featuresCol='features',labelCol='ArrDelay')
lr_model = lr.fit(trainingData)
prediction = lr_model.transform(testData)
result = prediction.select('ArrDelay','features','probability','prediction')
result.show(5)








