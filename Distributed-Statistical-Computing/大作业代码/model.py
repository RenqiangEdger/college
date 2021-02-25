#! /usr/bin/env python3.6
import findspark 
findspark.init('/usr/lib/spark-current')
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql.functions import col, when
from pyspark.sql.window import Window
import sys
import seaborn as sns


def get_sdummies(sdf,
                 dummy_columns,
                 keep_top,
                 threshold = 0.03,
                 replace_with='000_OTHERS',
                 dummy_info=[],
                 dropLast=True):
    """Index string columns and group all observations that occur in less then a keep_top% of the rows in sdf per column.
    :param sdf: A pyspark.sql.dataframe.DataFrame
    :param dummy_columns: String columns that need to be indexed
    :param keep_top: List [1, 0.8, 0.8]
    :param threshold:the min proporation of factor
    :param replace_with: String to use as replacement for the observations that need to be
    grouped.
    :param dropLast: bool. Whether to get k-1 dummies out of k categorical levels by
    removing the last level. Note that is behave differently with pandas.get_dummies()
    where it drops the first level.
    return sdf, dummy_info
    """
    total = sdf.count()
    column_i = 0

    factor_set = {}  # The full dummy sets
    factor_selected = {}  # Used dummy sets
    factor_dropped = {}  # Dropped dummy sets
    factor_selected_names = {}  # Final revised factors
    factor_pro = {} # The proporation of factors
    for string_col in dummy_columns:

        if len(dummy_info) == 0:
            # Descending sorting with counts
            sdf_column_count = sdf.groupBy(string_col).count().orderBy(
                'count', ascending=False)
            sdf_column_count = sdf_column_count.withColumn(
                "cumsum",
                F.sum("count").over(Window.partitionBy("count")))
            sdf_column_count = sdf_column_count.withColumn(
                "cumperc",col('cumsum')/total)
            sdf_column_count = sdf_column_count.withColumn(
                "perc",col('count')/total)
            # Obtain top dummy factors
            sdf_column_top_dummies = sdf_column_count.filter((col('cumperc') <= keep_top[column_i])&(col('perc')>=threshold))
            keep_list = sdf_column_top_dummies.select(string_col,'perc').rdd.map(
                lambda x: tuple(x)).collect()
            keep_list = dict(keep_list)
            perc_list = list(keep_list.values())
            keep_list = list(keep_list.keys())

            # Save factor sets
            factor_set[string_col] = dict(sdf_column_count.select(string_col,'perc').rdd.map(lambda x:tuple(x)).collect())
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



        else:
            keep_list = dummy_info["factor_selected"][string_col]

        # Replace dropped dummy factors with grouped factors.
        sdf = sdf.withColumn(
            string_col,
            when((col(string_col).isin(keep_list)),
                 col(string_col)).otherwise(replace_with))
        column_i += 1

    # The index of string vlaues multiple columns
    indexers = [
        StringIndexer(inputCol=c, outputCol="{0}_IDX".format(c))
        for c in dummy_columns
    ]

    # The encode of indexed vlaues multiple columns
    encoders = [
        OneHotEncoder(dropLast=dropLast,
                      inputCol=indexer.getOutputCol(),
                      outputCol="{0}_ONEHOT".format(indexer.getOutputCol()))
        for indexer in indexers
    ]

    # Vectorizing encoded values
    assembler = VectorAssembler(
        inputCols=[encoder.getOutputCol() for encoder in encoders],
        outputCol="features_ONEHOT")

    pipeline = Pipeline(stages=indexers + encoders + [assembler])
    # pipeline = Pipeline(stages=[assembler])
    onehot_model = pipeline.fit(sdf)
    sdf = onehot_model.transform(sdf)

    # Drop intermediate columns
    drop_columns = [x + "_IDX" for x in dummy_columns]
    drop_columns = [x + "_ONEHOT" for x in drop_columns] 

    sdf = sdf.drop(*drop_columns)

    if len(dummy_info) == 0:
        dummy_info = {
            'factor_set': factor_set,
            'factor_selected': factor_selected,
            'factor_proportion': factor_pro,
            'factor_dropped': factor_dropped,
            'factor_selected_names': factor_selected_names
        }

    return sdf, dummy_info


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

def dummy_plot(list_df,Y_column,dummy_columns):
    for k in range(len(dummy_columns)):
        df = list_df[k].copy()
        temp = df.groupby(dummy_columns[k]).agg([np.sum,np.size])['count']
        ncount = [temp['sum'][i] for i in range(len(temp['size'])) for j in range(temp['size'][i])]
        df['count'] = df['count']/ncount
        figure = sns.barplot(x=dummy_columns[k],y='count',hue=Y_column,data=df)
        fig = figure.get_figure()
        fig.savefig(dummy_columns[k]+'.png')


def dummy_statistics(sdf,Y_column, dummy_columns):
    dummys_df = []  # The full dummy sets
    for string_col in dummy_columns:
        # Descending sorting with counts
        sdf_column_count = sdf.groupBy(string_col,Y_column).count().orderBy(
                string_col,Y_column)
        df = sdf_column_count.toPandas()
        dummys_df.append(df)

    return dummys_df



spark = SparkSession.builder.appName("NYPD-Complaint-task").master('yarn').config('spark.port.maxRetries','32').config('spark.executor.memory','5g').getOrCreate()

schema_sdf = StructType([
        StructField('_c0', IntegerType(), True),
        StructField('LAW_CAT_CD', StringType(), True),
        StructField('CMPLNT_FR_DT', StringType(), True),
        StructField('CMPLNT_FR_TM', StringType(), True),
        StructField('CMPLNT_TO_DT', StringType(), True),
        StructField('CMPLNT_TO_TM', StringType(), True),
        StructField('OFNS_DESC', StringType(), True),
        StructField('BORO_NM', StringType(), True),
        StructField('LOC_OF_OCCUR_DESC', StringType(), True),
        StructField('PREM_TYP_DESC', StringType(), True),
        StructField('X_COORD_CD', IntegerType(), True),
        StructField('Y_COORD_CD', IntegerType(), True),
        StructField('PATROL_BORO', StringType(), True),
        StructField('VIC_AGE_GROUP', StringType(), True),
        StructField('VIC_RACE', StringType(), True),
        StructField('VIC_SEX', StringType(), True),
        StructField('SUSP_RACE', StringType(), True),
        StructField('SUSP_SEX', StringType(), True),
        StructField('HOUR', IntegerType(), True),
        StructField('MINUTE', IntegerType(), True), 
        StructField('YEAR', IntegerType(), True), 
        StructField('MONTH', IntegerType(), True), 
        StructField('WEEKOFDAY', IntegerType(), True), 
        StructField('DATEDIFF', IntegerType(), True)

    ])
data = spark.read.options(header='true').schema(schema_sdf).csv('/user/devel/2020210990Renqiang/data/small_data.csv')


dummy_columns = ['OFNS_DESC','PREM_TYP_DESC','BORO_NM','LOC_OF_OCCUR_DESC', 'PATROL_BORO','VIC_AGE_GROUP','VIC_RACE','VIC_SEX','SUSP_RACE','SUSP_SEX','YEAR','MONTH','WEEKOFDAY','HOUR','DATEDIFF','LAW_CAT_CD']
factor_set = sdummies(data,dummy_columns)
age_list = list(factor_set['VIC_AGE_GROUP'].keys())[:5]
data = data.filter(col('VIC_AGE_GROUP').isin(age_list))

data = data.withColumn('SUSP_SEX',when(col('SUSP_SEX')=='U','UNKNOWN').otherwise(col('SUSP_SEX')))

data = data.withColumn('DATEDIFF',when(col('DATEDIFF')>=2,2).otherwise(col('DATEDIFF')))

eve_hour = [23,24,0,1,2,3,4,5,6]
morning_hour = list(range(7,19))
after_hour = list(range(19,23))

data = data.withColumn('HOUR',when(col('HOUR').isin(eve_hour),111).otherwise(col('HOUR')))
data = data.withColumn('HOUR',when(col('HOUR').isin(morning_hour),222).otherwise(col('HOUR')))
data = data.withColumn('HOUR',when(col('HOUR').isin(after_hour),333).otherwise(col('HOUR')))

dummy_columns2 = dummy_columns.copy()[:-1]
dummy_columns2.remove('YEAR')

keep_top = [0.8,0.8,1,0.95,1,1,0.9,1,0.95,1,1,1,1,1]
data, dummy_info = get_sdummies(data,dummy_columns2,keep_top,threshold=0.05)

data = data.na.drop()
nrow = data.count()
# descriptive analysis 
## ablout date varibles

'''
dummys_stat = dummy_statistics(data,'LAW_CAT_CD'LAW_CAT_CD,dummy_columns2)  
figure = dummy_plot(dummys_stat,'LAW_CAT_CD',dummy_columns2)

dummys_df = dummys_stat[0]
for i in dummys_stat[1:]:
    dummys_df = dummys_df.append(i,ignore_index=True)
dummys_df.to_csv('descirpition.csv')
'''
# model 

indexer = StringIndexer(inputCol = 'LAW_CAT_CD',outputCol = 'label')

# Vectorizing encoded values
assembler = VectorAssembler(inputCols=['YEAR','X_COORD_CD','Y_COORD_CD']+['features_ONEHOT'],outputCol="features")

assembler2 = VectorAssembler(inputCols=['YEAR','X_COORD_CD','Y_COORD_CD']+ [i+'_IDX' for i in dummy_columns2],outputCol='features2')                                                                            
pipeline = Pipeline(stages=[indexer,assembler,assembler2])

data_transform=pipeline.fit(data).transform(data)

y_factors = list(factor_set['LAW_CAT_CD'].keys())
for i in range(len(y_factors)):
    data_transform = data_transform.withColumn('label'+str(i),(when(col('LAW_CAT_CD')==y_factors[i],1).otherwise(0)).cast(DoubleType()))

#data_transform = data_transform.withColumn('label',col('label').cast("Int"))

# linear SVM


from pyspark.ml.classification import LinearSVC,RandomForestClassifier,RandomForestClassificationModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator,MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder,CrossValidator

def getBestParam(cvModel):
    params = cvModel.getEstimatorParamMaps()
    avgMetrics = cvModel.avgMetrics

    all_params = list(zip(params, avgMetrics))
    best_param = sorted(all_params, key=lambda x: x[1], reverse=True)[0]
    return best_param,all_params


def multi_svm_cv_predict(labels,trainingdata,testdata):
    
    best_params_all = []
    params_all = []
    for i in labels:
        SVC = LinearSVC(labelCol=i,standardization=False)
        ParamGrid = ParamGridBuilder().addGrid(SVC.maxIter,[10,50]).addGrid(SVC.regParam,[0.01,0.05,0.1]).addGrid(SVC.aggregationDepth,[2,4]).build()
        evaluator = BinaryClassificationEvaluator(labelCol=i)
        cv = CrossValidator(estimator=SVC,evaluator=evaluator,estimatorParamMaps=ParamGrid,parallelism=2)
        svcmodel = cv.fit(trainingdata)
        testdata = svcmodel.transform(testdata)
        best_param, all_params = getBestParam(svcmodel)
        best_params_all.append(best_param)
        params_all.append(all_params)
        testdata = testdata.withColumnRenamed('prediction','prediction_'+i)
        testdata = testdata.withColumnRenamed('rawPrediction','rawPrediction_'+i)
    testdata = testdata.withColumn('prediction_all',F.array(col('prediction_'+labels[0]),col('prediction_'+labels[1])).cast(ArrayType(DoubleType())))
    
#    testdata.show(5)
    print('参数快出来')
    print(svcmodel.getEstimatorParamMaps())
    print('参数：',best_params_all)
    print('length',len(best_params_all),type(best_params_all))
    testdata.show(6)
    return testdata,best_params_all,params_all


def multi_svm_prediction(x):
    ones = np.where(np.array(x)==1)[0]
    length = len(ones)
    if length==0:
        return float(2)
    else:
        return float(ones[0])
        


argmax_udf = F.udf(multi_svm_prediction,DoubleType())



trainingdata,testdata = data_transform.randomSplit([0.8,0.2])

prediction_svm,best_params_all,params_all = multi_svm_cv_predict(labels=['label'+str(i) for i in range(2)],trainingdata=trainingdata,testdata=testdata)

print('参数',len(best_params_all),best_params_all)
print('真的是你吗，他们呢！！！')
prediction_svm.show(5)
print(prediction_svm.head(1))
prediction_svm = prediction_svm.withColumn('prediction',argmax_udf(col('prediction_all')))
prediction_svm.show(5)
evaluator_acc = MulticlassClassificationEvaluator(labelCol='label',metricName='accuracy')


acc = evaluator_acc.evaluate(prediction_svm)

print(best_params_all)
print('线性支持向量机accuracy',acc)
print('线性支持向量机最优参数',best_params_all)


# randomForest

rf = RandomForestClassifier(labelCol='label',featuresCol='features2')
ParamGrid_rf = ParamGridBuilder().addGrid(rf.maxDepth,[6,8,10]).addGrid(rf.numTrees,[20,25,30]).addGrid(rf.featureSubsetStrategy, ['onethird', 'sqrt', 'log2']).build()
evaluator_rf = MulticlassClassificationEvaluator(labelCol='label')
cv_rf = CrossValidator(estimator=rf,evaluator=evaluator_rf,estimatorParamMaps=ParamGrid_rf,parallelism=2)
rf_model = cv_rf.fit(trainingdata)

rf_predictions = rf_model.transform(testdata)


acc_rf = evaluator_acc.evaluate(rf_predictions)

rf_model_path = '/home/devel/students/2020210990Renqiang/spark/complaint_data/bestmodel/rfModel'
rf_model.bestModel.write().overwrite().save(rf_model_path)
best_model_rf = RandomForestClassificationModel.load(rf_model_path)

print('随机森林accuracy',acc_rf)
print('随机森林最优参数',getBestParam(rf_model))

print('变量重要程度',best_model_rf.featureImportances)









