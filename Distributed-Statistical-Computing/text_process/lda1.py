#! /usr/bin/env python3.6
import findspark 
findspark.init('/usr/lib/spark-current')
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pandas as pd
import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer,IDF,RegexTokenizer,Tokenizer,StopWordsRemover,NGram
from pyspark.ml.clustering import LDA



spark = SparkSession.builder.appName("text-model").master('yarn').config('spark.port.maxRetries','32').getOrCreate()

schema_sdf = StructType([StructField('paragraph', StringType(), True)])
text = spark.read.options(header='false').schema(schema_sdf).csv('/user/devel/2020210990Renqiang/data/speech.txt',sep='\n')


tokenizer = RegexTokenizer(inputCol='paragraph',outputCol='words',gaps=False,pattern='[a-z|A-Z]+')
text = tokenizer.transform(text)
text.show(5)

remover = StopWordsRemover(inputCol = 'words',outputCol = 'filtered_words')
text = remover.transform(text)
text.show(5)

ngramer = NGram(n = 2,inputCol = 'filtered_words',outputCol = 'ngrams')
text = ngramer.transform(text)
text.show(5)


count_vec = CountVectorizer(inputCol=ngramer.getOutputCol(),outputCol='ft_features')
count_vec_model = count_vec.fit(text)
vocab = count_vec_model.vocabulary
text = count_vec_model.transform(text)
text.show(5)

idf = IDF(inputCol = count_vec.getOutputCol(),outputCol = 'features')
text = idf.fit(text).transform(text)


lda = LDA(featuresCol = idf.getOutputCol(),k=5, maxIter=10)
lda_model = lda.fit(text)

topics = lda_model.describeTopics()
# topics_words = topics.rdd.map(lambda x: x['termIndices']).map(lambda x:[vocab[i] for i in x]).collect()
get_topics_words = F.udf(lambda x: [vocab[i] for i in x],ArrayType(StringType()))
topics = topics.withColumn('topic_words',get_topics_words(F.col('termIndices')))
topics.show()

text = lda_model.transform(text)
text.show(5)

'''
schema_sdf = StructType([StructField('paragraph', StringType(), True)])
text = spark.read.options(header='false').schema(schema_sdf).csv('/user/devel/2020210990Renqiang/data/speech.txt',sep='\n')

tokenizer = RegexTokenizer(inputCol='paragraph',outputCol='words',gaps=False,pattern='[a-z|A-Z]+')
remover = StopWordsRemover(inputCol = tokenizer.getOutputCol(),outputCol = 'filtered_words')
ngramer = NGram(n = 2,inputCol = remover.getOutputCol() ,outputCol = 'ngrams')

count_vec = CountVectorizer(inputCol=ngramer.getOutputCol(),outputCol='ft_features')

idf = IDF(inputCol = count_vec.getOutputCol(),outputCol = 'features')

pipe_stages = [tokenizer,remover,ngramer,count_vec,idf]
pipe_model = Pipeline(stages=pipe_stages)
text = pipe_model.fit(text).transform(text)
text.show(5)

lda = LDA(featuresCol = idf.getOutputCol(),k=5, maxIter=10)
lda_model = lda.fit(text)

topics = lda_model.describeTopics()
count_vec_model = count_vec.fit(text)
vocab = count_vec_model.vocabulary
get_topics_words = F.udf(lambda x: [vocab[i] for i in x],ArrayType(StringType()))
topics = topics.withColumn('topic_words',get_topics_words(F.col('termIndices')))
topics.show()

text = lda_model.transform(text)
text.show(5)
'''



