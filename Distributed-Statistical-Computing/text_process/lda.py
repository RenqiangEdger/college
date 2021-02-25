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
from pyspark.ml.feature import HashingTF,IDF,RegexTokenizer,Tokenizer,Word2Vec,StopWordsRemover,NGram
from pyspark.ml.clustering import LDA



spark = SparkSession.builder.appName("text-model").master('yarn').config('spark.port.maxRetries','32').getOrCreate()

schema_sdf = StructType([StructField('paragraph', StringType(), True)])
text = spark.read.options(header='false').schema(schema_sdf).csv('/user/devel/2020210990Renqiang/data/speech.txt',sep='\n')


tokenizer = RegexTokenizer(inputCol='paragraph',outputCol='words',gaps=False,pattern='[a-z|A-Z]+')
text = tokenizer.transform(text)
text.show(5)
#tokenizer = Tokenizer(inputCol='paragraph',outputCol='words')
#text = tokenizer.transform(text)
#text.text(5)

remover = StopWordsRemover(inputCol = 'words',outputCol = 'filtered_words')
text = remover.transform(text)
text.show(5)

ngramer = NGram(n = 2,inputCol = 'filtered_words',outputCol = 'ngrams')
text = ngramer.transform(text)
text.show(5)


words_len = text.select(ngramer.getOutputCol()).rdd.map(lambda x:len(x[0])).collect()
word2vec = Word2Vec(vectorSize = np.quantile(words_len,0.5),minCount=0,inputCol='ngrams',outputCol='word_vec')
text = word2vec.fit(text).transform(text)
text.show(5)


lda = LDA(featuresCol = word2vec.getOutputCol(),k=5, maxIter=10)
lda_model = lda.fit(text)

topics = lda_model.describeTopics(5)
topics.show(5)

text = lda_model.transform(text)
text.show(5)

'''
schema_sdf = StructType([StructField('paragraph', StringType(), True)])
text = spark.read.options(header='false').schema(schema_sdf).csv('/user/devel/2020210990Renqiang/data/speech.txt',sep='\n')

tokenizer = RegexTokenizer(inputCol='paragraph',outputCol='words',gaps=False,pattern='[a-z|A-Z]+')
remover = StopWordsRemover(inputCol = tokenizer.getOutputCol(),outputCol = 'filtered_words')
ngramer = NGram(n = 2,inputCol = remover.getOutputCol() ,outputCol = 'ngrams')
word2vec = Word2Vec(vectorSize = 20 ,minCount=0,inputCol=ngramer.getOutputCol(),outputCol='word_vec')

pipe_stages = [tokenizer,remover,ngramer,word2vec]
pipe_model = Pipeline(stages=pipe_stages)
text = pipe_model.fit(text).transform(text)
text.show(5)

lda = LDA(featuresCol = word2vec.getOutputCol(),k=5, maxIter=10)
lda_model = lda.fit(text)

topics = lda_model.describeTopics(5)
topics.show(5)

text = lda_model.transform(text)
text.show(5)
'''



