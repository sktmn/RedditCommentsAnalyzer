from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import json
from timeit import default_timer as timer
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import CommentsFileFetcher
from CommentsSchema import *

start = timer()
conf = SparkConf()
conf.setAppName('UserActivityAnalyser')
conf.setMaster('local[5]')
conf.set('spark.executor.memory', '5g')
conf.set('spark.executor.cores', '6')
conf.set('spark.cores.max', '6')
conf.set('spark.driver.memory', '5g')
conf.set('spark.logConf', False)

sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')

timer_sc = timer()
print('Spark Context created in', (timer_sc - start))

comment_data_files = CommentsFileFetcher.getCommentsData(2009, 12)

comments_data = sc.textFile(comment_data_files[0])

timer_start = timer()
sqlContext = SQLContext(sc)
df = sqlContext.read.json(comments_data)

df_user_subreddit_ups = df.select(Schema.Author, Schema.Ups, Schema.Subreddit)

df_user_subreddit_ups.registerTempTable('user_subreddit_ups')

comment_count_by_subr_sql = ' select ' + Schema.Subreddit + ' as ' + Schema.Subreddit + ', count(*) as CommentCount ' \
                ' from user_subreddit_ups where author != \'[deleted]\' ' \
                ' group by ' + Schema.Subreddit + ' order by CommentCount desc '

comment_count_by_subr = sqlContext.sql(comment_count_by_subr_sql)
timer_subRedditComments = timer()

df_comment_count_by_subr = comment_count_by_subr.toPandas()
print('Time to query user comments by subreddit', (timer_subRedditComments - timer_start))

sc.stop()
