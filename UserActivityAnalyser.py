import json
from timeit import default_timer as timer

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc


from CommentsSchema import *


def to_csv(data):
    return ','.join(str(d) for d in data)


timer_start = timer()
conf = SparkConf()
conf.setAppName('UserActivityAnalyser-gz-Input')
conf.setMaster('spark://sravyas-mbp.lan:7077')

# conf.set("spark.shuffle.service.enabled", "false")  # these are defaults so don't need to be set
# conf.set("spark.dynamicAllocation.enabled", "false")  # these are defaults so don't need to be set

# conf.set('spark.io.compression.codec', 'lz4')
# conf.set('spark.rdd.compress', 'true')

# conf.set('spark.memory.offHeap.enabled', 'true')
# conf.set('spark.memory.offHeap.size', '1g')

conf.set('spark.executor.memory', '4g')
conf.set('spark.worker.cores', '4')
conf.set('spark.cores.max', '4')

# conf.set('spark.rdd.compress', 'true')
# conf.set('spark.executor.cores', '6')
# conf.set('spark.driver.memory', '5g')
# conf.set('spark.logConf', False)

sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')  # To disable console logging

timer_sc = timer()
print('Spark Context created in', (timer_sc - timer_start))

# comment_data_files = CommentsFileFetcher.getCommentsData(2009, 12)
# comments_data = sc.textFile(comment_data_files[0])

comments_data = sc.textFile('/Users/sravyakatamneni/Spark/CommentsData/RC_2009-12.gz')

first_line = comments_data.first()
timer_firstLine = timer()
print('First line of comments loaded time', (timer_firstLine - timer_sc))

# comment_json = json.loads(first_line)

# comment_json_keys = []
#
# for item in comment_json.keys():
#     comment_json_keys.append(item)
#
# print('First line >> ', comment_json)
#
# comment_json_keys.sort()
#
# print('All keys in schema >> ', comment_json_keys)

timer_sqlContext = timer()
sqlContext = SQLContext(sc)

# schema = Schema.get()

df = sqlContext.read.json(comments_data)

df_user_subreddit_ups = df.select(Schema.Author, Schema.Ups, Schema.Subreddit)

# Actions-1
print('Total size = ', df_user_subreddit_ups.count(), '. Loaded in time', timer() - timer_sqlContext)

# Transformations
timer_gdf = timer()
user_subreddit_ups_gdf = df_user_subreddit_ups.filter('author != \'[deleted]\'')\
                            .groupBy(Schema.Subreddit).count().sort(desc('count'))
print('Grouped by subreddit, take 5 ', user_subreddit_ups_gdf.take(5), 'in time', timer() - timer_gdf)

# Spark SQL transformations
timer_temp_table = timer()
df_user_subreddit_ups.registerTempTable('user_subreddit_ups')
print('Time to register temp table', timer() - timer_temp_table)

timer_subr_comments = timer()
comment_count_by_subr_sql = ' select ' + Schema.Subreddit + ' as ' + Schema.Subreddit + ', count(*) as CommentCount ' \
                            ' from user_subreddit_ups where author != \'[deleted]\' ' \
                            ' group by ' + Schema.Subreddit + ' order by CommentCount desc '
comment_count_by_subr = sqlContext.sql(comment_count_by_subr_sql)
print('comment_count_by_subr take 5 >>', comment_count_by_subr.take(5))
print('Time to query user comments by subreddit', (timer() - timer_subr_comments))

# Spark SQL transformations - Example 2

timer_subr_comments_csv = timer()
comment_count_by_subr.coalesce(1).write.csv(path='comments_by_subr.csv', header='true', mode='overwrite', sep=',')
print('Time to write comments by subreddit as csv', (timer() - timer_subr_comments_csv))


timer_user_ups = timer()


ups_by_user_sql = ' select ' + Schema.Author + ' as ' + Schema.Author + ', sum(' + Schema.Ups + ') as totalUps ' \
                  ' from user_subreddit_ups where ' + Schema.Author + ' != \'[deleted]\' ' \
                  ' group by ' + Schema.Author + ' order by totalUps desc '

ups_by_user = sqlContext.sql(ups_by_user_sql)

print(ups_by_user.columns)
print('Time to get columns', (timer() - timer_user_ups))
print('ups_by_user take 5 >>', ups_by_user.take(5), 'in time', timer() - timer_user_ups)
timer_userUpVotes_csv = timer()

# ups_by_user.toPandas().to_csv('ups_by_user.csv')  # Works by converting to Pandas DF
# as_csv_lines = ups_by_user.coalesce(1).rdd.map(to_csv)  # Works by applying map on rdd
# as_csv_lines.saveAsTextFile('ups_by_user.csv')  # Creates many parts
# ups_by_user.coalesce(1).write.format('com.databricks.spark.csv')\
#     .mode('overwrite').options(header='true').save('ups_by_user.csv')
ups_by_user.coalesce(1).write.csv(path='ups_by_user.csv', header='true', mode='overwrite', sep=',')
print('Time to write user upvotes as csv', (timer() - timer_userUpVotes_csv))

print('Total time taken >> ', timer() - timer_start)
sc.stop()
