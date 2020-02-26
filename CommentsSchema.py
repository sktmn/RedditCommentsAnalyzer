from pyspark.sql.types import StructType, StructField, BooleanType, StringType, LongType


class Schema:
    Archived = 'archived'
    Author = 'author'
    Author_flair_css_class = 'author_flair_css_class'
    Author_flair_text = 'author_flair_text'
    Body = 'body'
    Controversiality = 'controversiality'
    Created_utc = 'created_utc'
    Distinguished = 'distinguished'
    Downs = 'downs'
    Edited = 'edited'
    Gilded = 'gilded'
    Id = 'id'
    Link_id = 'link_id'
    Name = 'name'
    Parent_id = 'parent_id'
    Retrieved_on = 'retrieved_on'
    Score = 'score'
    Score_hidden = 'score_hidden'
    Subreddit = 'subreddit'
    Subreddit_id = 'subreddit_id'
    Ups = 'ups'

    @staticmethod
    def get():
        fields = [StructField('archived', BooleanType(), True),
                  StructField('author', StringType(), True),
                  StructField('author_flair_css_class', StringType(), True),
                  StructField('body', StringType(), True),
                  StructField('controversiality', LongType(), True),
                  StructField('created_utc', StringType(), True),
                  StructField('distinguished', StringType(), True),
                  StructField('downs', LongType(), True),
                  StructField('edited', StringType(), True),
                  StructField('gilded', LongType(), True),
                  StructField('id', StringType(), True),
                  StructField('link_id', StringType(), True),
                  StructField('name', StringType(), True),
                  StructField('parent_id', StringType(), True),
                  StructField('retrieved_on', LongType(), True),
                  StructField('score', LongType(), True),
                  StructField('score_hidden', BooleanType(), True),
                  StructField('subreddit', StringType(), True),
                  StructField('subreddit_id', StringType(), True),
                  StructField('ups', LongType(), True)]

        return StructType(fields)

# def schema():
#     fields = [StructField('archived', BooleanType(), True),
#               StructField('author', StringType(), True),
#               StructField('author_flair_css_class', StringType(), True),
#               StructField('body', StringType(), True),
#               StructField('controversiality', LongType(), True),
#               StructField('created_utc', StringType(), True),
#               StructField('distinguished', StringType(), True),
#               StructField('downs', LongType(), True),
#               StructField('edited', StringType(), True),
#               StructField('gilded', LongType(), True),
#               StructField('id', StringType(), True),
#               StructField('link_id', StringType(), True),
#               StructField('name', StringType(), True),
#               StructField('parent_id', StringType(), True),
#               StructField('retrieved_on', LongType(), True),
#               StructField('score', LongType(), True),
#               StructField('score_hidden', BooleanType(), True),
#               StructField('subreddit', StringType(), True),
#               StructField('subreddit_id', StringType(), True),
#               StructField('ups', LongType(), True)]
#
#     return StructType(fields)
