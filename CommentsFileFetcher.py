import os
import fnmatch
from CommentsSchema import *

parent_dir = '/Users/sravyakatamneni/Spark/CommentsData/'


def getCommentsData(year: int, month: int = 0) -> list:
    result_files = []
    if not isinstance(year, int) or not isinstance(month, int):
        raise TypeError

    if month <= 0:
        comments_file_pattern = 'RC_' + str(year) + '-*.bz2'
    elif month < 10:
        comments_file_pattern = 'RC_' + str(year) + '-0' + str(month) + '.bz2'
    else:
        comments_file_pattern = 'RC_' + str(year) + '-' + str(month) + '.bz2'

    for root, dirs, files in os.walk(parent_dir, topdown=False):
        for file in fnmatch.filter(files, comments_file_pattern):
            result_files.append(os.path.join(parent_dir, file))

    result_files.sort()
    return result_files

# results = getCommentsData(2009)
# print(results)
#
# schema = Schema.get()


# ups_by_user_sql = ' select ' + Schema.Author + ', sum(' + Schema.Ups + ') as totalUps from user_ups ' \
#                 ' where ' + Schema.Author + ' != \'[deleted]\' ' \
#                 ' group by ' + Schema.Author + ' order by totalUps desc '
#
# comment_count_by_subr_sql = ' select ' + Schema.Subreddit + ', count(*) as CommentCount from user_subreddit_ups ' \
#                 ' where author != \'[deleted]\' group by ' + Schema.Subreddit + ' order by CommentCount desc '
#
# print(ups_by_user_sql)
# print(comment_count_by_subr_sql)
