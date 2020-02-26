import json
import pandas as pd
from pandas.io.json import json_normalize
from pyspark.sql.types import StructField, DataType


sample_json = '''{"distinguished":null,"downs":0,"created_utc":"1309478400","controversiality":0,"edited":false,"gilded":0,"author_flair_css_class":"mordekaiser","id":"c22x4aq","author":"adomorn","retrieved_on":1427302516,"score_hidden":false,"subreddit_id":"t5_2rfxx","score":1,"name":"t1_c22x4aq","author_flair_text":"[adomorn] (NA)","link_id":"t3_id1nc","archived":true,"ups":1,"parent_id":"t3_id1nc","subreddit":"leagueoflegends","body":"Good lord.  Yes."}'''

json_comment = json.loads(sample_json)
print('First Line As Dictionary ', json_comment)

json_pretty = json.dumps(json_comment, indent=4)
print('Pretty JSON', json_pretty)

json_keys = []
schema_list = []
for key in json_comment:
    json_keys.append(key)
    # schema_list.append(StructField(key, DataType(json_comment[key]), True))

print(json_keys)
# print(schema_list)
# df = pd.DataFrame(json_comment)
# print(df.)
print(json_normalize(json_comment))