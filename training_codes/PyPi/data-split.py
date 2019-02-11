
import json   
import pandas

with open('manifest-list-trimmed-unique.json', 'r') as f:
    a = 0
    content = json.load(f)
    x = dict()
    for package_list in content[0].get('package_list'):
        for package in package_list:
            if package:
                if package not in x:
                    x[package] = a
                    a = a+1

with open('package-to-id-dict-normalized.json', 'w') as w:
    json.dump(x, w)

id_to_package_dict = {v: k for k, v in x.items()}

with open('id-to-package-dict.json', 'w') as w:
    json.dump(id_to_package_dict, w)

"""Create an id to manifest mapping. Maps each unique manifest to a unique id."""
with open('manifest-list-trimmed-unique.json', 'r') as f:
    content = json.load(f)
    a = 0
    id_to_manifest_dict = dict()
    for manifest in content[0].get('package_list'):
        id_to_manifest_dict[a] = manifest
        a += 1

with open('id-to-manifest-dict.json', 'w') as w:
    json.dump(id_to_manifest_dict, w)

manifest_to_id_dict = {frozenset(v): k for k,v in id_to_manifest_dict.items()}

"""Create a manifest to id mapping. Maps each unique manifest to a unique id."""

import pickle

with open('manifest-to-id.pickle', 'wb') as w:
    pickle.dump(manifest_to_id_dict, w, protocol=pickle.HIGHEST_PROTOCOL)

with open('id-to-manifest-dict.json', 'r') as f:
    content = json.load(f)
    df = pandas.DataFrame.from_dict(content, orient='index')

del(content)

data_mapping_list = []

with open('id-to-manifest-dict.json', 'r') as m, open('package-to-id-dict-normalized.json', 'r') as p:
    content_man = json.load(m)
    content_pack = json.load(p)
    for k,v in content_man.items():
        userId = int(k)
        for package in v:
            itemId = content_pack[package]
            count = 1
            data_mapping_list.append(
                {
                    "UserId": userId,
                    "ItemId": itemId,
                    "Count": 1
                }
            )

with open('user-item-matrix.json', 'w') as f:
    json.dump(data_mapping_list, f)

with open('user-item-matrix.json', 'r') as f:
    df = pandas.read_json(f, orient='records')

import numpy as np

df.head()

df_user_id = df.groupby("UserId")

df_user_id.head()

def train_test_validate_split(df):
    return np.split(df.sample(frac=1), [int(.6*len(df)), int(.8*len(df))])


def train_test_split(df):
    return np.split(df.sample(frac=1), [int(.8*len(df))])

len(df_user_id)

"""This is slow somehow. Need to optimize it."""
dataframe = df_user_id.apply(train_test_split)

dataframe[5]

list_df_train = list()
list_df_test = list()
list_df_validate = list()


for s in dataframe:
    if s[0].empty:
        list_df_train.append(s[1])
    else:
        list_df_train.append(s[0])
        if not s[1].empty:
            list_df_test.append(s[1])

x = pandas.merge(training_data, test_data, how='inner', on=['Count', 'ItemId', 'UserId'])
assert x.empty
# x = pandas.merge(training_data, validate_data, how='inner', on=['Count', 'ItemId', 'UserId'])
# assert x.empty
# x = pandas.merge(test_data, validate_data, how='inner', on=['Count', 'ItemId', 'UserId'])
# assert x.empty

training_data.to_pickle('./training-data.pkl')

test_data.to_pickle('./test-data.pkl')
