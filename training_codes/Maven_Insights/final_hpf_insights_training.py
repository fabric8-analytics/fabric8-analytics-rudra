
import os 
import pickle
import numpy as np
import pandas as pd
import hpfrec
import itertools
import json
from pprint import pprint
import sparse
import scipy

with open('user_item_matrix.json', 'rb') as f:
    user_item_data = json.load(f)
    
user_item_df = pd.DataFrame(user_item_data)

with open('/home/lsuman/Downloads/maven/scoring/package_id_dict.json', 'rb') as f:
    package_id_dict = json.load(f)

with open('/home/lsuman/Downloads/maven/scoring/manifest_id_dict.json', 'rb') as f:
    manifest_id_dict = json.load(f)
    
with open('/home/lsuman/Downloads/maven/scoring/feedback_id_dict.json', 'rb') as f:
    feedback_id_dict = json.load(f)
    
package_id_dict = package_id_dict[0].get('package_list')
manifest_id_dict = manifest_id_dict[0].get('manifest_list')
feedback_id_dict = feedback_id_dict[0].get('feedback_list')
#Modified function for training and testing split
def train_test_split(data_df):
    data_df = data_df.sample(frac = 1)
    df_user = data_df.drop_duplicates(['UserId'])
    data_df = data_df.sample(frac = 1)
    df_item = data_df.drop_duplicates(['ItemId'])
#     print("User and item df: ", df_user, df_item)
    train_df = pd.concat([df_user, df_item]).drop_duplicates()
    fraction = round(frac(data_df, train_df), 2)
#     print("Train DF is: ", train_df)
#     print("fraction is: ", fraction)
    if fraction < 0.80:
        df_ = extra_df(fraction, data_df, train_df)
#         print("Extra DF is: ", df_)
        train_df = pd.concat([train_df, df_])
#         print("Train DF is : ", train_df)
    test_df = pd.concat([data_df, train_df]).drop_duplicates(keep=False)
#     print("Test DF is: ", test_df)
    return [train_df, test_df]
    
       
#Finding the unique elements from two lists
def check_unique(list1, list2):
    if set(list2).issubset(set(list1)):
        return True
    return [False, set(list2)&set(list1)]
    
#Calculating the fraction    
def frac(data_df, train_df):
    fraction = (len(train_df.index)/len(data_df.index))
    return fraction

#Calculating DataFrame according to fraction
def extra_df(frac, data_df, train_df):
    remain_frac = float("%.2f" % (0.80-frac))
    len_df = len(data_df.index)
    no_rows = round(remain_frac*len_df)
#     print("length of origignal df and number of rows are: ", len_df, no_rows)
    df_remain = pd.concat([data_df, train_df]).drop_duplicates(keep = False)
#     print("Dataframe which was remaining: ", df_remain )
    df_remain_rand = df_remain.sample(frac=1)
    return df_remain_rand[:no_rows]

#Calculating recall according to no of recommendations
def recall_at_m(m, test_df):
    recall = []
    for i in range(23114):
        x = np.array(test_df.loc[test_df.UserId.isin([i])].ItemId)
        l = len(x)
        recommendations = recommender.topN(user=i, n=m, exclude_seen=True)
        intersection_length = len(np.intersect1d(x, recommendations))
        try:
            recall.append({"recall": intersection_length/l, "length": l, "user": i})
        except ZeroDivisionError as e:
            pass
    
    recall_df = pd.DataFrame(recall, index=None)
    return recall_df['recall'].mean()

#Calculating Minimum and Maximum of list
def minmax(val_list):
    min_val = min(val_list)
    max_val = max(val_list)

    return (min_val, max_val)

#Count the frequency for each element of list
def CountFrequency(my_list): 
      
    # Creating an empty dictionary  
    freq = {} 
    for items in my_list: 
        freq[items] = my_list.count(items) 
    
    return freq

#Frequency for each recommendation of recommendation list
def freq_rec(m, users, recommender):
    total_rec = []
    for i in range(users):
        rec_user = recommender.topN(user = i,
                 n=m, exclude_seen = True)
        total_rec = np.append(total_rec, rec_user)    
    return CountFrequency(list(total_rec))
        
#Labelling of packages according to ID
def package_labelling(package_list):
    with open('/home/lsuman/Downloads/maven/scoring/package_id_dict.json', 'rb') as f:
        package_data = json.load(f)
    
    packages = package_data[0]['package_list']
    labeled_packages = dict((v,k) for k,v in packages.items())
    labeled_package_list = [labeled_packages[i] for i in package_list]
    
    return labeled_package_list

def package_to_id(input_packages):
    packages_id = [package_id_dict.get(i) for i in input_packages]
    return packages_id

def predict(input_stack, loaded_recommender):
    """Prediction function.

    :param input_stack: The user's package list
    for which companion recommendation are to be generated.
    :return companion_recommendation: The list of recommended companion packages
    along with condifence score.
    :return package_topic_dict: The topics associated with the packages
    in the input_stack+recommendation.
    :return missing_packages: The list of packages unknown to the HPF model.
    """
    input_stack = set(input_stack)
    input_id_set = set()
    missing_packages = set()
    package_topic_dict = {}
    companion_recommendation = []
    if not input_stack:
        return companion_recommendation, package_topic_dict, list(missing_packages)
    for package_name in input_stack:
        package_id = package_id_dict.get(package_name)
        if package_id:
            input_id_set.add(package_id)
            package_topic_dict[package_name] = []
        else:
            missing_packages.add(package_name)
#     print(missing_packages)
    if len(input_stack) - len(missing_packages) == 0 or             len(missing_packages) > len(input_stack) - len(missing_packages):
        print("Sorry, Lots of missing packages !")
        return [], {}, list(missing_packages)
    manifest_match = match_manifest(input_id_set)
    if manifest_match > 0:
        companion_recommendation = loaded_recommender.topN(user = manifest_match, n=5, exclude_seen = True)
        return companion_recommendation, manifest_match
    else:
        input_stack = list(package_to_id(input_stack))
        print("Input_stack is: ", input_stack)
        identity_arr = [1]*len(input_stack)
        identity_arr = list(map(int, identity_arr))
        new_df = pd.DataFrame({
                'ItemId':input_stack,
                'Count': identity_arr})
#         pprint(type(new_df['Count'][0]))
        
        print(loaded_recommender.nusers, new_df)
        is_user_added = loaded_recommender.add_user(
                        user_id = loaded_recommender.nusers, 
                        counts_df = new_df)
        
        if is_user_added:
            user_id = loaded_recommender.nusers-1
            companion_recommendation = loaded_recommender.topN(
                user=user_id,
                n=5)
            print("for new user", companion_recommendation)
            
            return companion_recommendation, user_id

        else:
            raise ValueError('Unable to add user')
            
    

def match_manifest(input_id_set):  # pragma: no cover
        """Find a manifest list that matches user's input package list and return its index.

        :param input_id_set: A set containing package ids of user's input package list.
        :return manifest_id: The index of the matched manifest.
        """
        # TODO: Go back to the difference based logic, this simpler logic will do for now.
        for manifest_id, dependency_set in manifest_id_dict.items():
            if input_id_set.issubset(dependency_set):
#                 current_app.logger.debug(
#              "input_id_set {} and manifest_id {}".format(input_id_set, manifest_id))
#                 print(int(manifest_id))
                return int(manifest_id)

        return -1

recommender = hpfrec.HPF(k=300, random_seed=123,
                  check_every=10, maxiter=400, reindex=False, verbose=True,
                  allow_inconsistent_math=True, ncores=24,
                  save_folder='/home/lsuman/red_hat_project/f8a-hpf-insights/hpf_data/')

recommender.step_size = None


training_df, testing_df = training_test_split(user_item_df)

recommender.fit(training_df)

with open('hpf_model_dummy_2.pkl', 'rb') as f:
    recommender_dummy = pickle.load(f)
 

final_list = recommender.topN(user = 31, n=5, exclude_seen = True)
print(list(final_list))

input_stack = [
    	 "javax.transaction:jta",
		 "jotm:jotm-carol",
		 "jotm:jotm-jrmp-stubs",
		 "org.springframework:spring-core",
		 "org.springframework:spring-jdbc",
		 "asm:asm"
    ]
input_stack_2 = [
    	 "com.networknt:status",
		 "io.undertow:undertow-core",
		 "com.fasterxml.jackson.core:jackson-core",
		 "io.github.lukehutch:fast-classpath-scanner",
		 "com.networknt:handler",
		 "com.networknt:swagger-meta",
		 "com.networknt:metrics",
		 "org.apache.maven.surefire:surefire-junit47",
		 "com.networknt:sanitizer",
		 "com.networknt:client",
		 "com.networknt:audit",
		 "com.networknt:body",
		 "com.networknt:config",
		 "com.fasterxml.jackson.core:jackson-databind",
		 "com.networknt:utility",
		 "com.networknt:info",
		 "com.networknt:balance",
		 "org.codehaus.plexus:plexus-compiler-javac-errorprone",
		 "com.networknt:traceability",
		 "com.google.errorprone:error_prone_core",
		 "com.networknt:consul",
		 "com.networknt:swagger-security",
		 "com.networknt:mask"
    ]
input_stack_3 = [
         "javax.transaction:jta",
         "jotm:jotm-carol",
         "jotm:jotm-jrmp-stubs",
         "org.springframework:spring-core",
         "org.springframework:spring-jdbc",
         "Asm:asm",
         "org.springframework:spring-context-support",
         "org.springframework:spring-contex",
         "javassist:javassist",
         "javax.resource:connector-api",
         "commons-dbcp:commons-dbcp",
         "org.springframework:spring-tx",
         "javax.persistence:persistence-api",
         "cglib:cglib-nodep",
         "org.springframework:spring-aop",
         "asm:asm-commons"
]

input_stack_4 = ['com.facebook.presto:presto-server',
                 'io.airlift:jaxrs',
                 'org.apache.commons:commons-text']

recommends = predict(input_stack, recommender)
print(package_labelling(recommends[0]))

def filter_recommendation(result, input_stack):
        """Filter based on feedback, input set & max recommendation count.

        :param result: The unfiltered companion recommendation result.
        :return companion_recommendation: The filtered list of recommended companion packages
        along with condifence score.
        """
        companion_recomendations = []
        input_stack = list(input_stack)
        result = np.delete(result, input_stack)
        result = np.where(np.isfinite(result))[0]
        def _sigmoid(x):
            return 1 / (1 + np.exp(-x))
        recommended_ids = result.argsort()[::-1][:5]
        print("recommended_ids are: ", recommended_ids)
        mean = np.mean(result[recommended_ids])
        result = result - mean
        result = np.divide(result, np.std(result))
        for package_id in recommended_ids:
            print(result[package_id])
            recommendation = {
                "cooccurrence_probability": round(_sigmoid(result[package_id]) * 100, 2),
                "package_name": package_labelling([package_id]),
                "topic_list": []  # We don't have topics for this ecosystem!!
            }
            # At least thirty percent probability is required for recommendation to go through.
            if recommendation['cooccurrence_probability'] > 30:
                companion_recomendations.append(recommendation)
        return companion_recomendations

def _sigmoid(x):
        return 1 / (1 + np.exp(-x))
    
def filter_recommendations_dummy(result, input_stack, user_id):
        package_id_set = input_stack
        recommendations = result
        companion_packages = []
        recommendations = np.array(list(itertools.compress(recommendations,
                                        [i not in package_id_set for i in recommendations])))

#         print("Filtered recommendation ids are: " + str(recommendations))

        poisson_values = loaded_recommender.predict(
            user=[user_id] * recommendations.size,
            item=recommendations
        )

        # This is copy pasted on as is basis from maven and NPM model.
        # It's not the right way of calculating probability by any means.
        # There is a more lengthier way to calculate probabilities using
        # logistic regression which remains to be implemented
        # (but that's also not completely correct).
        # For discussion please follow: https://github.com/david-cortes/hpfrec/issues/4
        normalized_poisson_values = _sigmoid(
            (poisson_values - poisson_values.mean()) / poisson_values.std()) * 100
#         print("Before is: ", recommendations)
        filtered_packages = package_labelling(recommendations)
#         print("After is: ", filtered_packages)
#         print("Poisson values are: ", normalized_poisson_values)
        for idx, package in enumerate(filtered_packages):
            if normalized_poisson_values[idx] >= 28:
                companion_packages.append({
                    "package_name": package,
                    "cooccurrence_probability": str(normalized_poisson_values[idx]),
                    "topic_list": []
                })

        return companion_packages
    
recommendations, manifst_match= predict(input_stack)
print("Input stack is : ",input_stack)
# print(recommendations, package_to_id(input_stack))
pprint(filter_recommendations_dummy(recommendations, package_to_id(input_stack), manifst_match))

print(package_labelling([106, 116, 102, 318, 180]))

package_labelling(predict(input_stack)[0])
manifest_id_dict.get['0']
package_id_dict.get("org.apache.commons:commons-lang3")
    
