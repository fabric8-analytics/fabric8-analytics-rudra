
import hpfrec
import pickle

import pandas as pd
import numpy as np

"""Fitting the model on training data, instead of validation set."""
recommender = hpfrec.HPF(k=40, reindex=False, stop_crit='train-llk', verbose=True, stop_thr=0.000001, maxiter=3000, random_seed=123,
                        save_folder='~/Documents/RedHat')

with open('training-data.pkl', 'rb') as f:
    train_df = pickle.load(f)

with open('test-data.pkl', 'rb') as f:
    test_df = pickle.load(f)

recommender.fit(train_df)

def recall_at_m(m):
    recall = []
    for i in range(69134):
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

recall_at_m(50)

recall_at_m(100)

recall_at_m(200)

recall_at_m(300)

# In[9]:


"""Save the model"""
recommender.step_size = None
with open('HPF_model.pkl', 'wb') as f:
    pickle.dump(recommender, f)

with open('HPF_model.pkl', 'rb') as f:
    recommender = pickle.load(f)


# #### We will do some common sense checks here to see if we trained the model correctly. To do that, predictions should:
# 
# 1. Be higher for this non-zero hold-out sample than for random items.
# 2. Produce a good discrimination between random items and those in the hold-out sample (very related to the first point).
# 3. Be correlated with the playcounts in the hold-out sample.
# 4. Follow an exponential distribution rather than a normal or some other symmetric distribution.

test_df['Predicted'] = recommender.predict(user=test_df.UserId, item=test_df.ItemId)
test_df['RandomItem'] = np.random.choice(train_df.ItemId, size=test_df.shape[0])
test_df['PredictedRandom'] = recommender.predict(user=test_df.UserId, item=test_df.RandomItem)
print("Average prediction for combinations in test set: ", test_df.Predicted.mean())
print("Average prediction for random combinations: ", test_df.PredictedRandom.mean())

from sklearn.metrics import roc_auc_score

was_played = np.r_[np.ones(test_df.shape[0]), np.zeros(test_df.shape[0])]
score_model = np.r_[test_df.Predicted.values, test_df.PredictedRandom.values]
roc_auc_score(was_played, score_model)
np.corrcoef(test_df.Count, test_df.Predicted)[0,1]
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')

_ = plt.hist(test_df.Predicted, bins=80)
plt.xlim(0,5)
plt.show()

