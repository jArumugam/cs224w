#!/usr/bin/env python
"""
Extracts graph features for PCA and logistic regression.
For each user, we we generate a training example with:

label - expert or nonexpert
[0] Authority score at 10th percentile of active lifetime
[1] Authority score at 20th percentile of active lifetime
[2] Authority score at 30th percentile of active lifetime
[3] Authority score at 40th percentile of active lifetime
[4] Authority score at 50th percentile of active lifetime
[5] PageRank score at 10th percentile of active lifetime
[6] PageRank score at 20th percentile of active lifetime
[7] PageRank score at 30th percentile of active lifetime
[8] PageRank score at 40th percentile of active lifetime
[9] PageRank score at 50th percentile of active lifetime
[10] ELO score at 10th percentile of active lifetime
[11] ELO score at 20th percentile of active lifetime
[12] ELO score at 30th percentile of active lifetime
[13] ELO score at 40th percentile of active lifetime
[14] ELO score at 50th percentile of active lifetime
[15] CAU score at 10th percentile of active lifetime
[16] CAU score at 20th percentile of active lifetime
[17] CAU score at 30th percentile of active lifetime
[18] CAU score at 40th percentile of active lifetime
[19] CAU score at 50th percentile of active lifetime

Our goal is to use these evolution of their network
properties at early stages to predict where they will
be at the end of the dataset (expert or nonexpert).

Because it is difficult to make a relation between
experts as identified by StackExchange reputation
and experts as vetted by people in real life, for the
purpose of this project we will assume that reputation
is ground truth for expertise. We define experts to be
the top 10 percentile of users by StackExchange reputation.
"""
import sys
import metrics
import search_utilities
import ml

feature_percentiles = [.1, .2, .3, .4, .5]

def cau_scores(cur, conn, user_id):
    """
    Returns a user CAU scores from 10th to 50th percentile.
    """
    return metrics.cau_for_user(cur, conn, user_id, samples = feature_percentiles)

def elo_scores(cur, conn, user_id):
    """
    Returns a user ELO scores from 10th to 50th percentile.
    """
    return metrics.elo_for_user(cur, conn, user_id, samples = feature_percentiles)

def pagerank_scores(cur, user_id):
    """
    Returns a user PageRank scores from 10th to 50th percentile.
    """
    return metrics.pagerank_for_user(cur, user_id, samples = feature_percentiles)

def auth_scores(cur, user_id):
    """
    Returns a user Auth scores from 10th to 50th percentile.
    """
    return metrics.auth_for_user(cur, user_id, samples = feature_percentiles)

def is_expert(user_id):
    """
    Returns true if the user is an expert.
    """
    return user_id in search_utilities.get_experts()

def training_examples(cur, conn, user_ids):
    """
    Returns a set of feature vectors and labels extracted
    from the dataset for the given user_ids.
    """
    fv = []
    labels = []
    for user_id in user_ids:
        uv = []
        uv += auth_scores(cur, user_id)
        uv += pagerank_scores(cur, user_id)
        uv += elo_scores(cur, conn, user_id)
        uv += cau_scores(cur, conn, user_id)
        fv.append(uv)
        labels.append(1 if is_expert(user_id) else 0)
    return fv, labels

def main(args):
    conn, cur = metrics.connect("Ben-han", "Ben-han")
    user_ids = search_utilities.get_experts() + search_utilities.get_nonexperts()
    data, labels = training_examples(cur, conn, user_ids)
    ml.plot_pca(data, labels)

if __name__ == '__main__':
    main(sys.argv)