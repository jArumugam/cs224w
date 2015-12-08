from __future__ import division
import matplotlib.pyplot as plt
import numpy as np
from sklearn.decomposition import PCA
from sklearn.linear_model import LogisticRegression


def plot_pca(samples, labels):
    pca = PCA(n_components=2)
    points = pca.fit_transform(samples)

    positive_x = [p for p, l in zip(points[:,0], labels) if l == 1]
    positive_y = [p for p, l in zip(points[:,1], labels) if l == 1]
    negative_x = [p for p, l in zip(points[:,0], labels) if l == 0]
    negative_y = [p for p, l in zip(points[:,1], labels) if l == 0]

    negative = plt.scatter(negative_x, negative_y, color='blue', alpha=.5)
    positive = plt.scatter(positive_x, positive_y, color='red', alpha=.5)
    
    legend_points = (negative, positive)
    legend_labels = ('Non-expert', 'Expert')
    plt.legend(legend_points, legend_labels)

    plt.show()


def logistic_test(train_data, train_labels, test_data, test_labels, cv=False):
    # Perform logistic regression.
    clf = LogisticRegressionCV if cv else LogisticRegression()
    clf.fit(train_data, train_labels)
    predicted_labels = clf.predict(test_data)

    # Count true positives, true negatives, false positives, false negatives.
    tp, tn, fp, fn = 0, 0, 0, 0
    for predicted, actual in zip(predicted_labels, test_labels):
        if predicted == 1 and actual == 1:
            tp += 1
        if predicted == 0 and actual == 0:
            tn += 1
        if predicted == 1 and actual == 0:
            fp += 1
        if predicted == 0 and actual == 1:
            fn += 1

    # Compute statistics. 
    accuracy =  (tp + tn) / (tp + tn + fp +fn)
    precision = 0 if (tp + fp) == 0 else tp / (tp + fp)
    recall = 0 if (tp + fn) == 0 else tp / (tp + fn)

    # Print report.
    print "Correctly classified {}/{}".format(tp + tn, tp + tn + fp +fn)
    print "Accuracy:", accuracy
    print "Precision:", precision
    print "Recall:", recall
    print "tp: {}; tn: {}; fp: {}; fn {}".format(tp, tn, fp, fn)

    return accuracy
