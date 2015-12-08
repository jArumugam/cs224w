#!/usr/bin/env python

import matplotlib.pyplot as plt
from sklearn.decomposition import PCA

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
