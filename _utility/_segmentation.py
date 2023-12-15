import numpy as np
from matplotlib import pyplot as plt
from sklearn.cluster import SpectralCoclustering, affinity_propagation

def corr_from_cov(covariance):
    v = np.sqrt(np.diag(covariance))
    outer_v = np.outer(v, v)
    correlation = covariance / outer_v
    correlation[covariance == 0] = 0
    return correlation

def _SpectralCoclustering(data, n_clusters, title, cols_nm):
    model = SpectralCoclustering(n_clusters=n_clusters, random_state=0)
    model.fit(data)

    _rank = np.argsort(model.row_labels_)
    _cols= cols_nm[_rank]
    fit_data = data[_rank]
    fit_data = fit_data[:, np.argsort(model.column_labels_)]

    n = len(_cols)
    fig = plt.figure(figsize=(9, 9))
    if n > 30:
        fig = plt.figure(figsize=(16, 16))
    ax = fig.add_subplot(111)
    ax.matshow(fit_data, interpolation='none', cmap=plt.cm.Blues)
    ax.set_yticks(np.arange(n))
    ax.set_yticklabels(_cols)
    plt.title(title)
    plt.tight_layout()
    plt.show()
