import pandas as pd
from pathlib import Path
import numpy as np
from numpy.fft import fftshift, rfft, irfft
from numpy import pi
from scipy.special import i0

mods = ['Z3F',
        '949paintball',
        'zhige',
        'atomicimploder',
        'ekjp',
        'TheNitromeFan',
        'davidjl123',
        'rschaosid',
        'KingCaspianX',
        'Urbul',
        'Zaajdaeon']


def combine_csvs(start, n):
    results = [''] * n
    for i in range(n):
        hog_path = Path(f"results/LOG_{start}to{start+1000}.csv")
        df = pd.read_csv(hog_path,
                         names=['count', 'username', 'timestamp', 'comment_id', 'submission_id'])
        if len(df) % 1000 != 0:
            print(hog_path)
        df = df.set_index('comment_id')
        results[i] = df
        start += 1000
    return pd.concat(results)


def is_mod(username):
    return username in mods


def response_graph(df, n=250):
    user_counts = df.groupby('username')['timestamp'].count()
    top = user_counts.sort_values().tail(n)
    edges = df.username.isin(top.index) & df.username.shift(1).isin(top.index)
    df['replying_to'] = df.username.shift(1)
    graph = df.loc[edges].groupby(['username', 'replying_to'], as_index=False)['timestamp'].count()
    graph.columns = ["Source", "Target", "Weight"]
    return graph


def effective_number_of_counters(counters):
    normalised_counters = counters / counters.sum()
    return 1 / (normalised_counters ** 2).sum()


def capture_the_flag_score(submission):
    submission['score'] = submission['timestamp'].diff().shift(-1)
    return submission.groupby('username')['score'].sum()


def k_core(graph):
    old_score = 0
    new_score = min([node.degree() for node in graph])
    while new_score > old_score:
        graph = graph.delete_least_connected_nodes()
        new_score = min([node.degree() for node in graph])
    return graph


def vonmises_distribution(x, mu=0, kappa=1):
    return np.exp(kappa * np.cos(x - mu)) / (2. * np.pi * i0(kappa))


def normal_distribution(x, mu=0, sigma=1):
    return np.exp(-0.5 * ((x - mu) / sigma) ** 2) / (sigma * np.sqrt(np.pi * 2))


def fft_kde_on_unit_circle(data, n_bins, kernel=vonmises_distribution, **params):
    """The fft approximation to the kde of data on the unit circle

    params:
    data: array-like: The data points to be approximated. Assumed to be distributed on [-pi, pi]
    n_bins: number of bins to use for approximation
    kernel: The kernel used to approximate the data. Default is the gaussian on the circle
    **params: parameters apart from x needed to define the kernel.

    returns:
    x: array-like: points on the unit circle at which to evaluate the density
    kde: Estimated density, normalised so that ∫kde dx = 1.

    In genergal, kernel density estimation means replacing each discrete point
    with some normalised distribution centered at that point, and then adding
    up all these results. Mathematically, this corresponds to convolving the
    original data with the chosen distribution. In fourier space, a convolution
    transforms to a product, so for periodic data we can get a fast
    approximation to this by finding the Fourier transform of our data, the
    Fourier transform of our kernel, multiplying them together and finding the
    inverse transform.

    """

    x_axis = np.linspace(-pi, pi, n_bins + 1, endpoint=True)
    hist, edges = np.histogram(data, bins=x_axis)
    x = np.mean([edges[1:], edges[:-1]], axis=0)
    kernel = kernel(x=x, **params)
    kde = fftshift(irfft(rfft(kernel) * rfft(hist)))
    kde /= np.trapz(kde, x=x)
    return x, kde


def fft_kde(data, n_bins, kernel=vonmises_distribution, **params):
    minval = 0
    maxval = 24 * 3600
    if kernel == 'vonmises_distribution':
        kernel = vonmises_distribution
    elif kernel == 'normal_distribution':
        kernel = normal_distribution
    data_on_unit_circle = (data - minval) / (maxval - minval) * 2 * pi - pi
    x, kde = fft_kde_on_unit_circle(data_on_unit_circle, n_bins, kernel, **params)
    x = (x + pi) / (2 * pi) * (maxval - minval) + minval
    kde /= np.trapz(kde, x=x)
    return x, kde
