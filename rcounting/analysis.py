import pandas as pd
from pathlib import Path
import numpy as np
from numpy.fft import fftshift, rfft, irfft
from numpy import pi
from scipy.special import i0

import rcounting as rct


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


def hoc_string(df, title):
    getter = rct.counters.apply_alias(df.iloc[-1]['username'])

    def hoc_format(username):
        username = rct.counters.apply_alias(username)
        return f'**/u/{username}**' if username == getter else f'/u/{username}'

    df['hoc_username'] = df['username'].apply(hoc_format)
    dt = pd.to_timedelta(df.iloc[-1].timestamp - df.iloc[0].timestamp, unit='s')
    table = df.iloc[1:]['hoc_username'].value_counts().to_frame().reset_index()
    data = table.set_index(table.index + 1).to_csv(None, sep='|', header=0)

    header = (f'Thread Participation Chart for {title}\n\nRank|Username|Counts\n---|---|---')
    footer = (f'It took {len(table)} counters {rct.utils.format_timedelta(dt)} '
              'to complete this thread. Bold is the user with the get\n'
              f'total counts in this chain logged: {len(df) - 1}')
    return '\n'.join([header, data, footer])


def response_graph(df, n=250, username_column="username"):
    indices = df.groupby(username_column).size().sort_values(ascending=False).index[:n]
    edges = (df[username_column].isin(indices)
             & df[username_column].shift(1).isin(indices))
    top = pd.concat([df[username_column],
                     df[username_column].shift()], axis=1).loc[edges]
    top.columns = ['username', 'replying_to']
    graph = top.groupby(['username', 'replying_to'], as_index=False).size()
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
