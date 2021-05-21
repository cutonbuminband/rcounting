import pandas as pd
from pathlib import Path
import numpy as np
from numpy.fft import fftshift, rfft, irfft
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
                         names=['count', 'username', 'timestamp', 'comment_id', 'thread_id'])
        if len(df) % 1000 != 0:
            print(hog_path)
        df = df.set_index('comment_id')
        results[i] = df
        start += 1000
    return pd.concat(results)


def is_mod(username):
    return username in mods


def capture_the_flag_score(thread):
    thread['score'] = thread['timestamp'].diff().shift(-1)
    return thread.groupby('username')['score'].sum()


def vonmises_distribution(x, mu, kappa):
    return np.exp(kappa * np.cos(x - mu)) / (2. * np.pi * i0(kappa))


def fft_kde(data, n_bins, kernel=vonmises_distribution, **params):
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

    x_axis = np.linspace(-np.pi, np.pi, n_bins + 1, endpoint=True)
    hist, edges = np.histogram(data, bins=x_axis)
    x = np.mean([edges[1:], edges[:-1]], axis=0)
    kernel = kernel(x=x, **params)
    kde = fftshift(irfft(rfft(kernel) * rfft(hist)))
    kde /= np.trapz(kde, x=x)
    return x, kde
