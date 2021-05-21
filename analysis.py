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


def vonmises_fft_kde(data, kappa, n_bins):
    """ Return the kde of data using the von mises distribution on the circle
    """

    bins = np.linspace(-np.pi, np.pi, n_bins + 1, endpoint=True)
    hist_n, bin_edges = np.histogram(data, bins=bins)
    bin_centers = np.mean([bin_edges[1:], bin_edges[:-1]], axis=0)
    kernel = vonmises_distribution(x=bin_centers,
                                   mu=0,
                                   kappa=kappa)
    kde = fftshift(irfft(rfft(kernel) * rfft(hist_n)))
    kde /= np.trapz(kde, x=bin_centers)
    return bin_centers, kde
