from datetime import timedelta

import matplotlib.colors as mcolors
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from cycler import cycler
from pandas.plotting import register_matplotlib_converters

from rcounting.analysis import fft_kde
from rcounting.counters import apply_alias, is_banned_counter
from rcounting.units import DAY, HOUR, MINUTE

register_matplotlib_converters()


def format_x_date_month(ax):
    months = mdates.MonthLocator()  # every month
    quarters = mdates.MonthLocator([1, 4, 7, 10])
    monthFmt = mdates.DateFormatter("%b %Y")
    ax.xaxis.set_major_locator(quarters)
    ax.xaxis.set_major_formatter(monthFmt)
    ax.xaxis.set_minor_locator(months)


def parts_vs_counts(df):
    k_parts = df.groupby("username")["submission_id"].nunique()
    hoc = df.groupby("username")["submission_id"].count()
    combined = pd.merge(k_parts, hoc, left_index=True, right_index=True)
    combined.columns = ["k_parts", "total_counts"]
    combined = combined.query("k_parts > 10")
    assert combined is not None, "Error, querying too small a dataframe"
    linear_model = np.polyfit(np.log10(combined["k_parts"]), np.log10(combined["total_counts"]), 1)
    print(linear_model)
    axis = np.linspace(1, combined["k_parts"].max(), endpoint=True)

    plt.scatter(combined["k_parts"], combined["total_counts"], alpha=0.7)
    plt.plot(
        axis, 10 ** (np.poly1d(linear_model)(np.log10(axis))), linestyle="--", color="0.3", lw=2
    )
    plt.xlabel("Threads participated in ")
    plt.ylabel("Total counts made")
    plt.yscale("log")
    plt.xscale("log")
    plt.xlim(left=10)
    plt.ylim(bottom=10)
    plt.savefig("parts_vs_counts.png", dpi=300, bbox_inches="tight")


def hoc_by_time(df, n=25, ax=None):
    if ax is None:
        _, ax = plt.subplots(1, figsize=(10, 6))

    if df.index.inferred_type != "datetime64" and "timestamp" in df.columns:
        df["date"] = pd.to_datetime(df["timestamp"], unit="s")
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)

    df["username"] = df["username"].apply(apply_alias)
    top_counters = [
        x
        for x in df.groupby("username").size().sort_values(ascending=False).index
        if not is_banned_counter(x)
    ][:n]
    filtered = df.query("username in @top_counters")
    cumulative = pd.get_dummies(filtered["username"]).resample("12h").sum().expanding().sum()
    cumulative[top_counters].plot(ax=ax)
    plt.legend(bbox_to_anchor=(1.05, 1.07))
    ax.set_ylabel("Cumulative counts")
    ax.set_xlabel("")
    ax.set_ylim(bottom=0)
    return ax


def speedrun_histogram(df, n=3):
    bins = np.arange(0, 21)
    df = df.copy()
    df["dt"] = df["timestamp"].diff()
    counters = df.query("dt < 20").groupby("username")["dt"].mean().sort_values().index
    fig, axes = plt.subplots(n, sharex=True, sharey=True)
    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    for idx, counter in enumerate(counters[:n]):
        ax = axes[idx]
        ax.hist(
            df.query("username == @counter")["dt"],
            bins=bins,
            alpha=0.6,
            label=counter,
            density=True,
            color=colors[idx],
            edgecolor="k",
        )
        ax.legend()
    ax.set_xlim(0, 11)
    ax.set_xticks([0.5, 5.5, 10.5])
    ax.set_xticklabels(["0", "5", "10"])
    return fig


def time_of_day_histogram(df, ax, n=4):
    bins = np.linspace(0, DAY, 24 * 12 + 1)
    df = df.copy()
    df["time_of_day"] = df["timestamp"].astype(int) % DAY
    top_counters = df["username"].value_counts().index[:n]
    ax.hist(df["time_of_day"], bins=bins, alpha=0.8, label="total", color="C3", edgecolor="k")
    for counter in top_counters:
        data = df.query("username==@counter")["time_of_day"]
        ax.hist(data, bins=bins, alpha=0.7, label=counter, edgecolor="k")
    ax.set_xlim(0, DAY + 1)
    intervals = range(0, 25, 3)
    ax.set_xticks([x * HOUR for x in intervals])
    ax.set_xticklabels(["f{x:02d}:00" for x in intervals])
    ax.legend()
    ax.set_ylabel("Number of counts per 5 min interval")
    return ax


def time_of_day_kde(
    df, ax, n_counters=4, time_resolution=MINUTE / 2, show_total=True, normalize=True
):
    alpha = 0.8
    sigma = 0.02
    nbins = int(DAY / time_resolution)
    df = df[["username", "timestamp"]].copy()
    df["time_of_day"] = df["timestamp"].astype(int) % DAY
    counts = df["username"].value_counts()
    top_counters = counts.index[:n_counters]
    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    if show_total:
        x, kde = fft_kde(df["time_of_day"], nbins, kernel="normal_distribution", sigma=sigma)
        if normalize:
            kde *= len(df)
        ax.fill_between(x, kde, label="All Counts", color="0.8")
    for idx, counter in enumerate(top_counters):
        data = df.query("username==@counter")["time_of_day"]
        x, kde = fft_kde(data, nbins, kernel="normal_distribution", sigma=sigma)
        if normalize:
            kde *= counts.loc[counter]
        ax.fill_between(x, kde, color=colors[idx], alpha=alpha)
        ax.plot(x, kde, label=counter, color=colors[idx], lw=2)
    intervals = range(0, 25, 3)
    ax.set_xlim(0, DAY + 1)
    ax.set_xticks([x * HOUR for x in intervals])
    ax.set_xticklabels([f"{x:02d}:00" for x in intervals])
    ax.set_ylim(bottom=0)
    ax.legend()
    ax.set_xlabel("Time of day [UTC]")
    ax.set_ylabel("Counts per second")
    return ax


def plot_get_time(df, ax, **kwargs):
    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"][:2]
    cc = cycler(colors) * cycler(marker=list("vs"))
    ax.set_prop_cycle(cc)
    modstring = {False: "Non-mod", True: "Mod"}
    get_type = {"get": "Get", "assist": "Assist"}

    for count_type in ["get", "assist"]:
        for modness in [False, True]:
            subset = df.query("is_moderator == @modness and count_type == @count_type")
            ax.plot(
                subset["timestamp"],
                subset["elapsed_time"],
                linestyle="None",
                label=f"{get_type[count_type]} by {modstring[modness]}",
                alpha=0.8,
                **kwargs,
            )

    one_day = timedelta(1)
    start_date, end_date = df["timestamp"].min(), df["timestamp"].max()
    ax.set_xlim(left=start_date - one_day, right=end_date + one_day)
    format_x_date_month(ax)
    ax.set_ylim(0, 30)
    ax.set_ylabel("Elapsed time for assists and gets [s]")
    ax.legend(loc="upper right")
    return ax


def simulate_alpha(color, alpha):
    """Find the average of the current color and pure white, weighted by alpha"""
    white = np.array((1, 1, 1))
    return tuple(np.array(mcolors.to_rgb(color)) * alpha + (1 - alpha) * white)


def make_time_axis(ax):
    """Take an x axis that runs from 0 to 86400 seconds and make ticks at
    sensible intervals with sensible names

    """
    ticks, labels = zip(*[(x * HOUR, f"{x:02d}:00") for x in range(0, 25, 3)])
    ax.set_xlim(0, DAY + 1)
    ax.set_xticks(ticks)
    ax.set_xticklabels(labels)
    ax.set_ylim(bottom=0)
