import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import timedelta
from pandas.plotting import register_matplotlib_converters
from parsing import find_count_in_text
register_matplotlib_converters()


def format_x_date_month(ax):
    months = mdates.MonthLocator()  # every month
    quarters = mdates.MonthLocator([1, 4, 7, 10])
    monthFmt = mdates.DateFormatter('%b %Y')
    ax.xaxis.set_major_locator(quarters)
    ax.xaxis.set_major_formatter(monthFmt)
    ax.xaxis.set_minor_locator(months)


def speedrun_histogram(df, ax, n=3):
    bins = np.arange(0, 21)
    df = df.copy()
    df['dt'] = df['timestamp'].diff()
    counters = df.query('dt < 20').groupby('username').mean()['dt'].sort_values().index
    for counter in counters[:n]:
        ax.hist(df.query(f'username == "{counter}"')['dt'],
                bins=bins, alpha=0.6, label=counter, density=True,
                edgecolor='k')
    ax.set_xlim(0, 20)
    ax.set_xticks([0.5, 5.5, 10.5, 15.5])
    ax.set_xticklabels(['0', '5', '10', '15'])
    ax.legend()
    return ax


def time_of_day_histogram(df, ax, n=4):
    bins = np.linspace(0, 24 * 3600, 24 * 12 + 1)
    df = df.copy()
    df['time_of_day'] = df['timestamp'].astype(int) % (24 * 3600)
    top_counters = df['username'].value_counts().index[:n]
    ax.hist(df['time_of_day'], bins=bins, alpha=0.8, label='total', color='C3',
            edgecolor='k')
    for counter in top_counters:
        data = df.query(f'username=="{counter}"')['time_of_day']
        ax.hist(data, bins=bins, alpha=0.7, label=counter,
                edgecolor='k')
    ax.set_xlim(0, 24*3600 + 1)
    hour = 3600
    ax.set_xticks([0*hour, 6*hour, 12*hour, 18*hour, 24*hour])
    ax.set_xticklabels(['00:00', '06:00', '12:00', '18:00', '00:00'])
    ax.legend()
    ax.set_ylabel("Number of counts per 5 min interval")
    return ax


def plot_get_time(df, ax, **kwargs):
    markers = {"get": "v", "assist": "s"}
    colors = {True: "C1", False: "C0"}
    modstring = {False: "Non-mod", True: "Mod"}
    get_type = {"get": "Get", "assist": "Assist"}

    for count_type in ['get', 'assist']:
        for modness in [False, True]:
            subset = df.query(f'is_moderator == {modness} and count_type == "{count_type}"')
            ax.plot(subset['timestamp'], subset['elapsed_time'],
                    marker=markers[count_type],
                    color=colors[modness],
                    linestyle="None",
                    label=f"{get_type[count_type]} by {modstring[modness]}",
                    markeredgecolor=colors[modness],
                    alpha=0.8,
                    **kwargs)

    one_day = timedelta(1)
    start_date, end_date = assists['timestamp'].min(), gets['timestamp'].max()
    ax.set_xlim(left=start_date - one_day, right=end_date + one_day)
    format_x_date_month(ax)
    ax.set_ylim(0, 30)
    ax.set_ylabel('Elapsed time for assists and gets [s]')
    ax.legend(loc='upper right')
    return ax


if __name__ == "__main__":
    gets = pd.read_csv('gets.csv')
    assists = pd.read_csv('assists.csv')

    gets['timestamp'] = pd.to_datetime(gets['timestamp'])
    gets['count_type'] = 'get'
    gets['count'] = gets['body'].apply(find_count_in_text)
    gets = gets.set_index('comment_id')
    assists['timestamp'] = pd.to_datetime(assists['timestamp'])
    assists['count_type'] = 'assist'
    assists['count'] = gets['body'].apply(find_count_in_text)
    assists = assists.set_index('comment_id')

    df = pd.concat([gets, assists])

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax = plot_get_time(df, ax)
    plt.savefig('assists_gets.png')
    plt.show()
