import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import timedelta
from pathlib import Path
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

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
        df = df.set_index('count')
        results[i] = df
        start += 1000
    return pd.concat(results)


def is_mod(username):
    return username in mods


def capture_the_flag_score(thread):
    thread['score'] = elapsed_time = thread['timestamp'].diff().shift(-1)
    return thread.groupby('username')['score'].sum()


gets = pd.read_csv('gets.csv')
assists = pd.read_csv('assists.csv')

gets['timestamp'] = pd.to_datetime(gets['timestamp'])
assists['timestamp'] = pd.to_datetime(assists['timestamp'])


def format_x_date_month(ax):
    months = mdates.MonthLocator()  # every month
    quarters = mdates.MonthLocator([1, 4, 7, 10])
    monthFmt = mdates.DateFormatter('%b %Y')
    ax.xaxis.set_major_locator(quarters)
    ax.xaxis.set_major_formatter(monthFmt)
    ax.xaxis.set_minor_locator(months)


fig = plt.figure()
ax = fig.add_subplot(111)
markers = "vs"
colors = ["C1", "C0"]
modstring = ["Non-mod", "Mod"]
get_type = ["Get", "Assist"]

for marker_idx, data in enumerate([gets, assists]):
    for modness in [0, 1]:
        subset = data[data['is_moderator'] == modness]
        ax.plot(subset['timestamp'], subset['elapsed_time'],
                marker=markers[marker_idx],
                color=colors[modness],
                linestyle="None",
                label=f"{get_type[marker_idx]} by {modstring[modness]}",
                markeredgecolor=colors[modness],
                alpha=0.8)

one_day = timedelta(1)
start_date, end_date = assists['timestamp'].min(), gets['timestamp'].max()
ax.set_xlim(left=start_date - one_day, right=end_date + one_day)
format_x_date_month(ax)
ax.set_ylim(0, 30)
ax.set_ylabel('Elapsed time for assists and gets [s]')
ax.legend(loc='upper right')
plt.savefig('assists_gets.png')
plt.show()
