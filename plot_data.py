import pandas as pd
import matplotlib.pyplot as plt
import re

"""
It is a python file for drawing data.
"""


def parse_data(text):
    data = {
        'Number of Publishers': [],
        'Publisher QoS': [],
        'Analyzer QoS': [],
        'Delay': [],
        'Total Messages': [],
        'Expected Messages': [],
        'Total Rate': [],
        'Message Loss Rate': [],
        'Out of Order Rate': [],
        'Median Gap': []
    }

    for block in text.split('--------------------------------------------------'):
        if 'Publisher QoS:' in block:
            data['Number of Publishers'].append(int(re.search(r'Number of publishers: (\d+)', block).group(1)))
            data['Publisher QoS'].append(int(re.search(r'Publisher QoS: (\d+)', block).group(1)))
            data['Analyzer QoS'].append(int(re.search(r'Analyzer QoS: (\d+)', block).group(1)))
            data['Delay'].append(int(re.search(r'Configuration: counter/\d+/\d+/(\d+)', block).group(1)))
            data['Total Messages'].append(int(re.search(r'Received messages: (\d+)', block).group(1)))
            data['Expected Messages'].append(int(re.search(r'Expected messages: (\d+)', block).group(1)))
            data['Total Rate'].append(float(re.search(r'Total average rate: ([\d\.]+) messages/sec', block).group(1)))
            data['Message Loss Rate'].append(float(re.search(r'Message loss rate: ([\d\.]+)%', block).group(1)))
            data['Out of Order Rate'].append(float(re.search(r'Out of order rate: ([\d\.]+)%', block).group(1)))
            gap_match = re.search(r'Median inter-message gap: ([\d\.]+) ms', block)
            data['Median Gap'].append(float(gap_match.group(1)) if gap_match else None)

    return pd.DataFrame(data)


# read file
with open('statistics_results.txt', 'r') as file:
    text_data = file.read()

df = parse_data(text_data)


# plot
def plot_metrics(df, publisher_count):
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    plt.suptitle(f'Stats for {publisher_count} Publishers', fontsize=16)

    qos_analyzer_combinations = df[(df['Number of Publishers'] == publisher_count)].groupby(
        ['Publisher QoS', 'Analyzer QoS'])

    for (pub_qos, ana_qos), group in qos_analyzer_combinations:
        for ax, metric, title in zip(axes.flatten(),
                                     ['Total Rate', 'Message Loss Rate', 'Out of Order Rate', 'Median Gap'],
                                     ['Total Message Rate (msgs/sec)', 'Message Loss Rate (%)', 'Out of Order Rate (%)',
                                      'Median Inter-message Gap (ms)']):
            ax.plot(group['Delay'], group[metric], label=f'Pub QoS {pub_qos}, Ana QoS {ana_qos}')
            ax.set_title(title)
            ax.set_xlabel('Delay')
            ax.set_ylabel(title)
            ax.legend()
            ax.grid(True)


for publisher_count in df['Number of Publishers'].unique():
    plot_metrics(df, publisher_count)
    plt.show()
