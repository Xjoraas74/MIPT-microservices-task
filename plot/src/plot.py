import time

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

while True:
    data = pd.read_csv('./logs/metric_log.csv', index_col='id')
    sns.histplot(data['absolute_error'], kde=True)
    plt.xlabel('absolute_error')
    plt.ylabel('Count')
    plt.savefig('./logs/error_distribution.png')

    time.sleep(10)