#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Gilles Peiffer
"""

# https://stackoverflow.com/questions/56983979/how-to-change-color-of-outliers-in-seaborn-scatterplot ?

import numpy as np
import pandas as pd
import seaborn as sns

import matplotlib.pyplot as plt

# Suppress warnings
import warnings
warnings.filterwarnings('ignore')

# Theoretical prediction function
def theopredict(service, arrival, m):
    res = []
    for ar in arrival:
        ar = 1000/ar
        util = ar/(m*service)
        a = ar/service
        pi0 = 1/(sum(a**i/np.math.factorial(i) for i in range(m)) + a**m/(np.math.factorial(m) * (1 - util)))
        res.append(1000000000/ar * (a + (util * a**m * pi0)/((1 - util)**2 * np.math.factorial(m))))
    
    return res

# GetAverage query

# Import Data
data = "../data/rate_influence_average.csv"
df = pd.read_csv(data)

plt.figure(figsize=(10, 6))
g = sns.scatterplot(x='mean_time', y='time', hue='is_average', data=df, legend='full')
g.set_yscale("log")
g.set(xlabel='Mean time between query arrivals (ms)', ylabel='Response time (ns)', title='Influence of mean arrival rate on response time for GetAverage queries (3 server threads)')

arrivaltimes = range(54, 1600)
serv = 1000/160
plt.plot(arrivaltimes, theopredict(serv, arrivaltimes, 3), '-g')
g.get_legend().set_title('Legend')
# Hack to remove the first legend entry (which is the undesired title).
vpacker = g.get_legend()._legend_handle_box.get_children()[0]
vpacker._children = vpacker.get_children()[1:]
# Correcting legend values
l = g.legend_
l.texts[1].set_text('Data')
l.texts[2].set_text('Average')
l.texts[3].set_text('Theoretical model')
plt.savefig('rate_influence_average.eps', format='eps', dpi=1_000_000)
plt.show()

# Select query

# Import Data
data = "../data/rate_influence_select.csv"
df = pd.read_csv(data)

plt.figure(figsize=(10, 6))
g = sns.scatterplot(x='mean_time', y='time', hue='is_average', data=df, legend='full')
g.set_yscale("log")
g.set(xlabel='Mean time between query arrivals (ms)', ylabel='Response time (ns)', title='Influence of mean arrival rate on response time for Select queries (3 server threads)')

arrivaltimes = range(77, 1600)
serv = 1000/230
plt.plot(arrivaltimes, theopredict(serv, arrivaltimes, 3), '-g')
g.get_legend().set_title('Legend')
# Hack to remove the first legend entry (which is the undesired title).
vpacker = g.get_legend()._legend_handle_box.get_children()[0]
vpacker._children = vpacker.get_children()[1:]
# Correcting legend values
l = g.legend_
l.texts[1].set_text('Data')
l.texts[2].set_text('Average')
l.texts[3].set_text('Theoretical model')

plt.savefig('rate_influence_select.eps', format='eps', dpi=1_000_000)
plt.show()

# Write query

# Import Data
data = "../data/rate_influence_write.csv"
df = pd.read_csv(data)

plt.figure(figsize=(10, 6))
g = sns.scatterplot(x='mean_time', y='time', hue='is_average', data=df, legend='full')
g.set_yscale("log")
g.set(xlabel='Mean time between query arrivals (ms)', ylabel='Response time (ns)', title='Influence of mean arrival rate on response time for Write queries (3 server threads)')

arrivaltimes = range(9, 1600)
serv = 1000/25
plt.plot(arrivaltimes, theopredict(serv, arrivaltimes, 3), '-g')
g.get_legend().set_title('Legend')
# Hack to remove the first legend entry (which is the undesired title).
vpacker = g.get_legend()._legend_handle_box.get_children()[0]
vpacker._children = vpacker.get_children()[1:]
# Correcting legend values
l = g.legend_
l.texts[1].set_text('Data')
l.texts[2].set_text('Average')
l.texts[3].set_text('Theoretical model')

plt.savefig('rate_influence_write.eps', format='eps', dpi=1_000_000)
plt.show()
