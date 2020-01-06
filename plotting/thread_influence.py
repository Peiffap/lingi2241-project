#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Gilles Peiffer
"""

import numpy as np
import pandas as pd
import seaborn as sns

import matplotlib.pyplot as plt

# Suppress warnings
import warnings
warnings.filterwarnings('ignore')

# Theoretical prediction function
def theopredict(service, arr, m):
    res = []
    for mval in m:
        ar = 1000/arr
        util = ar/(mval*service)
        print(mval)
        print(util)
        print(ar)
        print(service)
        print()
        if util < 1:
            a = ar/service
            pi0 = 1/(sum(a**i/np.math.factorial(i) for i in range(mval)) + a**mval/(np.math.factorial(mval) * (1 - util)))
            res.append(1000000000/ar * (a + (util * a**mval * pi0)/((1 - util)**2 * np.math.factorial(mval))))
    
    return res

# GetAverage query

# Import Data
data = "../data/thread_influence_1.csv"
df = pd.read_csv(data)

plt.figure(figsize=(10, 6))
g = sns.scatterplot(x='n_threads', y='time', hue='is_average', data=df, legend='full')
data = "../data/thread_influence_2.csv"
df = pd.read_csv(data)
g = sns.scatterplot(x='n_threads', y='time', hue='is_average', data=df, legend=False)
data = "../data/thread_influence_3.csv"
df = pd.read_csv(data)
g = sns.scatterplot(x='n_threads', y='time', hue='is_average', data=df, legend=False)
data = "../data/thread_influence_4.csv"
df = pd.read_csv(data)
g = sns.scatterplot(x='n_threads', y='time', hue='is_average', data=df, legend=False)
data = "../data/thread_influence_5.csv"
df = pd.read_csv(data)
g = sns.scatterplot(x='n_threads', y='time', hue='is_average', data=df, legend=False)
data = "../data/thread_influence_6.csv"
df = pd.read_csv(data)
g = sns.scatterplot(x='n_threads', y='time', hue='is_average', data=df, legend=False)
data = "../data/thread_influence_7.csv"
df = pd.read_csv(data)
g = sns.scatterplot(x='n_threads', y='time', hue='is_average', data=df, legend=False)
data = "../data/thread_influence_8.csv"
df = pd.read_csv(data)
g = sns.scatterplot(x='n_threads', y='time', hue='is_average', data=df, legend=False)
g.set_yscale("log")
g.set(xlabel='Number of server threads', ylabel='Response time (ns)', title='Influence of number of threads on response time for GetAverage queries (5 queries/s arrival rate)')

arrivaltimes = 200
m = [1, 2, 3, 4, 5, 6, 7, 8]
serv = 1000/190
plt.plot(m, theopredict(serv, arrivaltimes, m), '-g')
g.get_legend().set_title('Legend')
# Hack to remove the first legend entry (which is the undesired title).
vpacker = g.get_legend()._legend_handle_box.get_children()[0]
vpacker._children = vpacker.get_children()[1:]
# Correcting legend values
l = g.legend_
l.texts[1].set_text('Data')
l.texts[2].set_text('Average')
l.texts[3].set_text('Theoretical model')
plt.savefig('thread_influence.eps', format='eps', dpi=1_000_000)
plt.show()
