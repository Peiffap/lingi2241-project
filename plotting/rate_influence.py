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


# Import Data
data = "../data/rate_influence.csv"
df = pd.read_csv(data)

plt.figure(figsize=(10, 6))
g = sns.scatterplot(x='mean_time', y='time', hue='is_average', data=df, legend='full')
g.set_yscale("log")
g.set(xlabel='Mean time between query arrivals', ylabel='Response time (ns)', title='Influence of mean arrival rate on response time')
g.get_legend().set_title('Legend')
# Hack to remove the first legend entry (which is the undesired title).
vpacker = g.get_legend()._legend_handle_box.get_children()[0]
vpacker._children = vpacker.get_children()[1:]
# Correcting legend values
l = g.legend_
l.texts[1].set_text('Data')
l.texts[2].set_text('Average')

plt.savefig('rate_influence.eps', format='eps', dpi=1_000_000)