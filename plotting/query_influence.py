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
data = "../data/query_influence.csv"
df = pd.read_csv(data)

plt.figure(figsize=(10, 6))
g = sns.scatterplot(x='query_type', y='time', hue='query_type', data=df, legend=False)
g.set_yscale("log")
g.set(xlabel='SQL query type', ylabel='Response time (ns)', title='Influence of query type on response time')

plt.savefig('query_influence.eps', format='eps', dpi=4000)