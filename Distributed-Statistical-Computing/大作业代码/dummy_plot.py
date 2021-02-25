#! /usr/bin/env python3.6
import pandas as pd
import numpy as np
import seaborn as sns
def dummy_plot(list_df,Y_column,dummy_columns):
    for k in range(len(dummy_columns)):
        df = list_df[k].copy()
        temp = df.groupby(dummy_columns[k]).agg([np.sum,np.size])['count']
        ncount = [temp['sum'][i] for i in range(len(temp['size'])) for j in range(temp['size'][i])]
        df['count'] = df['count']/ncount
        figure = sns.barplot(x=dummy_columns[k],y='count',hue=Y_column,data=df)
        fig = figure.get_figure()
        fig.savefig(dummy_columns[k]+'.png')
