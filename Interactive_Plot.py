# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 23:03:27 2018

@author: ben
"""
import pandas as pd
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
import sys
import plotly.plotly as py
import plotly.graph_objs as go
import plotly.offline

import numpy as np

# path = sys.argv[1]
path = "./ClusteringByUsers/grade_3/5591e16fa323c7ae921dc54bb7235c61.csv"


fig = plt.figure(figsize=(20, 18))
ax = fig.add_subplot(111, projection='3d')



x0 = pd.read_csv("./BasketClicksDistResults/reg_bef_time_8_noBuy.csv", header =None).loc[0:1000:,1]
y0 = pd.read_csv("./BasketClicksDistResults/reg_aft_time_8_noBuy.csv", header =None).loc[0:1000:,1]

x1 = pd.read_csv("./BasketClicksDistResults/reg_bef_time_8_buy.csv", header =None).loc[0:1000:,1]
y1 = pd.read_csv("./BasketClicksDistResults/reg_aft_time_8_buy.csv", header =None).loc[0:1000:,1]

x0 = pd.cut(x0,[0,50]+[i for i in range(100,3600,100)], labels= False,include_lowest=True)
x1 = pd.cut(x1,[0,50]+[i for i in range(100,3600,100)], labels= False,include_lowest=True)
y0 = pd.cut(y0,[0,50]+[i for i in range(100,3600,100)], labels= False,include_lowest=True)
y1 = pd.cut(y1,[0,50]+[i for i in range(100,3600,100)], labels= False,include_lowest=True)


plt.hist

# ax.scatter(x0, y0, c='r', s=100)
# ax.scatter(x1, y1, c='b', s=100)
#
#
# ax.set_xlabel('Before basket Event')
# ax.set_ylabel('After Basket Event')
#
# fig.savefig("stammm.png")
# plt.show()
#
#
#
# trace0 = go.Scatter(
#     x=x0,
#     y=y0,
#     mode='markers',
#     marker=dict(
#         color='rgba(0, 0, 150,0.8)',
#         size=12,
#         line=dict(
#             color='rgb(0, 0, 150)',
#             width=0.5
#         ),
#         opacity=0.8
#     ),
#     name='Cluster 1'
# )
#
# trace1 = go.Scatter(
#     x=x1,
#     y=y1,
#     mode='markers',
#     marker=dict(
#         color='rgba(150, 0, 0,0.8)',
#         size=12,
#         symbol='circle',
#         line=dict(
#             color='rgb(150, 0, 0)',
#             width=1
#         ),
#         opacity=0.9
#     ),
#     name='Cluster 2'
# )
#
#
# data = [trace0, trace1]
# layout = go.Layout(
#     margin=dict(
#         l=0,
#         r=0,
#         b=0,
#         t=0
#     ),
#     scene=dict(
#         xaxis=dict(title='X- Before Basket Events'),
#         yaxis=dict(title='Y- After Basket Events'),
#     )
# )
# fig = go.Figure(data=data, layout=layout)
# plotly.offline.plot(fig, filename='simple-3d-scatter.html')
