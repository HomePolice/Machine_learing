#!/usr/bin/env python
# coding: utf-8

# In[1]:


# modules
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler


# In[ ]:





# In[2]:


def learn_video(data):
    KM = KMeans(n_clusters=2, algorithm='auto', random_state=10)
    n_data = data[['cnt_max', 'cnt_max_divide_mean']]
    scaler = MinMaxScaler(copy=True, feature_range=(0, 1))
    scaler.fit(n_data)
    final = scaler.transform(n_data)
    final = pd.DataFrame(final)
    
    KM.fit(final)
    
    return KM


# In[ ]:





# In[3]:


def learn_cctv(data):
    KM = KMeans(n_clusters=2, algorithm='auto', random_state=1)
    n_data = data[['cnt_max', 'minus_Min_Max', 'std', 'ipLenCount', 'srcPort']]
    scaler = MinMaxScaler(copy=True, feature_range=(0,1))
    scaler.fit(n_data)
    final = scaler.transform(n_data)
    final = pd.DataFrame(final)
    
    KM.fit(final)
    
    return KM


# In[ ]:




