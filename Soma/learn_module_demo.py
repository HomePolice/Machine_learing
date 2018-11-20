#!/usr/bin/env python
# coding: utf-8

# In[2]:


# modules
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from sklearn import svm
from statsmodels.tsa.arima_model import ARIMA


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


def learn_anomaly(data):
    model = ARIMA(np.log(data['length']), order=(5,0,4))
    mode_fit = model.fit(trend='nc', full_output=True, disp=1)

    return model_fit


# In[ ]:




