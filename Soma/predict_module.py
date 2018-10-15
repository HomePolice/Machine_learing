#!/usr/bin/env python
# coding: utf-8

# In[1]:


# modules
import pandas as pd
import json
import datetime as dt
import socket
import threading
import numpy as np
import learn_module
from scipy import stats
from sklearn.naive_bayes import GaussianNB
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from neo4j.v1 import GraphDatabase


# In[ ]:





# In[2]:


# 일분전 찾기
def get_date():
    minute_ago = dt.datetime.now() - dt.timedelta(minutes=1)
    return dt.datetime.strftime(minute_ago, "%Y-%m-%d %H:%M:%S")


# In[ ]:





# In[3]:


def cal_MAX(v, data):
    minute_ago = v.timestamp - dt.timedelta(seconds=60)
    temp = data[data.timestamp >= minute_ago]
    temp = temp[temp.timestamp <= v.timestamp]
    new = temp['cnt'].groupby(temp['dstIp']).max()
    return new[v.dstIp]


# In[4]:


def cal_MEAN(v, data):
    minute_ago = v.timestamp - dt.timedelta(seconds=60)
    temp = data[data.timestamp >= minute_ago]
    temp = temp[temp.timestamp <= v.timestamp]
    new = temp['cnt'].groupby(temp['dstIp']).mean()
    return new[v.dstIp]


# In[5]:


def minus_Min_Max(v, data):
    temp = data[data.index < v.name]
    temp = temp[temp.dstIp == v.dstIp]
    temp = temp.iloc[-60:,:]
    return temp.len.max() - temp.len.min()


# In[6]:


def cal_VAR(v, data):
    temp = data[data.index < v.name]
    temp = temp[temp.dstIp == v.dstIp]
    temp = temp.iloc[-10:,:]
    return np.std(temp.len)


# In[ ]:





# In[ ]:





# In[7]:


class DataAccessObject(object):
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
    # closer
    def close(self):
        self._driver.close()

    # 모든 노드와 관계를 뽑아옵니다.
    def get_data_all(self):
        output = []
        with self._driver.session() as session:
            response = session.write_transaction(self._get_data_all)
        for line in response:
            output.append({'dns': line['n.dns'], 
            'ip': line['n.ip'],
            'destIp': line['r.destIp'],
            'destPort': line['r.destPort'],
            'sourcePort': line['r.sourcePort'],
            'sourceIp': line['r.sourceIp'],
            'protocol': line['r.proto'],
            'length': line['r.length'],
            'count': line['r.count'],
            'timestamp': line['r.timestamp'],
            'payload': line['r.payload']})
        return output

    # 아이디를 특정하여 그 데이터만 뽑아옵니다.
    def get_data_id(self, ids):
        output = []
        with self._driver.session() as session:
            response = session.write_transaction(self._get_data_id, ids)
        for line in response:
            output.append({'dns': line['n.dns'], 
            'ip': line['n.ip'],
            'destIp': line['r.destIp'],
            'destPort': line['r.destPort'],
            'sourcePort': line['r.sourcePort'],
            'sourceIp': line['r.sourceIp'],
            'protocol': line['r.proto'],
            'length': line['r.length'],
            'count': line['r.count'],
            'timestamp': line['r.timestamp'],
            'payload': line['r.payload']})
        return output
    
    def get_minute_date(self, ids):
        output = []
        with self._driver.session() as session:
            timestamp = dt.datetime.strftime(dt.datetime.now(), "%Y-%m-%d %H:%M:%S");
            print(timestamp, get_date())
            response = session.write_transaction(self._get_minute_date, ids, timestamp, get_date())
        for line in response:
            output.append({'dns': line['n.dns'], 
            'ip': line['n.ip'],
            'destIp': line['r.destIp'],
            'destPort': line['r.destPort'],
            'sourcePort': line['r.sourcePort'],
            'sourceIp': line['r.sourceIp'],
            'protocol': line['r.proto'],
            'length': line['r.length'],
            'count': line['r.count'],
            'timestamp': line['r.timestamp'],
            'payload': line['r.payload'],
            'cnt_mean': line['r.cnt_mean'],
            'cnt_max': line['r.cnt_max'],
            'cnt_max_divide_mean': line['r.cnt_max_divide_mean'],
            'minus_Min_Max': line['r.minus_Min_Max'],
            'ip_len_count': line['r.ip_len_count'],
            'std': line['r.std']})
        return output

    #이하 static method
    # 위의 공개된 method의 이름에 _ 하나 붙어있음
    @staticmethod #
    def _get_data_all(tx):
        result = tx.run("MATCH (n)-[r]-() "
                        "RETURN n.dns, n.ip, r.destIp, r.destPort, r.sourcePort, r.sourceIp, r.proto, r.length, r.count, r.timestamp, r.payload")
        return result

    @staticmethod #
    def _get_data_id(tx, ids):
        result = tx.run("MATCH (n:"+ ids +")-[r]-() "
                        "RETURN n.dns, n.ip, r.destIp, r.destPort, r.sourcePort, r.sourceIp, r.proto, r.length, r.count, r.timestamp, r.payload")
        return result
    @staticmethod #
    def _get_minute_date(tx, ids, ts, mg):
        result = tx.run("MATCH (n:"+ ids +")-[r]-() "
                        "WHERE r.timestamp <= $timestamp AND r.timestamp >= $minute_ago "
                        "RETURN n.dns, n.ip, r.destIp, r.destPort, r.sourcePort, r.sourceIp, r.proto, r.length, r.count, r.timestamp, r.payload, r.cnt_mean, r.cnt_max, r.cnt_max_divide_mean, r.minus_Min_Max, r.ip_len_count, r.std", timestamp=ts, minute_ago=mg)
        return result


# In[ ]:





# In[8]:


def predict_video(tmp):
    n_data = tmp[['cnt_max', 'cnt_max_divide_mean']]
    scaler = MinMaxScaler(copy=True, feature_range=(0,1))
    scaler.fit(n_data)
    final = scaler.transform(n_data)
    final = pd.DataFrame(final)
    predict = pd.DataFrame(model_video.predict(final))
    predict.columns=['isVIDEO']
      
    result = pd.concat([tmp, predict], axis=1)
        
    return result
    


# In[ ]:





# In[9]:


def predict_cctv(tmp):
    n_data = tmp[['cnt_max', 'minus_Min_Max', 'std', 'ip_len_count']]
    scaler = MinMaxScaler(copy=True, feature_range=(0,1))
    scaler.fit(n_data)
    final = scaler.transform(n_data)
    final = pd.DataFrame(n_data)
    predict = pd.DataFrame(model_cctv.predict(final))
    predict.colmuns = ['isCCTV']
    
    result = pd.concat([tmp, predict], axis=1)
    
    return result


# In[ ]:





# In[10]:


def predict_anomaly(temp):
    result_anomaly = []
    result_cctv = []
    
    # 영상 패킷 탐지
    isVideo = predict_video(temp)
    video_packet = isVideo[isVideo.isVIDEO == 1]
    
    if len(video_packet) == 0:
        print('\n=========탐지된 영상 패킷이 없습니다==========\n')
    else:
        # CCTV 패킷 탐지
        isCctv = predict_cctv(video_packet)

        for row in isCctv.itertuples():
            print(row)
            if row.isCCTV == 1:
                result_anomaly.append(row)
            else:
                result_cctv.append(row)

        if len(result_anomaly) == 0:
            print('\n==========탐지된 Non-CCTV 패킷이 없습니다==========')
        else:
            print("\n==========Non-CCTV 패킷 목록==========")
            for i in result_anomaly:
                print('===============')
                print('SrcIp -> DstIp : ' + i.srcIp + ' -> ' + i.dstIp)
                print('SrcPort : ' + i.srcPort)
                print('DstPort : ' + i.dstPort)
                print('Protocol : ' + i.proto)
                print('Length : ' + i.length)
                print('count : ' + i.count)
                print('Timestamp : ' + i.timestamp)


# In[11]:


end = False


# In[12]:


def execute_func(second=1.0):
    global end
    if end:
        return 
    
    
    neo4j = DataAccessObject("bolt://13.209.93.63:7687", "neo4j", " ")
    user = neo4j.get_minute_date("test")
    user = pd.DataFrame.from_records(user)
    
    predict_anomaly(user)
    
    
    threading.Timer(second, execute_func, [second]).start()


# In[ ]:





# In[13]:


raw_data = pd.read_excel('exports3.xlsx')


# In[14]:


data = raw_data


# In[15]:


data['timestamp'] = pd.to_datetime(data['timestamp'])


# In[16]:


data['cnt_max'] = data.apply(lambda v: cal_MAX(v, data), axis = 1)


# In[17]:


data['cnt_mean'] = data.apply(lambda v: cal_MEAN(v, data), axis = 1)


# In[18]:


data['cnt_max_divide_mean'] = data['cnt_max'] / (data['cnt_mean'] + 0.00000001)


# In[19]:


data = data.fillna(0)


# In[20]:


data['minus_Min_Max'] = data.apply(lambda v: minus_Min_Max(v, data), axis = 1).fillna(0)


# In[21]:


data['std'] = data.apply(lambda v: cal_VAR(v, data), axis = 1).fillna(0)


# In[ ]:





# In[22]:


model_video = learn_module.learn_video(data)


# In[23]:


model_cctv = learn_module.learn_cctv(data)


# In[ ]:





# In[152]:


execute_func(60.0)


# In[ ]:




