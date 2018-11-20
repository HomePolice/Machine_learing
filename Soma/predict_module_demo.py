#!/usr/bin/env python
# coding: utf-8

# In[59]:


# modules
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import json
import datetime as dt
import socket
import threading
import numpy as np
import learn_module
import pymysql
from statsmodels.tsa.arima_model import ARIMA
from sklearn.cluster import KMeans
from scipy import stats
from sklearn.preprocessing import MinMaxScaler
from neo4j.v1 import GraphDatabase


# In[60]:


# 1분전 찾기
def get_date():
    minute_ago = dt.datetime.now - dt.timedelta(minutes=1)
    return dt.datetime.strftime(minute_ago, "%Y-%m-%d %H:%M:%S")


# In[61]:


# 30분전 찾기
def get_date2():
    thirty_minute_ago = dt.datetime.now - dt.timedelta(minutes=30)
    return dt.datetime.strftime(minute_ago, "%Y-%m-%d %H:%M:%S")


# In[ ]:





# In[62]:


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
    
    # 현재시간 기준 1분전 데이터 가져옵니다.
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
    
    # 현재시간 기준 30분전 데이터 가져옵니다.
    def get_thirty_minute_date(self, ids):
        output = []
        with self._driver.session() as session:
            timestamp = dt.datetime.strftime(dt.datetime.now(), "%Y-%m-%d %H:%M:%S");
            print(timestamp, get_date2())
            response = session.write_transaction(self._get_thirty_minute_date, ids, timestamp, get_date2())
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
    @staticmethod #
    def _get_thirty_minute_date(tx, ids, ts, mg):
        result = tx.run("MATCH (n:"+ ids +")-[r]-() "
                        "WHERE r.timestamp <= $timestamp AND r.timestamp >= $thirty_minute_ago "
                        "RETURN n.dns, n.ip, r.destIp, r.destPort, r.sourcePort, r.sourceIp, r.proto, r.length, r.count, r.timestamp, r.payload, r.cnt_mean, r.cnt_max, r.cnt_max_divide_mean, r.minus_Min_Max, r.ip_len_count, r.std", timestamp=ts, thirty_minute_ago=mg)
        return result


# In[ ]:





# In[63]:


# Video/Non-Video 군집화 및 탐지를 수행합니다.
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





# In[64]:


# 영상 데이터에 대해 이상 탐지를 수행합니다.
def predict_anomaly(tmp):
    n_data = np.log(tmp['length'])
    
    predict = model_anomaly.forecast(steps=len(n_data), alpha=0.01)
    
    thresolds = pd.DataFrame(predict[2])
    thresolds.columns =['min', 'max']
    tmp = pd.concat([tmp, thresolds], axis=1)
    
    return tmp


# In[ ]:





# In[72]:


# MySQL DB 전송 함수
def send_Data(data):
    db = pymysql.connect(host="117.16.136.73", user='konkuk-user', password='konkuk-user', db='homepolice', charsert='utf8')
    curs = db.cursor()
    
    # 이상 패킷 전송
    for i in range(len(data)):
        sql = "INSERT INTO homepolice.history (occured_time, ,src_ip, dest_ip, src_port, dest_port, protocol, description, count, account) VALUES (data.iloc[i,9], data.iloc[i, 5], data.iloc[i, 2], data.iloc[i, 4], data.iloc[i, 3], data.iloc[i, 6], 'Abnormal Packet', data.iloc[i, 8], 'test')"
        curs.execute(sql)
        
    # 임계값 전송
    sql = "INSERT INTO homepolice.thresolds (min, max, account, origin, start_time, end_time) VALUES (list(data['min'].values), list(data['max'].values), 'test', data, data.iloc[0, 9], data.iloc[len(data)-1, 9])"
    
    db.commit()
    db.close()


# In[ ]:





# In[66]:


# 전체 이상 탐지 함수
def predict_main(temp):
    result_anomaly = []
    result_normal = []
    
    # 영상 패킷 탐지
    isv = predict_video(temp)
    video_packet = isv[isv.isVIDEO == 1]
  
    if (len(video_packet) / len(isv))*100<= 0.2:
        print('\n=========탐지된 영상 패킷이 없습니다==========\n')
    else:
        # 영상 데이터 중 CCTV 패킷 분리
        isc = video_packet[video_packet.proto == 17]
        last = predict_anomaly(isc)
        
        # 예측 결과가 이상인 패킷은 result_anomaly 리스트에 추가
        for row in last.itertuples():
            new_data = np.log(row.length)
            if new_data < row.min or new_data > row.max:
                result_anomaly.append(row)
            else:
                result_normal.append(row)
        
        # 이상 탐지된 패킷들을 MySQL DB로 전송
        if (len(result_anomaly) / len(last)) * 100 > 0.2:
            send_Data(result_anomaly)


# In[67]:


# 전역 변수

# 쓰레드 종료
end = False
 
# video 탐지 모델, 이상 탐지 모델 선언
model_video = KMeans() 
model_anomaly = []


# In[68]:


# 탐지 쓰레드 함수 
def execute_func(neo):
    global end
    if end:
        return 
    
    user = neo.get_minute_date("test")
    user = pd.DataFrame.from_records(user)
    user['timestamp'] = pd.to_datetime(user['timestamp'])
    user['isVIDEO'] = 0
    
    predict_main(user)


# In[ ]:





# In[69]:


# 학습 쓰레드 함수
def learn_func(neo):
    global end
    if end:
        return
    
    user = pd.read_csv("testdata.csv", sep=',')
    del user['index']
    user['timestamp'] = pd.to_datetime(user['timestamp'])
    
    model_video = learn_module.learn_video(user)
    user['kmeans1'] = model_video.predict(user['cnt_max', 'cnt_max_divide_mean'])
    user = user[data.kmeans1 == 1]
    model_anomaly = learn_module.learn_anomaly(user)


# In[ ]:





# In[70]:


# 메인 함수
def main():
    neo4j = DataAccessObject("bolt://13.209.93.63:7687", "neo4j", " ") #neo4j 서버와 세션 연결
   
    learn_func(neo4j)
    threading.Timer(10.0, execute_func, args=[neo4j]).start() # 10초 간격으로 탐지 쓰레드 실행


# In[ ]:





# In[34]:


main()

