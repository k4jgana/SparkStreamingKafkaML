import json
import time
import pandas as pd
import os

from kafka import KafkaProducer
import random

url='https://drive.google.com/file/d/13IImWV1uamCbSPVttVRE4VxmvdWj27uB/view?usp=sharing'
url='https://drive.google.com/uc?id=' + url.split('/')[-2]
df=pd.read_csv(url)

producer = KafkaProducer(bootstrap_servers='localhost:9092', security_protocol="PLAINTEXT")


for i,row in df.iterrows():

    record=row.to_json(orient='records')
    print(json.dumps(record))
    producer.send(
        topic="sensors",
        value=json.dumps(record).encode("utf-8")
    )
    time.sleep(random.randint(500, 2000) / 1000.0)



