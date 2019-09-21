

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
import random, time
import pandas as pd
from datetime import datetime
import json

# A random programmatic shadow client ID.
SHADOW_CLIENT = "FD001"

# The unique hostname that &IoT; generated for 
# this device.
HOST_NAME = "a5p5zfdle6qow-ats.iot.ap-southeast-1.amazonaws.com"

# The relative path to the correct root CA file for &IoT;, 
# which you have already saved onto this device.
ROOT_CA = "AmazonRootCA1.pem"

# The relative path to your private key file that 
# &IoT; generated for this device, which you 
# have already saved onto this device.
PRIVATE_KEY = "51517d25fc-private.pem.key"

# The relative path to your certificate file that 
# &IoT; generated for this device, which you 
# have already saved onto this device.
CERT_FILE = "51517d25fc-certificate.pem.crt"

# A programmatic shadow handler name prefix.
SHADOW_HANDLER = "A0105714N_FD001"

# Automatically called whenever the shadow is updated.
def myShadowUpdateCallback(payload, responseStatus, token):
  print()
  print('UPDATE: $aws/things/' + SHADOW_HANDLER + 
    '/shadow/update/#')
  print("payload = " + payload)
  print("responseStatus = " + responseStatus)
  print("token = " + token)

# Create, configure, and connect a shadow client.
myShadowClient = AWSIoTMQTTShadowClient(SHADOW_CLIENT)
myShadowClient.configureEndpoint(HOST_NAME, 8883)
myShadowClient.configureCredentials(ROOT_CA, PRIVATE_KEY,
  CERT_FILE)
myShadowClient.configureConnectDisconnectTimeout(10)
myShadowClient.configureMQTTOperationTimeout(5)
myShadowClient.connect()

# Create a programmatic representation of the shadow.
myDeviceShadow = myShadowClient.createShadowHandlerWithName(
  SHADOW_HANDLER, True)

print("Start uploading!")

# Read data from file 'train_FD001.txt'
data = pd.read_csv('train_FD001.txt', header=None, delim_whitespace=True)

# Assign column name: id, cycle, os1, os2, os3, sensor1, ...., sensor21
sensor_name = ['sensor' + str(i) for i in range(1,22)]
column_list = ['id', 'cycle', 'os1', 'os2', 'os3'] + sensor_name
data = pd.DataFrame(data.values, columns=column_list)
data['id'] = data['id'].map(lambda s: 'FD001_'+str(s))

nums, dimsm = data.shape

for i in range(3000):
    tmp = data.iloc[i]
    now = datetime.utcnow()
    tmp = tmp.append(pd.Series(['A0105714N', str(now)], index=['MatricID', 'timestamp']))
    tmp = tmp.to_dict()
    
    jsonPayload = {"state": {"reported": tmp}}
    jsonPayload = json.dumps(jsonPayload)
    print(jsonPayload)
    myDeviceShadow.shadowUpdate(jsonPayload,myShadowUpdateCallback, 5)
    time.sleep(2)
    






























