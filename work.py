import os
from environs import Env

env = Env()
env.read_env()

CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
REFRESH_TOKEN = os.environ.get('REFRESH_TOKEN')

env_variables:dict = {
        'CLIENT_ID': CLIENT_ID,
        'CLIENT_SECRET': CLIENT_SECRET,
        'REFRESH_TOKEN': REFRESH_TOKEN
    }

# print(env_variables)

import requests

from src import endpoints
from src.env_handler import env_variables


payload:dict = {
'client_id': env_variables['CLIENT_ID'],
'client_secret': env_variables['CLIENT_SECRET'],
'refresh_token': env_variables['REFRESH_TOKEN'],
'grant_type': "refresh_token",
'f': 'json'
}
res = requests.post(endpoints.auth_endpoint, data=payload, verify=False)
access_token = res.json()['access_token']

print(access_token)