# для тележки
import requests
import json
from urllib.parse import urlencode

def telega (message):
    
    with open('token') as f:
        token = f.read().strip()
    
    message=message
    api_patern=f'https://api.telegram.org/bot{token}'
    get_Up=f'{api_patern}/getUpdates'
    
    info=requests.get(get_Up)
    chat_id = info.json()['result'][0]['message']['chat']['id']
    
    params = {'chat_id': chat_id, 'text': message}

    base_url = f'https://api.telegram.org/bot{token}/'
    url = base_url + 'sendMessage?' + urlencode(params)
    
    resp = requests.get(url)