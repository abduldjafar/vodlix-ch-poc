import requests
import random
headers = {
    'accept': 'application/json',
    # Already added when you pass json= but not when you pass data=
    # 'Content-Type': 'application/json',
}

json_data={
    'tb_name': 'v1',
    'db_name': 'devel2',
    'datas':{
    "userid":0,
            "session_id":1111 ,
            "ip":"192.168.100.1",
            "user_agent":"Chrome",
            "browser" :"chrome",
            "browser_version" :123,
            "os":"linux",
            "os_version":"amd64",
            "device":"android",
            "device_name" :"alpine",
            "country":"indonesia",
            "region":"asia",
            "city":"Jakarta",
            "latitude":123.45,
            "longitude":111.56 ,
            "isp" :"Telkomsel",
            "internet_speed":3336,
            "app":"app",
            "app_version":1,
            "app_id" :"app_id",
            "object_id":1,
            "object_type":1,
            "object_url":"http://www.testing.com",
            "content_length":"Nullable(String)",
            "title" :"Nullable(String)",
            "collection_id" :123,
            "content_list_id":123,
            "partner_id" :123,
            "event_type":"watch",
            "event_value" :123456,
            "bitrate":123456,
            "width":123456,
            "height" :123456,
            "utm_source" :"utm_source",
            "utm_term" :"utm_term",
            "utm_id":123,
            "utm_medium" :"utm_medium",
            "referrer" :"referrer",
            "referrer_path":"referrer_path"

}
    }

for x in range(0,1000):
    json_data["datas"]["event_value"] = random.randint(10, 10000)
    response = requests.post('http://localhost:18000/api/v1/datas', headers=headers, json=json_data)
