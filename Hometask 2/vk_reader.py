import requests
import socket
import json


def get_records(ACCESS_TOKEN, URL, COUNT, PAGES_IDS):
    ans = []
    for ID in PAGES_IDS:
        query_url = URL + "?owner_id={}&count={}&access_token={}&v=5.110".format(ID, COUNT, ACCESS_TOKEN)
        response = requests.get(query_url, stream=True)

        data = [line for line in response.iter_lines()][0]
        data = json.loads(data)

        for obj in data['response']['items']:
            ans.append(obj)
        print(query_url, "\nitems: ", len(data['response']['items']), "\n")
    return ans


def send_to_port(conn, records):
    for record in records:
        if 'attachments' in record:
            del record['attachments'] # Удаление медиафайлов (фото, видео)
        d = json.dumps(record)
        conn.sendall(bytes(d + "\n", encoding="utf-8"))


ACCESS_TOKEN = 'f3299f6f5e6c39842f3cd661e9f5cdfbec628591eaec3e456c794d4cac1ade4a9befbfc318bb413699603'
URL = "https://api.vk.com/method/wall.get"
COUNT = 100
PAGES_IDS = [-51765902, -63066646, -39758598, -49128190]
# -51765902 - https://vk.com/thisiscolossal
# -63066646 - https://vk.com/public63066646
# -39758598 - https://vk.com/st.englishnews
# -49128190 - https://vk.com/public49128190

records = get_records(ACCESS_TOKEN, URL, COUNT, PAGES_IDS)

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("localhost", 8990))
s.listen(1)
print("Waiting for connection")
conn, addr = s.accept()
print("Connected")

send_to_port(conn, records)
conn.close()
