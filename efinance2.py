import urllib.request
import json
import time
from datetime import datetime
import ssl

# 忽略 SSL 证书校验（防止内网 SSL 拦截导致断开）
context = ssl._create_unverified_context()

CONFIG = {
    "appKey": "HTYRfNml3kDUwaxD",
    "sign": "3cbd4addc5ce40c1db764e6aca04afb265ac5e3311f62adfbc09d3f7d1e3ab6d",
    "worksheetId": "Sheet1",
    "api_url": "https://nocode.qxerp.com/x-nocode/api/v1/open/worksheet/addRows"
}

def get_data_urllib():
    url = "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=100&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f62&fs=m:90+t:3+f:!50&fields=f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205,f124,f1,f13"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
        "Referer": "https://quote.eastmoney.com/"
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, context=context, timeout=15) as response:
            res_data = response.read().decode('utf-8')
            return json.loads(res_data)['data']['diff']
    except Exception as e:
        print(f"抓取源数据失败: {e}")
        return []

def push_data_urllib(row_json, name):
    payload = {
        "appKey": CONFIG["appKey"],
        "sign": CONFIG["sign"],
        "worksheetId": CONFIG["worksheetId"],
        "rows": [row_json],
        "triggerWorkflow": True
    }
    data = json.dumps(payload).encode('utf-8')
    headers = {
        "Content-Type": "application/json",
        "Connection": "close"
    }
    try:
        req = urllib.request.Request(CONFIG["api_url"], data=data, headers=headers, method='POST')
        with urllib.request.urlopen(req, context=context, timeout=15) as response:
            print(f"  [√] {name} 成功")
            return True
    except Exception as e:
        print(f"  [!] {name} 推送失败: {e}")
        return False

def run():
    print("--- 启动原生同步任务 ---")
    items = get_data_urllib()
    if not items: return
    
    today_str = datetime.now().strftime("%Y-%m-%d")
    for index, item in enumerate(items):
        row = [
            {"controlId": "bankuaixuhao", "value": str(index + 1)},
            {"controlId": "daima", "value": str(item['f12'])},
            {"controlId": "mingcheng", "value": str(item['f14'])},
            {"controlId": "riqi", "value": today_str},
            {"controlId": "zhulijingliuru", "value": str(round(float(item['f62'])/100000000, 2))},
            {"controlId": "sanhujingliuru", "value": str(round(float(item['f72'])/100000000, 2))},
            {"controlId": "zhanchengjiaoebi", "value": str(item['f184']) + "%"},
        ]
        push_data_urllib(row, item['f14'])
        time.sleep(1.5)

if __name__ == "__main__":
    run()