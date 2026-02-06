import efinance as ef
import pandas as pd
import requests
import json
import time
from datetime import datetime

# ================= 配置区 =================
CONFIG = {
    "appKey": "HTYRfNml3kDUwaxD",
    "sign": "3cbd4addc5ce40c1db764e6aca04afb265ac5e3311f62adfbc09d3f7d1e3ab6d",
    "worksheetId": "Sheet1",
    "api_url": "https://nocode.qxerp.com/x-nocode/api/v1/open/worksheet/addRows"
}

def push_to_nocode(row_data):
    """单条推送，强制断开连接，极高稳定性"""
    headers = {
        "Content-Type": "application/json",
        "Connection": "close",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0"
    }
    payload = {
        "appKey": CONFIG["appKey"],
        "sign": CONFIG["sign"],
        "worksheetId": CONFIG["worksheetId"],
        "rows": [row_data],
        "triggerWorkflow": True
    }
    try:
        resp = requests.post(CONFIG["api_url"], data=json.dumps(payload), headers=headers, timeout=20)
        return resp.status_code == 200
    except Exception as e:
        print(f"  [!] 推送异常: {e}")
        return False

def run_sync():
    print(f"[{datetime.now()}] 正在通过 efinance 获取数据...")
    try:
        # 获取概念板块资金流排名 (比 akshare 更稳)
        df = ef.stock.get_concept_billboard()
        
        if df is None or df.empty:
            print("未能获取到数据，请检查网络。")
            return

        today_str = datetime.now().strftime("%Y-%m-%d")
        print(f"成功获取 {len(df)} 条板块数据，开始推送...")

        for index, row in df.iterrows():
            # efinance 的列名非常固定：
            # '股票名称' (此处为板块名), '今日主力净流入额', '今日主力净流入占比'
            row_json = [
                {"controlId": "bankuaixuhao", "value": str(index + 1)},
                {"controlId": "daima", "value": "000000"}, # 概念板块代码可设固定
                {"controlId": "mingcheng", "value": str(row['股票名称'])},
                {"controlId": "riqi", "value": today_str},
                # efinance 返回的通常已经是元，我们转为亿
                {"controlId": "zhulijingliuru", "value": str(round(float(row['今日主力净流入额'])/100000000, 2))},
                {"controlId": "sanhujingliuru", "value": str(round(float(row['今日小单净流入额'])/100000000, 2))},
                {"controlId": "zhanchengjiaoebi", "value": str(row['今日主力净流入占比']) + "%"},
                {"controlId": "zhanliutongshizhibi", "value": "0"}
            ]
            
            # 推送并打印进度
            if push_to_nocode(row_json):
                print(f"  [√] {row['股票名称']} 同步成功")
            else:
                print(f"  [X] {row['股票名称']} 同步失败")
            
            # 每推一条休息一下，彻底解决 RemoteDisconnected 问题
            time.sleep(1.5)

    except Exception as e:
        print(f"运行出错: {e}")

if __name__ == "__main__":
    run_sync()