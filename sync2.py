import akshare as ak
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

# 建议先关掉历史，把“今日同步”跑通，再开启历史
SYNC_HISTORY = False 

def push_single_row_safely(row_data, label="数据"):
    """
    单条推送逻辑：最稳、最慢，防止服务器断开
    """
    headers = {
        "Content-Type": "application/json",
        "Connection": "close",  # 关键：告诉服务器不要保持连接，发完就断
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
    }
    
    payload = {
        "appKey": CONFIG["appKey"],
        "sign": CONFIG["sign"],
        "worksheetId": CONFIG["worksheetId"],
        "rows": [row_data], # 单条包装
        "triggerWorkflow": True
    }

    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 使用普通的 requests.post 而不使用 session 保持
            resp = requests.post(
                CONFIG["api_url"], 
                data=json.dumps(payload), 
                headers=headers, 
                timeout=20
            )
            if resp.status_code == 200:
                print(f"  [√] {label} 同步成功")
                return True
            else:
                print(f"  [!] HTTP {resp.status_code}: {resp.text}")
        except Exception as e:
            print(f"  [!] 第 {attempt+1} 次尝试失败: {e}")
            time.sleep(3) # 失败了多等会儿
            
    return False

def start_sync():
    print("--- 启动同步任务 ---")
    
    # 1. 获取对照表
    try:
        concept_df = ak.stock_board_concept_name_em()
        code_map = dict(zip(concept_df['板块名称'], concept_df['板块代码']))
    except:
        code_map = {}

    # 2. 今日实时排名
    print("\n>>> 开始同步今日排名数据...")
    try:
        df = ak.stock_sector_fund_flow_rank(indicator="今日")
        if df.empty:
            print("今日数据为空")
            return

        success_count = 0
        for index, row in df.iterrows():
            name = str(row['名称'])
            # 过滤掉容易出问题的特殊字符
            clean_name = name.replace(" ", "").replace("_", "")
            
            row_json = [
                {"controlId": "bankuaixuhao", "value": str(index + 1)},
                {"controlId": "daima", "value": code_map.get(name, "000000")},
                {"controlId": "mingcheng", "value": clean_name},
                {"controlId": "riqi", "value": datetime.now().strftime("%Y-%m-%d")},
                {"controlId": "zhulijingliuru", "value": str(round(float(row['今日主力净流入-净额'])/100000000, 2))},
                {"controlId": "sanhujingliuru", "value": str(round(float(row['今日小单净流入-净额'])/100000000, 2))},
                {"controlId": "zhanchengjiaoebi", "value": str(row['今日主力净流入-净占比'])}
            ]
            
            # 单条同步
            if push_single_row_safely(row_json, label=f"排名-{clean_name}"):
                success_count += 1
            
            # 强制间隔，模拟真人
            time.sleep(1.2)
            
        print(f"\n今日排名同步结束，成功 {success_count}/{len(df)} 条")

    except Exception as e:
        print(f"今日同步主流程异常: {e}")

    # 3. 历史数据（如果需要）
    if SYNC_HISTORY:
        print("\n>>> 开始同步三年历史...")
        # 逻辑同上，改为调用 push_single_row_safely ...

if __name__ == "__main__":
    start_sync()