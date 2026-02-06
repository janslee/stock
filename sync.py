import akshare as ak
import pandas as pd
import requests
import time
from datetime import datetime

# ================= 配置区 =================
CONFIG = {
    "appKey": "HTYRfNml3kDUwaxD",
    "sign": "3cbd4addc5ce40c1db764e6aca04afb265ac5e3311f62adfbc09d3f7d1e3ab6d",
    "worksheetId": "Sheet1",
    "api_url": "https://nocode.qxerp.com/x-nocode/api/v1/open/worksheet/addRows"
}

# 是否同步最近3年历史数据？ (第一次运行时建议选 True)
SYNC_HISTORY = True 

def push_to_nocode(rows, batch_name="数据"):
    if not rows: return
    headers = {"Content-Type": "application/json"}
    # 历史数据量大时，分批发送
    batch_size = 50 
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        payload = {**CONFIG, "rows": batch, "triggerWorkflow": False}
        try:
            resp = requests.post(CONFIG["api_url"], json=payload, headers=headers, timeout=30)
            if resp.status_code == 200:
                print(f"[{batch_name}] 成功推送 {i+len(batch)}/{len(rows)} 条")
            else:
                print(f"推送失败: {resp.text}")
        except Exception as e:
            print(f"连接异常: {e}")
        time.sleep(0.5)

def start_sync():
    print("1. 正在获取板块代码对照表...")
    try:
        concept_df = ak.stock_board_concept_name_em()
        code_map = dict(zip(concept_df['板块名称'], concept_df['板块代码']))
    except:
        print("无法获取代码表，将使用默认代码。")
        code_map = {}

    # --- 情况 A: 同步最近三年历史数据 ---
    if SYNC_HISTORY:
        print("2. 开始同步最近三年历史数据 (可能需要较长时间)...")
        three_years_ago = (datetime.now() - pd.DateOffset(years=3)).strftime("%Y-%m-%d")
        
        # 为了效率，我们遍历主要的板块
        target_names = list(code_map.keys())
        for name in target_names:
            try:
                print(f"  -> 同步板块历史: {name}")
                h_df = ak.stock_concept_fund_flow_hist(symbol=name)
                h_df['日期'] = pd.to_datetime(h_df['日期'])
                recent = h_df[h_df['日期'] >= three_years_ago]
                
                history_rows = []
                for _, r in recent.iterrows():
                    history_rows.append([
                        {"controlId": "bankuaixuhao", "value": "0"},
                        {"controlId": "daima", "value": code_map.get(name, "000000")},
                        {"controlId": "mingcheng", "value": name},
                        {"controlId": "riqi", "value": r['日期'].strftime("%Y-%m-%d")},
                        {"controlId": "zhulijingliuru", "value": str(round(r['主力净流入-净额']/100000000, 2))},
                        {"controlId": "sanhujingliuru", "value": str(round(r['小单净流入-净额']/100000000, 2))},
                        {"controlId": "zhanchengjiaoebi", "value": str(r['主力净流入-净占比'])}
                    ])
                push_to_nocode(history_rows, batch_name=f"历史-{name}")
                time.sleep(1) # 保护 AKShare 接口
            except Exception as e:
                print(f"板块 {name} 历史同步跳过: {e}")

    # --- 情况 B: 同步今日排名数据 ---
    print("3. 正在获取今日实时排名...")
    try:
        df = ak.stock_sector_fund_flow_rank(indicator="今日")
        today_str = datetime.now().strftime("%Y-%m-%d")
        today_rows = []
        for index, row in df.iterrows():
            today_rows.append([
                {"controlId": "bankuaixuhao", "value": str(index + 1)},
                {"controlId": "daima", "value": code_map.get(row['名称'], "000000")},
                {"controlId": "mingcheng", "value": str(row['名称'])},
                {"controlId": "riqi", "value": today_str},
                {"controlId": "zhulijingliuru", "value": str(round(float(row['今日主力净流入-净额'])/100000000, 2))},
                {"controlId": "sanhujingliuru", "value": str(round(float(row['今日小单净流入-净额'])/100000000, 2))},
                {"controlId": "zhanchengjiaoebi", "value": str(row['今日主力净流入-净占比'])}
            ])
        push_to_nocode(today_rows, batch_name="今日排名")
    except Exception as e:
        print(f"今日数据获取失败: {e}")

if __name__ == "__main__":
    start_sync()