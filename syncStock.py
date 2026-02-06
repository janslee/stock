import akshare as ak
import pandas as pd
import requests
import json
import time
from datetime import datetime, timedelta
from tqdm import tqdm

# --- 1. 接口配置参数 ---
API_URL = "https://nocode.qxerp.com/x-nocode/api/v1/open/worksheet/addRows"
APP_KEY = "HTYRfNml3kDUwaxD"
SIGN = "3cbd4addc5ce40c1db764e6aca04afb265ac5e3311f62adfbc09d3f7d1e3ab6d"
WORKSHEET_ID = "600546"
OWNER_ID = "9623f876-905d-44ba-8c04-ae50aef15469"

# 分批上传大小（建议 100-200，取决于接口响应速度和负载）
BATCH_SIZE = 100 

# 计算时间范围
end_date = datetime.now().strftime("%Y%m%d")
start_date = (datetime.now() - timedelta(days=3*365)).strftime("%Y%m%d")

def format_row_data(symbol, row):
    """根据接口格式要求转换单行数据"""
    mapping = {
        "日期": "riqi",
        "开盘": "kaipan",
        "收盘": "shoupan",
        "最高": "zuigao",
        "最低": "zuidi",
        "成交量": "chengjiaoliang",
        "成交额": "chengjiaoe",
        "振幅": "zhenfu",
        "涨跌幅": "zhangdiefu",
        "涨跌额": "zhangdiee",
        "换手率": "huanshoulv"
    }
    
    api_row = []
    # 股票代码
    api_row.append({"controlId": "gupiaodaima", "value": str(symbol)})
    
    # 各项指标数据
    for ak_col, cid in mapping.items():
        val = row.get(ak_col, "0")
        api_row.append({"controlId": cid, "value": str(val)})
    
    # 唯一标识日期代码 和 OwnerID
    api_row.append({"controlId": "gupiaoriqi", "value": f"{symbol}_{row['日期']}"})
    api_row.append({"controlId": "ownerid", "value": OWNER_ID})
    
    return api_row

def post_to_api(batch_rows):
    """调用 addRows 接口推送数据"""
    payload = {
        "appKey": APP_KEY,
        "sign": SIGN,
        "worksheetId": WORKSHEET_ID,
        "rows": batch_rows,
        "triggerWorkflow": True
    }
    
    try:
        response = requests.post(API_URL, json=payload, timeout=30)
        if response.status_code != 200:
            print(f"\n接口报错: {response.text}")
            return False
        return True
    except Exception as e:
        print(f"\n网络异常: {e}")
        return False

def start_sync():
    print(f"正在获取全量 A 股列表...")
    try:
        # 获取所有 A 股代码
        stock_list_df = ak.stock_zh_a_spot_em()
        stocks = stock_list_df[['代码', '名称']].values.tolist()
    except Exception as e:
        print(f"无法获取股票列表: {e}")
        return

    print(f"共发现 {len(stocks)} 只股票，准备推送 3 年数据...")
    
    # 遍历每只股票
    for symbol, name in tqdm(stocks, desc="同步进度"):
        try:
            # 抓取 AkShare 3年数据 (前复权)
            df = ak.stock_zh_a_hist(
                symbol=symbol, 
                period="daily", 
                start_date=start_date, 
                end_date=end_date, 
                adjust="qfq"
            )
            
            if df.empty:
                continue
            
            # 准备当前股票的待发送行数据
            current_stock_batch = []
            
            for _, row in df.iterrows():
                formatted_row = format_row_data(symbol, row)
                current_stock_batch.append(formatted_row)
                
                # 当累计达到 BATCH_SIZE 时发送一次
                if len(current_stock_batch) >= BATCH_SIZE:
                    post_to_api(current_stock_batch)
                    current_stock_batch = [] # 重置批次
            
            # 发送该股票剩余的行
            if current_stock_batch:
                post_to_api(current_stock_batch)
            
            # 适当请求间隔，保护接口不被屏蔽
            time.sleep(0.1)
            
        except Exception as e:
            print(f"\n处理股票 {symbol}({name}) 时发生错误: {e}")
            time.sleep(1)
            continue

if __name__ == "__main__":
    start_sync()