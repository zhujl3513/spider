import requests
import pandas as pd
import json
import time
import random
from typing import List, Dict, Any, Optional

class SSEKCBFullDataCrawler:
    """
    上海证券交易所科创板全量数据抓取类
    用于获取所有科创板上市公司的完整数据
    """

    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://www.sse.com.cn/',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
        }
        self.session.headers.update(self.headers)

    def get_all_kcb_stocks(self) -> List[Dict[str, Any]]:
        """
        获取所有科创板股票列表
        参考: https://query.sse.com.cn/sseQuery/commonQuery.do
        """
        import random
        url = "https://query.sse.com.cn/sseQuery/commonQuery.do"

        # 随机生成回调函数名，避免缓存
        jsonp_callback = f"jsonpCallback{random.randint(100000, 999999)}"

        params = {
            'jsonCallBack': jsonp_callback,
            'STOCK_TYPE': '8',  # 8代表科创板
            'sqlId': 'COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L',
            'COMPANY_STATUS': '2,4,5,7,8',  # 各种上市状态
            'type': 'inParams',
            'isPagination': 'true',
            'pageHelp.cacheSize': '1',
            'pageHelp.beginPage': '1',
            'pageHelp.pageSize': '1000',  # 设置足够大的值获取所有股票
            'pageHelp.pageNo': '1',
            'pageHelp.endPage': '5'
        }

        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()

            # 处理JSONP响应
            json_str = response.text.replace(f'{jsonp_callback}(', '').rstrip(')')
            data = json.loads(json_str)

            if 'result' in data:
                print(f"成功获取 {len(data['result'])} 只科创板股票")
                return data['result']
            else:
                print("未找到科创板股票列表数据")
                return []

        except Exception as e:
            print(f"获取科创板股票列表失败: {e}")
            return []

    def get_stock_detailed_info(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        获取单只股票的详细信息
        包括公司基本信息和股本结构等
        """
        import random
        url = "https://query.sse.com.cn/commonQuery.do"

        jsonp_callback = f"jsonpCallback{random.randint(100000, 999999)}"

        # 接口1: 获取公司基本信息
        params1 = {
            'jsonCallBack': jsonp_callback,
            'isPagination': 'false',
            'sqlId': 'COMMON_SSE_CP_GPJCTPZ_GPLB_GPGK_GSGK_C',
            'COMPANY_CODE': stock_code
        }

        # 接口2: 获取股本结构信息
        params2 = {
            'jsonCallBack': f"jsonpCallback{random.randint(100000, 999999)}",
            'isPagination': 'false',
            'sqlId': 'COMMON_SSE_CP_GPJCTPZ_GPLB_GPGK_GBJG_C',
            'COMPANY_CODE': stock_code
        }

        try:
            # 请求公司基本信息
            response1 = self.session.get(url, params=params1, timeout=10)
            response1.raise_for_status()
            json_str1 = response1.text[response1.text.find('(')+1:response1.text.rfind(')')]
            data1 = json.loads(json_str1)

            # 请求股本结构信息
            time.sleep(0.5)  # 添加延迟
            response2 = self.session.get(url, params=params2, timeout=10)
            response2.raise_for_status()
            json_str2 = response2.text[response2.text.find('(')+1:response2.text.rfind(')')]
            data2 = json.loads(json_str2)

            # 合并数据
            result = {}
            if 'result' in data1 and data1['result']:
                result.update(data1['result'][0])
            if 'result' in data2 and data2['result']:
                result.update(data2['result'][0])

            return result if result else None

        except Exception as e:
            print(f"获取股票 {stock_code} 详细信息失败: {e}")
            return None

    def get_stock_market_data(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        获取股票市场数据（行情数据）
        使用不同的接口获取更丰富的市场数据
        """
        import random
        url = "https://query.sse.com.cn/commonQuery.do"

        jsonp_callback = f"jsonpCallback{random.randint(100000, 999999)}"

        params = {
            'jsonCallBack': jsonp_callback,
            'isPagination': 'false',
            'sqlId': 'COMMON_SSE_SSE_CP_GPJCTPZ_GPLB_SSGSJ_C',  # 实时股价数据接口
            'STOCK_CODE': stock_code
        }

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()

            json_str = response.text[response.text.find('(')+1:response.text.rfind(')')]
            data = json.loads(json_str)

            if 'result' in data and data['result']:
                return data['result'][0]
            else:
                return None

        except Exception as e:
            print(f"获取股票 {stock_code} 市场数据失败: {e}")
            return None

    def crawl_all_kcb_data(self, output_file: str = "sse_kcb_full_data.csv"):
        """
        爬取所有科创板股票完整数据
        """
        print("开始获取科创板股票列表...")
        all_stocks = self.get_all_kcb_stocks()

        if not all_stocks:
            print("未获取到科创板股票列表，程序结束")
            return

        print(f"共找到 {len(all_stocks)} 只科创板股票，开始获取详细数据...")

        all_data = []
        success_count = 0
        fail_count = 0

        for i, stock in enumerate(all_stocks):
            stock_code = stock.get('A_STOCK_CODE', '')
            stock_name = stock.get('COMPANY_ABBR', '未知公司')

            if not stock_code:
                continue

            print(f"正在处理第 {i+1}/{len(all_stocks)} 只股票: {stock_name}({stock_code})")

            # 获取详细信息
            detailed_info = self.get_stock_detailed_info(stock_code)
            market_data = self.get_stock_market_data(stock_code)

            # 合并所有数据
            merged_data = {}
            merged_data.update(stock)  # 基础信息
            if detailed_info:
                merged_data.update(detailed_info)  # 详细信息
            if market_data:
                merged_data.update(market_data)  # 市场数据

            if merged_data:
                all_data.append(merged_data)
                success_count += 1
                print(f"√ 成功获取 {stock_name} 的数据")
            else:
                fail_count += 1
                print(f"× 获取 {stock_name} 数据失败")

            # 添加随机延时，避免请求过于频繁 (1-3秒)
            delay = random.uniform(1, 3)
            time.sleep(delay)

        # 保存数据到CSV文件
        if all_data:
            df = pd.DataFrame(all_data)

            # 重命名列名为中文，便于理解
            column_mapping = {
                'A_STOCK_CODE': '股票代码',
                'COMPANY_ABBR': '公司简称',
                'FULL_NAME': '公司全称',
                'FULL_NAME_EN': '英文全称',
                'AREA_NAME': '地区',
                'CSRC_CODE_DESC': '证监会行业',
                'CSRC_GREAT_CODE_DESC': '证监会大类行业',
                'A_LIST_DATE': '上市日期',
                'REG_CAPITAL': '注册资本',
                'A_ISSUE_VOL': '发行数量',
                'A_ISSUE_PRICE': '发行价格',
                'TOTAL_SHARES': '总股本',
                'FLOW_SHARES': '流通股本',
                'CLOSE_PRICE': '收盘价',
                'CHANGE_RATE': '涨跌幅',
                'TURNOVER_RATE': '换手率',
                'PE_RATE': '市盈率',
                'PB_RATE': '市净率',
                'TOTAL_MARKET_VALUE': '总市值',
                'FLOW_MARKET_VALUE': '流通市值'
            }

            df.rename(columns=column_mapping, inplace=True)

            # 保存到CSV文件
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"数据已保存到 {output_file}")
            print(f"成功: {success_count}, 失败: {fail_count}")
        else:
            print("未获取到任何有效数据")

        return all_data

    def get_kcb_history_data(self, stock_code: str, start_date: str, end_date: str) -> Optional[Dict[str, Any]]:
        """
        获取科创板历史行情数据
        参数:
            stock_code: 股票代码，格式如 'sh688001'
            start_date: 开始日期，格式 '20200101'
            end_date: 结束日期，格式 '20201231'
        返回: 历史行情数据
        """

        import random
        url = "https://query.sse.com.cn/commonQuery.do"

        jsonp_callback = f"jsonpCallback{random.randint(100000, 999999)}"

        params = {
            'jsonCallBack': jsonp_callback,
            'sqlId': 'COMMON_SSE_CP_GPJCTPZ_GPLB_KLINE_C',
            'STOCK_CODE': stock_code,
            'START_DATE': start_date,
            'END_DATE': end_date,
            'KLINE_TYPE': 'D'  # D表示日K线
        }

        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()

            json_str = response.text[response.text.find('(')+1:response.text.rfind(')')]
            data = json.loads(json_str)

            if 'result' in data:
                return data['result']
            else:
                return None

        except Exception as e:
            print(f"获取股票 {stock_code} 历史数据失败: {e}")
            return None

# 使用示例
if __name__ == "__main__":
    # 创建爬虫实例
    crawler = SSEKCBFullDataCrawler()

    # 爬取所有科创板数据
    all_data = crawler.crawl_all_kcb_data("sse_kcb_complete_data.csv")

    # 如果需要获取某只股票的历史数据，可以这样调用：  后续改进
    # history_data = crawler.get_kcb_history_data('sh688001', '20230101', '20231231')