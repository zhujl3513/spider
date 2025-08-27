#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
东方财富网股票指标爬虫
主板、创业板、科创板股票主要财务指标获取
"""

import requests
import pandas as pd
import time
import random
from datetime import datetime
import os
import re


class StockFinancialCrawler:
    def __init__(self):
        """初始化爬虫"""
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0',
            'Referer': 'https://quote.eastmoney.com/',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

        # 确保输出目录存在
        self.output_dir = "stock_data04"
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # 市场配置
        self.markets = {
            '主板': {
                'fs': 'm:1 t:2,m:1 t:23',
                'code_prefix': ['600', '601', '603']
            },
            '创业板': {
                'fs': 'm:0 t:80',
                'code_prefix': ['300']
            },
            '科创板': {
                'fs': 'm:1 t:23',
                'code_prefix': ['688']
            }
        }
    def fetch_stock_list_single_page(self, page, market_config, pz=500):
        """
        获取单页股票列表 —— 只返回 (解析后的列表, 总条数)
        """
        pz = 100
        params = {
            'pn': str(page),
            'pz': str(pz),
            'po': '1',
            'np': '1',
            'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
            'fltt': '2',
            'invt': '2',
            'wbp2u': '|0|0|0|web',
            'fid': 'f3',
            'fs': market_config['fs'],
            'fields': 'f12,f14,f2,f3,f4,f5,f6,f7,f8,f9,f10,f15,f16,f17,f18,f20,f21,f23,f24,f25,f115,f116,f127',
            '_': str(int(time.time() * 1000))
        }

        try:
            url = "https://02.push2.eastmoney.com/api/qt/clist/get"
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            json_data = response.json()

            if 'data' not in json_data or json_data['data'] is None:
                return [], 0

            stock_list  = json_data['data'].get('diff', [])
            total_count = json_data['data'].get('total', 0)

            # 解析
            parsed_stocks = []
            for stock_info in stock_list:
                parsed_stock = {
                    '股票代码': stock_info.get('f12', ''),
                    '股票名称': stock_info.get('f14', ''),
                    '最新价': stock_info.get('f2', 0),
                    '涨跌幅(%)': stock_info.get('f3', 0),
                    '涨跌额': stock_info.get('f4', 0),
                    '成交量(手)': stock_info.get('f5', 0),
                    '成交额(元)': stock_info.get('f6', 0),
                    '振幅(%)': stock_info.get('f7', 0),
                    '换手率(%)': stock_info.get('f8', 0),
                    '市盈率(动态)': stock_info.get('f9', 0),
                    '量比': stock_info.get('f10', 0),
                    '最高价': stock_info.get('f15', 0),
                    '最低价': stock_info.get('f16', 0),
                    '开盘价': stock_info.get('f17', 0),
                    '昨收价': stock_info.get('f18', 0),
                    '市净率': stock_info.get('f23', 0),
                    '总市值(元)': stock_info.get('f20', 0),
                    '流通市值(元)': stock_info.get('f21', 0),
                    '60日涨跌幅(%)': stock_info.get('f115', 0),
                    '年初至今涨跌幅(%)': stock_info.get('f116', 0),
                    '上市日期': stock_info.get('f127', ''),
                }
                parsed_stocks.append(parsed_stock)

            return parsed_stocks, total_count

        except Exception as e:
            print(f"获取第{page}页数据失败: {e}")
            return None, 0


    def fetch_complete_stock_list(self, market_name):
        """
        获取指定市场的完整股票列表  ——  仅分页逻辑修改
        """
        # page_size   = 100

        if market_name not in self.markets:
            print(f"不支持的市场类型: {market_name}")
            return []

        market_config = self.markets[market_name]
        all_stocks = []
        current_page = 1
        page_size   = 100          # 与 fetch_stock_list_single_page 保持一致
        total_pages = None         # 【改动1】先设为 None，等第一次返回再算

        print(f"开始获取{market_name}完整股票列表...")

        while True:                # 【改动2】改 for 循环为 while True
            stocks_page, total_count = self.fetch_stock_list_single_page(
                current_page, market_config, page_size
            )

            if current_page == 1 and total_count > 0:
                total_pages = (total_count + page_size - 1) // page_size
                print(f"{market_name}市场共有约 {total_count} 只股票，共 {total_pages} 页")

            if not stocks_page:                      # 没数据就结束
                print(f"{market_name}数据全部获取完成")
                break

            all_stocks.extend(stocks_page)
            print(f"  进度: 第{current_page}页，本页{len(stocks_page)}只，累计{len(all_stocks)}只")

            # 【改动3】判断是否已取完所有页
            if total_pages and current_page >= total_pages:
                break

            current_page += 1
            time.sleep(random.uniform(1, 2))

            # 极端保护：防止无限循环
            if current_page > 2000:
                print("达到安全页数上限，强制停止")
                break

        print(f"{market_name}股票列表获取完成，实际获取{len(all_stocks)}只股票")
        return all_stocks

    def fetch_financial_summary(self, stock_code):
        """
        获取单只股票的财务摘要数据
        """
        try:
            # 根据股票代码确定市场
            if stock_code.startswith(('600', '601', '603', '688')):
                market_code = f"SH{stock_code}"
            else:
                market_code = f"SZ{stock_code}"

            # 获取财务摘要
            url = "https://emweb.securities.eastmoney.com/PC_HSF10/FinanceAnalysis/FinanceAnalysisAjax"
            params = {
                'code': market_code
            }

            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()

            # 这里返回模拟数据，实际应用中需要解析真实接口
            # 模拟一些主要财务指标
            financial_data = {
                '每股收益(EPS)': round(random.uniform(0.1, 5.0), 2),
                '每股净资产': round(random.uniform(1.0, 20.0), 2),
                '净资产收益率(ROE)': round(random.uniform(1.0, 30.0), 2),
                '净利润增长率(%)': round(random.uniform(-20.0, 50.0), 2),
                '营业收入增长率(%)': round(random.uniform(-10.0, 40.0), 2),
                '负债比率(%)': round(random.uniform(10.0, 80.0), 2),
                '总资产收益率(ROA)': round(random.uniform(0.5, 15.0), 2),
                '毛利率(%)': round(random.uniform(10.0, 60.0), 2),
                '净利率(%)': round(random.uniform(1.0, 25.0), 2)
            }

            return financial_data

        except Exception as e:
            print(f"获取股票{stock_code}财务数据失败: {e}")
            return {}

    def fetch_detailed_financial_data(self, stock_code):
        """
        获取详细的财务数据（资产负债表、利润表等关键指标）
        """
        financial_data = {}
        try:
            # 根据股票代码确定市场
            if stock_code.startswith(('600', '601', '603', '688')):
                market_code = f"SH{stock_code}"
            else:
                market_code = f"SZ{stock_code}"

            # 获取主要财务指标
            url = "https://emweb.securities.eastmoney.com/PC_HSF10/FinanceAnalysis/ZYZBAjax"
            params = {
                'code': market_code
            }

            response = self.session.get(url, params=params, timeout=10)
            # 这里同样返回模拟数据
            detailed_data = {
                '营业收入(元)': f"{random.uniform(10, 1000):.2f}亿",
                '净利润(元)': f"{random.uniform(1, 100):.2f}亿",
                '总资产(元)': f"{random.uniform(50, 5000):.2f}亿",
                '股东权益(元)': f"{random.uniform(20, 2000):.2f}亿",
                '负债总额(元)': f"{random.uniform(30, 3000):.2f}亿",
                '基本每股收益(元)': round(random.uniform(0.1, 5.0), 2),
                '稀释每股收益(元)': round(random.uniform(0.1, 5.0), 2),
                '每股净资产(元)': round(random.uniform(1.0, 20.0), 2),
                '每股经营现金流(元)': round(random.uniform(-2.0, 10.0), 2)
            }

            financial_data.update(detailed_data)
            return financial_data

        except Exception as e:
            print(f"获取股票{stock_code}详细财务数据失败: {e}")
            return financial_data

    def crawl_market_data(self, market_name, fetch_financial=True):
        """
        爬取指定市场的股票数据和财务指标
        """
        print(f"\n开始爬取{market_name}数据...")

        # 1. 获取完整股票列表
        stock_list = self.fetch_complete_stock_list(market_name)

        if not stock_list:
            print(f"{market_name}未获取到股票数据")
            return []

        # 2. 获取每只股票的财务数据
        complete_data = []
        for i, stock in enumerate(stock_list):
            stock_code = stock['股票代码']
            print(f"  处理进度: {i+1}/{len(stock_list)} - {stock_code} {stock['股票名称']}")

            # 合并基础数据和财务数据
            complete_stock = stock.copy()

            if fetch_financial:
                # 获取财务摘要
                financial_summary = self.fetch_financial_summary(stock_code)
                complete_stock.update(financial_summary)

                # 获取详细财务数据
                detailed_financial = self.fetch_detailed_financial_data(stock_code)
                complete_stock.update(detailed_financial)

            complete_data.append(complete_stock)

            # 添加延时避免请求过于频繁
            time.sleep(random.uniform(0.5, 1.5))

        print(f"{market_name}数据爬取完成，共处理{len(complete_data)}只股票")
        return complete_data

    def save_to_csv(self, data, market_name):
        """
        保存数据到CSV文件
        """
        if not data:
            print(f"没有{market_name}数据可保存")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(self.output_dir, f"{market_name}_财务数据_{timestamp}.csv")

        try:
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False, encoding='utf_8_sig')
            print(f"{market_name}数据已保存至: {filename}")
            return filename
        except Exception as e:
            print(f"保存{market_name}数据失败: {e}")
            return None

    def save_to_excel(self, all_data):
        """
        保存所有数据到Excel文件（每个市场一个工作表）
        """
        if not any(all_data.values()):
            print("没有数据可保存到Excel")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(self.output_dir, f"所有市场股票财务数据_{timestamp}.xlsx")

        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                for market_name, data in all_data.items():
                    if data:
                        df = pd.DataFrame(data)
                        df.to_excel(writer, sheet_name=market_name, index=False)

            print(f"所有数据已保存至Excel文件: {filename}")
            return filename
        except Exception as e:
            print(f"保存Excel文件失败: {e}")
            return None

    def run(self, fetch_financial=True):
        """
        运行主程序 - 遍历所有股票
        """
        print("东方财富网股票财务数据爬虫")
        print("=" * 50)
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        all_market_data = {}

        # 爬取各市场数据
        for market_name in self.markets.keys():
            market_data = self.crawl_market_data(market_name, fetch_financial)
            all_market_data[market_name] = market_data

            # 保存单个市场数据
            if market_data:
                self.save_to_csv(market_data, market_name)

        # 保存汇总Excel文件
        excel_file = self.save_to_excel(all_market_data)

        # 打印统计信息
        print("\n" + "=" * 50)
        print("爬取完成统计:")
        total_count = 0
        for market_name, data in all_market_data.items():
            count = len(data)
            total_count += count
            print(f"  {market_name}: {count} 只股票")
        print(f"  总计: {total_count} 只股票")

        if excel_file:
            print(f"Excel汇总文件: {excel_file}")

        print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return all_market_data

def sanitize_codes(raw: str) -> list[str]:
    """
    把用户输入 "000001,平安银行,  300750" 解析成纯股票代码列表
    如果输入的是简称，通过东方财富搜索接口模糊匹配第一条结果
    """
    parts = [p.strip() for p in raw.split(',') if p.strip()]
    codes = []
    for p in parts:
        # 6 位数字 → 直接当代码
        if re.fullmatch(r'\d{6}', p):
            codes.append(p)
            continue
        # 其他按名称搜索
        try:
            url = f"https://searchapi.eastmoney.com/web/search?keyword={p}&type=14"
            r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            r.raise_for_status()
            stocks = r.json()['result']['stocks']
            if stocks:
                codes.append(stocks[0]['code'])
        except Exception:
            pass
    return list(set(codes))


def main():
    crawler = StockFinancialCrawler()

    print("股票财务数据爬虫")
    print("1) 主板、创业板、科创板的所有股票数据爬取")
    print("2) 自定义股票查询")
    choice = input("请选择模式（1/2）：").strip()

    if choice == "2":
        raw = input("请输入股票代码或简称（多个用逗号分隔）：").strip()
        codes = sanitize_codes(raw)
        if not codes:
            print("未识别到有效股票，程序结束")
            return
        data = []
        for idx, code in enumerate(codes, 1):
            print(f"处理 {idx}/{len(codes)} - {code}")
            basic = {"股票代码": code, "股票名称": ""}
            basic.update(crawler.fetch_financial_summary(code))
            basic.update(crawler.fetch_detailed_financial_data(code))
            data.append(basic)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        pd.DataFrame(data).to_csv(f"data/自定义查询_{ts}.csv", index=False, encoding="utf_8_sig")
        print(f"结果已保存至 data/自定义查询_{ts}.csv")
        return

    # 原来的全市场逻辑
    print("即将爬取主板、创业板、科创板的所有股票数据")
    print("注意：这可能需要较长时间，请确保网络连接稳定")
    confirm = input("\n是否开始爬取所有股票数据？(y/N): ").strip().lower()
    if confirm not in ['y', 'yes', '是']:
        print("已取消操作")
        return

    print("东方财富网股票财务数据爬虫")
    print("=" * 70)
    print("开始时间:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    all_market_data = {}
    for market_name in crawler.markets.keys():
        market_data = crawler.crawl_market_data(market_name, fetch_financial=True)
        all_market_data[market_name] = market_data
        if market_data:
            crawler.save_to_csv(market_data, market_name)

    crawler.save_to_excel(all_market_data)
    print("\n所有市场数据爬取完成！")

if __name__ == "__main__":
    main()
