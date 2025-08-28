#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
东方财富网股票指标爬虫
使用 akshare 实现 A 股全市场股票主要财务指标获取
支持并行处理功能
"""

import akshare as ak
import pandas as pd
import time
import random
from datetime import datetime
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


class StockFinancialCrawler:
    def __init__(self):
        """初始化爬虫"""
        # 确保输出目录存在
        self.output_dir = "stock_data04"
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # 用于线程安全的打印
        self.print_lock = Lock()

    def fetch_all_stock_list(self):
        """
        使用 akshare 获取 A 股全市场股票列表
        """
        print("开始获取A股全市场股票列表...")
        try:
            # 获取A股股票列表
            stock_list = ak.stock_info_a_code_name()
            print(f"A股全市场股票列表获取完成，共获取{len(stock_list)}只股票")
            return stock_list
        except Exception as e:
            print(f"获取A股全市场股票列表失败: {e}")
            return pd.DataFrame()

    def fetch_stock_list_by_market(self, market_type):
        """
        根据市场类型获取股票列表
        :param market_type: 市场类型 ('sh': 上海, 'sz': 深圳, 'bj': 北京)
        """
        with self.print_lock:
            print(f"开始获取{market_type.upper()}股股票列表...")
        try:
            if market_type == 'sh':
                stock_list = ak.stock_info_sh_name_code()
                # 统一列名
                if '公司代码' in stock_list.columns and '公司简称' in stock_list.columns:
                    stock_list = stock_list.rename(columns={'公司代码': 'code', '公司简称': 'name'})
                    stock_list = stock_list[['code', 'name']]
                else:
                    with self.print_lock:
                        print("上海股票列表数据格式不正确")
                    return pd.DataFrame()
            elif market_type == 'sz':
                stock_list = ak.stock_info_sz_name_code(indicator="A股列表")
                # 统一列名
                if '证券代码' in stock_list.columns and '证券简称' in stock_list.columns:
                    stock_list = stock_list.rename(columns={'证券代码': 'code', '证券简称': 'name'})
                    stock_list = stock_list[['code', 'name']]
                else:
                    with self.print_lock:
                        print("深圳股票列表数据格式不正确")
                    return pd.DataFrame()
            elif market_type == 'bj':
                stock_list = ak.stock_info_bj_name_code()
                # 统一列名
                if '证券代码' in stock_list.columns and '证券简称' in stock_list.columns:
                    stock_list = stock_list.rename(columns={'证券代码': 'code', '证券简称': 'name'})
                    stock_list = stock_list[['code', 'name']]
                else:
                    with self.print_lock:
                        print("北京股票列表数据格式不正确")
                    return pd.DataFrame()
            else:
                with self.print_lock:
                    print(f"不支持的市场类型: {market_type}")
                return pd.DataFrame()

            with self.print_lock:
                print(f"{market_type.upper()}股股票列表获取完成，共获取{len(stock_list)}只股票")
            return stock_list
        except Exception as e:
            with self.print_lock:
                print(f"获取{market_type.upper()}股股票列表失败: {e}")
            return pd.DataFrame()

    def fetch_financial_summary_akshare(self, stock_code):
        """
        使用 akshare 获取单只股票的财务摘要数据
        """
        try:
            # 获取主要财务指标
            financial_data = {}

            # 尝试使用同花顺接口
            try:
                df = ak.stock_financial_abstract_ths(symbol=stock_code)
                if df is not None and not df.empty:
                    # 获取最新一期数据
                    latest_data = df.iloc[0]
                    for key, value in latest_data.items():
                        if key in ['基本每股收益', '每股净资产', '净资产收益率', '归属净利润同比增长(%)',
                                   '营业总收入同比增长(%)', '资产负债率(%)', '毛利率(%)', '净利率(%)']:
                            financial_data[key] = value if value is not None else 0
            except Exception:
                pass

            # 如果同花顺接口没有数据，尝试使用东财接口
            if not financial_data:
                try:
                    df = ak.stock_financial_abstract(symbol=stock_code)
                    if df is not None and not df.empty:
                        # 获取最新一期数据
                        latest_data = df.iloc[0]
                        for key, value in latest_data.items():
                            if key in ['每股收益', '每股净资产', '净资产收益率', '净利润增长率(%)',
                                       '营业收入增长率(%)', '资产负债比率(%)']:
                                financial_data[key] = value if value is not None else 0
                except Exception:
                    pass

            # 标准化字段名称
            standardized_data = {}
            field_mapping = {
                '基本每股收益': '每股收益',
                '归属净利润同比增长(%)': '净利润增长率(%)',
                '营业总收入同比增长(%)': '营业收入增长率(%)',
                '资产负债率(%)': '负债比率(%)',
                '净资产收益率': '净资产收益率(ROE)',
                '毛利率(%)': '毛利率(%)',
                '净利率(%)': '净利率(%)'
            }

            for key, value in financial_data.items():
                new_key = field_mapping.get(key, key)
                standardized_data[new_key] = value if value is not None else 0

            return standardized_data

        except Exception:
            return {}

    def fetch_detailed_financial_data_akshare(self, stock_code):
        """
        使用 akshare 获取详细的财务数据
        """
        financial_data = {}
        try:
            # 获取资产负债表数据
            try:
                balance_df = ak.stock_balance_sheet_by_report_em(symbol=stock_code)
                if balance_df is not None and not balance_df.empty:
                    # 获取最新一期数据
                    latest_balance = balance_df.iloc[0]
                    balance_fields = {
                        '总资产': '总资产',
                        '股东权益合计': '股东权益',
                        '负债合计': '负债总额'
                    }

                    for key, new_key in balance_fields.items():
                        if key in latest_balance:
                            value = latest_balance[key]
                            financial_data[new_key] = value if value is not None else 0
            except Exception:
                pass

            # 获取利润表数据
            try:
                income_df = ak.stock_profit_sheet_by_report_em(symbol=stock_code)
                if income_df is not None and not income_df.empty:
                    # 获取最新一期数据
                    latest_income = income_df.iloc[0]
                    income_fields = {
                        '营业总收入': '营业收入',
                        '净利润': '净利润'
                    }

                    for key, new_key in income_fields.items():
                        if key in latest_income:
                            value = latest_income[key]
                            financial_data[new_key] = value if value is not None else 0
            except Exception:
                pass

            # 获取现金流量表数据
            try:
                cashflow_df = ak.stock_cash_flow_sheet_by_report_em(symbol=stock_code)
                if cashflow_df is not None and not cashflow_df.empty:
                    # 获取最新一期数据
                    latest_cashflow = cashflow_df.iloc[0]
                    if '经营活动产生的现金流量净额' in latest_cashflow:
                        value = latest_cashflow['经营活动产生的现金流量净额']
                        financial_data['经营现金流'] = value if value is not None else 0
            except Exception:
                pass

            return financial_data

        except Exception:
            return financial_data

    def fetch_stock_info_akshare(self, stock_code):
        """
        使用 akshare 获取股票基本信息
        """
        try:
            # 获取实时行情数据
            stock_info = {}
            try:
                df = ak.stock_zh_a_spot_em()
                if df is not None and not df.empty:
                    # 筛选出对应股票代码的数据
                    stock_data = df[df['代码'] == stock_code]
                    if not stock_data.empty:
                        info_fields = {
                            '最新价': '最新价',
                            '涨跌幅': '涨跌幅(%)',
                            '成交量': '成交量',
                            '成交额': '成交额',
                            '振幅': '振幅(%)',
                            '换手率': '换手率(%)',
                            '市盈率-动态': '市盈率(动态)',
                            '量比': '量比',
                            '最高': '最高价',
                            '最低': '最低价',
                            '今开': '开盘价',
                            '昨收': '昨收价',
                            '市净率': '市净率',
                            '总市值': '总市值',
                            '流通市值': '流通市值'
                        }

                        for key, new_key in info_fields.items():
                            if key in stock_data.columns:
                                value = stock_data.iloc[0][key]
                                stock_info[new_key] = value if value is not None else 0
            except Exception:
                pass

            return stock_info
        except Exception:
            return {}

    def process_single_stock(self, stock, fetch_financial=True, fetch_info=True, index=0, total=0):
        """
        处理单只股票数据（用于并行处理）
        """
        stock_code = stock['股票代码']
        stock_name = stock['股票名称']

        with self.print_lock:
            print(f"  处理进度: {index+1}/{total} - {stock_code} {stock_name}")

        # 合并基础数据和财务数据
        complete_stock = stock.copy()

        if fetch_info:
            # 获取股票基本信息
            stock_info = self.fetch_stock_info_akshare(stock_code)
            complete_stock.update(stock_info)

        if fetch_financial:
            # 获取财务摘要
            financial_summary = self.fetch_financial_summary_akshare(stock_code)
            complete_stock.update(financial_summary)

            # 获取详细财务数据
            detailed_financial = self.fetch_detailed_financial_data_akshare(stock_code)
            complete_stock.update(detailed_financial)

        # 添加随机延时避免请求过于频繁
        time.sleep(random.uniform(0.1, 0.3))

        return complete_stock

    def crawl_all_market_data(self, fetch_financial=True, fetch_info=True, use_parallel=False, max_workers=10):
        """
        爬取A股全市场股票数据
        :param fetch_financial: 是否获取财务数据
        :param fetch_info: 是否获取基本信息
        :param use_parallel: 是否使用并行处理
        :param max_workers: 并行处理的最大线程数
        """
        print("\n开始爬取A股全市场数据...")

        # 1. 获取完整股票列表
        stock_list = self.fetch_all_stock_list()

        if stock_list.empty:
            print("未获取到股票数据")
            return []

        # 转换为列表格式
        stock_items = []
        for _, row in stock_list.iterrows():
            stock_items.append({
                '股票代码': row['code'],
                '股票名称': row['name']
            })

        complete_data = []
        total_count = len(stock_items)
        success_count = 0

        if use_parallel:
            print(f"使用并行处理，线程数: {max_workers}")
            # 使用并行处理
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_stock = {
                    executor.submit(self.process_single_stock, stock, fetch_financial, fetch_info, i, total_count): i
                    for i, stock in enumerate(stock_items)
                }

                # 收集结果
                for future in as_completed(future_to_stock):
                    try:
                        result = future.result()
                        complete_data.append(result)
                        success_count += 1
                    except Exception as e:
                        index = future_to_stock[future]
                        stock = stock_items[index]
                        print(f"处理股票 {stock['股票代码']} {stock['股票名称']} 时出错: {e}")
        else:
            print("使用串行处理")
            # 使用串行处理
            for i, stock in enumerate(stock_items):
                try:
                    result = self.process_single_stock(stock, fetch_financial, fetch_info, i, total_count)
                    complete_data.append(result)
                    success_count += 1
                except Exception as e:
                    print(f"处理股票 {stock['股票代码']} {stock['股票名称']} 时出错: {e}")

        print(f"A股全市场数据爬取完成，成功处理{success_count}/{total_count}只股票")
        return complete_data

    def crawl_market_data_by_type(self, market_type, fetch_financial=True, fetch_info=True, use_parallel=False, max_workers=10):
        """
        根据市场类型爬取股票数据
        :param market_type: 市场类型 ('sh': 上海, 'sz': 深圳, 'bj': 北京)
        :param fetch_financial: 是否获取财务数据
        :param fetch_info: 是否获取基本信息
        :param use_parallel: 是否使用并行处理
        :param max_workers: 并行处理的最大线程数
        """
        print(f"\n开始爬取{market_type.upper()}股数据...")

        # 1. 获取股票列表
        stock_list = self.fetch_stock_list_by_market(market_type)

        if stock_list.empty:
            print(f"{market_type.upper()}股未获取到股票数据")
            return []

        # 转换为列表格式
        stock_items = []
        for _, row in stock_list.iterrows():
            stock_items.append({
                '股票代码': row['code'],
                '股票名称': row['name']
            })

        complete_data = []
        total_count = len(stock_items)
        success_count = 0

        if use_parallel:
            print(f"使用并行处理，线程数: {max_workers}")
            # 使用并行处理
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_stock = {
                    executor.submit(self.process_single_stock, stock, fetch_financial, fetch_info, i, total_count): i
                    for i, stock in enumerate(stock_items)
                }

                # 收集结果
                for future in as_completed(future_to_stock):
                    try:
                        result = future.result()
                        complete_data.append(result)
                        success_count += 1
                    except Exception as e:
                        index = future_to_stock[future]
                        stock = stock_items[index]
                        with self.print_lock:
                            print(f"处理股票 {stock['股票代码']} {stock['股票名称']} 时出错: {e}")
        else:
            print("使用串行处理")
            # 使用串行处理
            for i, stock in enumerate(stock_items):
                try:
                    result = self.process_single_stock(stock, fetch_financial, fetch_info, i, total_count)
                    complete_data.append(result)
                    success_count += 1
                except Exception as e:
                    with self.print_lock:
                        print(f"处理股票 {stock['股票代码']} {stock['股票名称']} 时出错: {e}")

        print(f"{market_type.upper()}股数据爬取完成，成功处理{success_count}/{total_count}只股票")
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

    def run_all_market(self, fetch_financial=True, fetch_info=True, use_parallel=False, max_workers=10):
        """
        运行主程序 - 爬取全市场数据
        """
        print("A股全市场股票财务数据爬虫（基于 akshare）")
        print("=" * 50)
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # 爬取全市场数据
        all_data = self.crawl_all_market_data(fetch_financial, fetch_info, use_parallel, max_workers)

        # 保存数据
        if all_data:
            self.save_to_csv(all_data, "A股全市场")
            # 保存Excel文件
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(self.output_dir, f"A股全市场股票财务数据_{timestamp}.xlsx")
            try:
                df = pd.DataFrame(all_data)
                df.to_excel(filename, index=False, engine='openpyxl')
                print(f"Excel文件已保存至: {filename}")
            except Exception as e:
                print(f"保存Excel文件失败: {e}")

        # 打印统计信息
        print("\n" + "=" * 50)
        print("爬取完成统计:")
        print(f"  A股全市场: {len(all_data)} 只股票")

        print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return {"A股全市场": all_data}

    def run_by_market_type(self, fetch_financial=True, fetch_info=True, use_parallel=False, max_workers=10):
        """
        运行主程序 - 按市场类型爬取数据
        """
        print("A股股票财务数据爬虫（基于 akshare）")
        print("=" * 50)
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        market_types = ['sh', 'sz']  # 暂时不包含bj，因为可能数据格式不同
        market_names = {'sh': '上海', 'sz': '深圳', 'bj': '北京'}
        all_market_data = {}

        # 爬取各市场数据
        for market_type in market_types:
            market_data = self.crawl_market_data_by_type(market_type, fetch_financial, fetch_info, use_parallel, max_workers)
            market_name = f"{market_names[market_type]}股"
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
    如果输入的是简称，通过akshare模糊匹配第一条结果
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
            # 使用akshare搜索股票
            df = ak.stock_info_a_code_name()
            # 查找匹配的股票名称
            matched = df[df['name'].str.contains(p, na=False)]
            if not matched.empty:
                codes.append(matched.iloc[0]['code'])
        except Exception:
            pass
    return list(set(codes))


def main():
    crawler = StockFinancialCrawler()

    print("股票财务数据爬虫（基于 akshare）")
    print("1) A股全市场所有股票数据爬取（推荐）")
    print("2) 按市场分类爬取（上海、深圳）")
    print("3) 自定义股票查询")
    choice = input("请选择模式（1/2/3）：").strip()

    # 询问是否使用并行处理
    use_parallel = False
    max_workers = 10
    if choice in ["1", "2"]:
        parallel_choice = input("是否使用并行处理加速数据获取？(y/N): ").strip().lower()
        if parallel_choice in ['y', 'yes', '是']:
            use_parallel = True
            try:
                workers_input = input(f"请输入并行线程数（默认为{max_workers}，建议不超过20）: ").strip()
                if workers_input:
                    max_workers = int(workers_input)
            except ValueError:
                print(f"输入无效，使用默认线程数: {max_workers}")

    if choice == "3":
        raw = input("请输入股票代码或简称（多个用逗号分隔）：").strip()
        codes = sanitize_codes(raw)
        if not codes:
            print("未识别到有效股票，程序结束")
            return
        data = []
        for idx, code in enumerate(codes, 1):
            print(f"处理 {idx}/{len(codes)} - {code}")
            basic = {"股票代码": code, "股票名称": ""}
            # 获取股票名称
            try:
                df = ak.stock_info_a_code_name()
                name_row = df[df['code'] == code]
                if not name_row.empty:
                    basic["股票名称"] = name_row.iloc[0]['name']
            except Exception:
                pass

            # 获取数据
            basic.update(crawler.fetch_stock_info_akshare(code))
            basic.update(crawler.fetch_financial_summary_akshare(code))
            basic.update(crawler.fetch_detailed_financial_data_akshare(code))
            data.append(basic)

            # 添加延时
            time.sleep(random.uniform(0.3, 0.8))

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        # 确保data目录存在
        if not os.path.exists("data"):
            os.makedirs("data")
        pd.DataFrame(data).to_csv(f"data/自定义查询_{ts}.csv", index=False, encoding="utf_8_sig")
        print(f"结果已保存至 data/自定义查询_{ts}.csv")
        return

    if choice == "2":
        print("即将爬取上海、深圳股票市场的所有股票数据")
        print("注意：这可能需要较长时间，请确保网络连接稳定")
        confirm = input("\n是否开始爬取所有股票数据？(y/N): ").strip().lower()
        if confirm not in ['y', 'yes', '是']:
            print("已取消操作")
            return

        crawler.run_by_market_type(fetch_financial=True, fetch_info=True, use_parallel=use_parallel, max_workers=max_workers)
        return

    # 默认全市场爬取
    print("即将爬取A股全市场的所有股票数据")
    print("注意：这可能需要较长时间（可能几小时），请确保网络连接稳定")
    confirm = input("\n是否开始爬取所有股票数据？(y/N): ").strip().lower()
    if confirm not in ['y', 'yes', '是']:
        print("已取消操作")
        return

    crawler.run_all_market(fetch_financial=True, fetch_info=True, use_parallel=use_parallel, max_workers=max_workers)
    print("\nA股全市场数据爬取完成！")

if __name__ == "__main__":
    main()
