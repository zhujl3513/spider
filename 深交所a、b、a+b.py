import requests
import pandas as pd
import time
import re
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional

class SZSECrawler:
    """
    深圳证券交易所数据抓取类 - 改进版
    基于个股页面 URL: https://www.szse.cn/certificate/individual/index.html?code=股票代码
    """

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://www.szse.cn/',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def get_stock_list(self) -> List[Dict[str, Any]]:
        """
        获取深交所所有股票列表
        """
        # 深交所官方数据接口
        url = "https://www.szse.cn/api/report/ShowReport"
        params = {
            'SHOWTYPE': 'xlsx',
            'CATALOGID': '1110',
            'TABKEY': 'tab1',
            'random': str(time.time())
        }

        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()

            # 保存临时文件
            temp_file = f"szse_stock_list_{int(time.time())}.xlsx"
            with open(temp_file, 'wb') as f:
                f.write(response.content)

            # 读取Excel文件
            df = pd.read_excel(temp_file, dtype={'A股代码': str, 'B股代码': str})

            # 转换为字典列表
            stock_list = df.to_dict('records')

            # 删除临时文件
            import os
            os.remove(temp_file)

            print(f"成功获取 {len(stock_list)} 只股票列表")
            return stock_list

        except Exception as e:
            print(f"获取股票列表失败: {e}")
            return []

    def get_stock_detail(self, stock_code: str, is_b_stock: bool = False) -> Dict[str, Any]:
        """
        获取单只股票的详细信息
        参数:
            stock_code: 股票代码
            is_b_stock: 是否为B股
        """
        # 构建个股页面URL
        url = f"https://www.szse.cn/certificate/individual/index.html?code={stock_code}"

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            # 使用BeautifulSoup解析HTML
            soup = BeautifulSoup(response.text, 'html.parser')

            # 提取股票基本信息
            stock_info = self._extract_stock_info(soup, stock_code)

            # 提取公司基本信息
            company_info = self._extract_company_info(soup)

            # 提取财务数据
            financial_info = self._extract_financial_info(soup)

            # 合并所有信息
            result = {
                '股票代码': stock_code,
                **stock_info,
                **company_info,
                **financial_info
            }

            return result

        except Exception as e:
            print(f"获取股票 {stock_code} 详情失败: {e}")
            return {}

    def _extract_stock_info(self, soup: BeautifulSoup, stock_code: str) -> Dict[str, Any]:
        """
        提取股票基本信息
        """
        info = {}

        try:
            # 查找股票名称
            name_element = soup.find('div', class_='company-name')
            if name_element:
                info['股票名称'] = name_element.get_text(strip=True)

            # 查找股票代码
            code_element = soup.find('div', class_='company-code')
            if code_element:
                info['股票代码'] = code_element.get_text(strip=True)

            # 查找股价信息
            price_element = soup.find('div', class_='price')
            if price_element:
                info['当前价格'] = price_element.get_text(strip=True)

            # 查找涨跌幅
            change_element = soup.find('div', class_='change')
            if change_element:
                info['涨跌幅'] = change_element.get_text(strip=True)

            # 查找其他交易信息
            trade_info = soup.find('div', class_='trade-info')
            if trade_info:
                rows = trade_info.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) == 2:
                        key = cols[0].get_text(strip=True).replace('：', '')
                        value = cols[1].get_text(strip=True)
                        info[key] = value

        except Exception as e:
            print(f"提取股票信息失败: {e}")

        return info

    def _extract_company_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        提取公司基本信息
        """
        info = {}

        try:
            # 查找公司基本信息区域
            company_info = soup.find('div', class_='company-info')
            if company_info:
                rows = company_info.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) == 2:
                        key = cols[0].get_text(strip=True).replace('：', '')
                        value = cols[1].get_text(strip=True)
                        info[key] = value

        except Exception as e:
            print(f"提取公司信息失败: {e}")

        return info

    def _extract_financial_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        提取财务数据
        """
        info = {}

        try:
            # 查找财务数据区域
            financial_info = soup.find('div', class_='financial-info')
            if financial_info:
                rows = financial_info.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 2:
                        key = cols[0].get_text(strip=True).replace('：', '')
                        value = cols[1].get_text(strip=True)
                        info[key] = value

            # 如果没有找到特定class，尝试查找表格
            tables = soup.find_all('table')
            for table in tables:
                caption = table.find('caption')
                if caption and '财务' in caption.get_text():
                    rows = table.find_all('tr')
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) >= 2:
                            key = cols[0].get_text(strip=True).replace('：', '')
                            value = cols[1].get_text(strip=True)
                            info[key] = value

        except Exception as e:
            print(f"提取财务信息失败: {e}")

        return info

    # def crawl_all_stocks(self, output_file: str = "szse_all_stocks.csv", max_stocks: int = 50):
    def crawl_all_stocks(self, output_file: str = "szse_all_stocks.csv", max_stocks: int = 5000):
        """
        爬取所有股票数据
        参数:
            output_file: 输出文件名
            max_stocks: 最大爬取数量（用于测试，设为None则爬取所有）
        """
        print("开始获取股票列表...")
        stock_list = self.get_stock_list()

        if not stock_list:
            print("未获取到股票列表，程序结束")
            return

        # 限制爬取数量（用于测试）
        if max_stocks and max_stocks < len(stock_list):
            stock_list = stock_list[:max_stocks]

        print(f"开始爬取 {len(stock_list)} 只股票数据...")

        all_data = []
        success_count = 0
        fail_count = 0

        for i, stock in enumerate(stock_list):
            # 获取A股代码
            stock_code = stock.get('A股代码', '')
            if not stock_code:
                # 如果没有A股代码，尝试获取B股代码
                stock_code = stock.get('B股代码', '')
                if not stock_code:
                    continue

            stock_name = stock.get('A股简称', '') or stock.get('B股简称', '')

            print(f"正在处理第 {i+1}/{len(stock_list)} 只股票: {stock_name}({stock_code})")

            # 获取详细信息
            detail_info = self.get_stock_detail(stock_code)

            if detail_info:
                # 合并基础信息和详细信息
                merged_data = {**stock, **detail_info}
                all_data.append(merged_data)
                success_count += 1
                print(f"√ 成功获取 {stock_name} 的数据")
            else:
                fail_count += 1
                print(f"× 获取 {stock_name} 数据失败")

            # 添加延时，避免请求过于频繁
            time.sleep(2)

        # 保存数据到CSV文件
        if all_data:
            df = pd.DataFrame(all_data)
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"数据已保存到 {output_file}")
            print(f"成功: {success_count}, 失败: {fail_count}")
        else:
            print("未获取到任何有效数据")

        return all_data

# 使用示例
if __name__ == "__main__":
    crawler = SZSECrawler()

    # 爬取所有股票数据（限制前50只用于测试）
    # all_data = crawler.crawl_all_stocks("szse_all_stocks.csv", max_stocks=50)
    all_data = crawler.crawl_all_stocks("szse_all_stocks.csv")


    # 如果要爬取所有股票，去掉max_stocks参数
    # all_data = crawler.crawl_all_stocks("szse_all_stocks.csv")

    print(f"\n数据爬取完成! 共获取 {len(all_data)} 只股票的数据")