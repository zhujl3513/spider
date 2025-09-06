import requests
import pandas as pd
import time
import re
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
import os

class SZSEDetailedCrawler:
    """
    深圳证券交易所详细数据抓取类
    基于个股页面: https://www.szse.cn/certificate/individual/index.html?code=股票代码
    主板A股和B股放在一起处理，创业板单独处理
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
        self.base_url = "https://www.szse.cn/certificate/individual/index.html?code="

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
            os.remove(temp_file)

            print(f"成功获取 {len(stock_list)} 只股票列表")
            return stock_list

        except Exception as e:
            print(f"获取股票列表失败: {e}")
            return []

    def classify_stocks(self, stock_list: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        将股票分类为主板（A股和B股）和创业板
        根据股票代码前缀进行分类：
          - 创业板：股票代码以3开头
          - 主板：股票代码以0或2开头
        """
        classified = {
            "main_board": [],  # 主板（包含A股和B股）
            "gem": []          # 创业板
        }

        for stock in stock_list:
            a_code = stock.get('A股代码', '')
            b_code = stock.get('B股代码', '')

            # 判断股票类型
            if a_code and a_code.startswith('3'):
                # 创业板（A股代码以3开头）
                classified["gem"].append(stock)
            elif a_code or b_code:
                # 主板（A股代码以0或2开头，或者只有B股代码）
                classified["main_board"].append(stock)

        print(f"分类完成: 主板 {len(classified['main_board'])} 只, "
              f"创业板 {len(classified['gem'])} 只")

        return classified

    def get_stock_detail(self, stock_code: str, stock_name: str = "") -> Dict[str, Any]:
        """
        获取单只股票的详细信息
        参数:
            stock_code: 股票代码
            stock_name: 股票名称（用于日志输出）
        """
        # 构建个股页面URL
        url = f"{self.base_url}{stock_code}"

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            # 使用BeautifulSoup解析HTML
            soup = BeautifulSoup(response.text, 'html.parser')

            # 提取基本信息
            basic_info = self._extract_basic_info(soup, stock_code)

            # 提取财务数据
            financial_info = self._extract_financial_info(soup)

            # 提取公司信息
            company_info = self._extract_company_info(soup)

            # 提取交易信息
            trading_info = self._extract_trading_info(soup)

            # 合并所有信息
            result = {
                '股票代码': stock_code,
                '股票名称': stock_name,
                '个股链接': url,
                **basic_info,
                **financial_info,
                **company_info,
                **trading_info
            }

            return result

        except Exception as e:
            print(f"获取股票 {stock_code}({stock_name}) 详情失败: {e}")
            return {}

    def _extract_basic_info(self, soup: BeautifulSoup, stock_code: str) -> Dict[str, Any]:
        """
        提取基本信息
        """
        info = {}

        try:
            # 尝试查找股票名称
            title = soup.find('title')
            if title:
                info['页面标题'] = title.get_text(strip=True)

            # 尝试查找股票基本信息
            info_elements = soup.find_all(['div', 'table'], class_=re.compile(r'info|data|stock|company', re.I))

            for element in info_elements:
                # 提取键值对信息
                rows = element.find_all(['tr', 'div', 'p'], class_=re.compile(r'row|item|field', re.I))
                for row in rows:
                    cols = row.find_all(['td', 'div', 'span'])
                    if len(cols) >= 2:
                        key = cols[0].get_text(strip=True).replace('：', '').replace(':', '')
                        value = cols[1].get_text(strip=True)
                        if key and value:
                            info[key] = value

            # 如果没有找到特定元素，尝试通用提取
            if len(info) < 3:  # 如果提取的信息太少
                # 尝试查找所有可能的键值对
                all_text = soup.get_text()
                # 使用正则表达式提取常见信息
                patterns = {
                    '最新价': r'最新价[：:]\s*([\d.,]+)',
                    '涨跌幅': r'涨跌幅[：:]\s*([-\d.,%]+)',
                    '成交量': r'成交量[：:]\s*([\d.,]+)',
                    '成交额': r'成交额[：:]\s*([\d.,]+)',
                    '市盈率': r'市盈率[：:]\s*([\d.,]+)',
                    '总市值': r'总市值[：:]\s*([\d.,]+)',
                }

                for key, pattern in patterns.items():
                    match = re.search(pattern, all_text)
                    if match:
                        info[key] = match.group(1)

        except Exception as e:
            print(f"提取基本信息失败: {e}")

        return info

    def _extract_financial_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        提取财务数据
        """
        info = {}

        try:
            # 查找所有表格
            tables = soup.find_all('table')

            for table in tables:
                # 检查是否是财务数据表
                caption = table.find('caption')
                text = table.get_text()

                if caption and any(word in caption.get_text() for word in ['财务', '利润', '资产', '负债', '收入', '现金流']):
                    # 提取表格数据
                    rows = table.find_all('tr')
                    for row in rows:
                        cols = row.find_all(['td', 'th'])
                        if len(cols) >= 2:
                            key = cols[0].get_text(strip=True).replace('：', '').replace(':', '')
                            value = cols[1].get_text(strip=True)
                            if key and value and not key.isdigit():
                                info[key] = value

        except Exception as e:
            print(f"提取财务信息失败: {e}")

        return info

    def _extract_company_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        提取公司信息
        """
        info = {}

        try:
            # 查找所有包含公司信息的元素
            elements = soup.find_all(['div', 'section'], class_=re.compile(r'company|profile|intro|about', re.I))

            for element in elements:
                # 提取公司信息
                items = element.find_all(['p', 'div', 'li'])
                for item in items:
                    text = item.get_text(strip=True)
                    if '：' in text:
                        parts = text.split('：', 1)
                        if len(parts) == 2:
                            key = parts[0].strip()
                            value = parts[1].strip()
                            info[key] = value

            # 如果没有找到特定元素，尝试从页面文本中提取
            if not info:
                all_text = soup.get_text()
                # 使用正则表达式提取常见公司信息
                patterns = {
                    '公司全称': r'公司全称[：:]\s*([^\n]+)',
                    '英文名称': r'英文名称[：:]\s*([^\n]+)',
                    '注册地址': r'注册地址[：:]\s*([^\n]+)',
                    '办公地址': r'办公地址[：:]\s*([^\n]+)',
                    '法定代表人': r'法定代表人[：:]\s*([^\n]+)',
                    '董事会秘书': r'董事会秘书[：:]\s*([^\n]+)',
                }

                for key, pattern in patterns.items():
                    match = re.search(pattern, all_text)
                    if match:
                        info[key] = match.group(1).strip()

        except Exception as e:
            print(f"提取公司信息失败: {e}")

        return info

    def _extract_trading_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        提取交易信息
        """
        info = {}

        try:
            # 查找交易信息相关的元素
            elements = soup.find_all(['div', 'table'], class_=re.compile(r'trading|market|quote|price', re.I))

            for element in elements:
                # 提取交易信息
                rows = element.find_all(['tr', 'div'])
                for row in rows:
                    cols = row.find_all(['td', 'div', 'span'])
                    if len(cols) >= 2:
                        key = cols[0].get_text(strip=True).replace('：', '').replace(':', '')
                        value = cols[1].get_text(strip=True)
                        if key and value:
                            info[key] = value

        except Exception as e:
            print(f"提取交易信息失败: {e}")

        return info

    def crawl_by_category(self, stocks: List[Dict[str, Any]], category_name: str,
                          output_file: str, max_stocks: int = None) -> List[Dict[str, Any]]:
        """
        按类别爬取股票数据
        """
        if not stocks:
            print(f"没有{category_name}股票数据，跳过")
            return []

        # 限制爬取数量（用于测试）
        if max_stocks and max_stocks < len(stocks):
            stocks = stocks[:max_stocks]

        print(f"开始爬取 {len(stocks)} 只{category_name}股票数据...")

        all_data = []
        success_count = 0
        fail_count = 0

        for i, stock in enumerate(stocks):
            # 获取股票代码和名称
            stock_code = stock.get('A股代码', '') or stock.get('B股代码', '')
            stock_name = stock.get('A股简称', '') or stock.get('B股简称', '')

            if not stock_code:
                continue

            print(f"正在处理{category_name}第 {i+1}/{len(stocks)} 只股票: {stock_name}({stock_code})")

            # 获取详细信息
            detail_info = self.get_stock_detail(stock_code, stock_name)

            if detail_info:
                # 合并基础信息和详细信息
                merged_data = {**stock, **detail_info}
                merged_data['板块类型'] = category_name
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
            print(f"{category_name}数据已保存到 {output_file}")
            print(f"成功: {success_count}, 失败: {fail_count}")
        else:
            print(f"未获取到任何{category_name}有效数据")

        return all_data

    def crawl_all_categories(self, output_dir: str = "szse_detailed_data", max_per_category: int = None):
        """
        爬取所有类别的数据
        """
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)

        # 获取股票列表
        print("开始获取股票列表...")
        stock_list = self.get_stock_list()

        if not stock_list:
            print("未获取到股票列表，程序结束")
            return {}

        # 分类股票
        print("开始分类股票...")
        classified_stocks = self.classify_stocks(stock_list)

        # 定义类别信息
        categories = [
            ("main_board", "主板", "szse_main_board_detailed.csv"),
            ("gem", "创业板", "szse_gem_detailed.csv")
        ]

        all_data = {}

        for category_key, category_name, output_file in categories:
            stocks = classified_stocks.get(category_key, [])
            output_path = os.path.join(output_dir, output_file)

            data = self.crawl_by_category(
                stocks, category_name, output_path, max_per_category
            )
            all_data[category_key] = data

            # 类别间添加较长延时
            time.sleep(3)

        # 生成汇总报告
        self.generate_summary_report(all_data, output_dir)

        return all_data

    def generate_summary_report(self, all_data: Dict[str, List], output_dir: str):
        """
        生成汇总报告
        """
        report_data = []

        category_names = {
            "main_board": "主板",
            "gem": "创业板"
        }

        for category_key, data in all_data.items():
            category_name = category_names.get(category_key, category_key)
            report_data.append({
                '板块类型': category_name,
                '股票数量': len(data),
                '数据文件': f"szse_{category_key}_detailed.csv"
            })

        # 保存汇总报告
        report_df = pd.DataFrame(report_data)
        report_file = os.path.join(output_dir, "szse_detailed_summary.csv")
        report_df.to_csv(report_file, index=False, encoding='utf-8-sig')

        print("\n" + "="*50)
        print("深交所各板块详细数据爬取汇总")
        print("="*50)
        for item in report_data:
            print(f"{item['板块类型']}: {item['股票数量']} 只股票")
        print("="*50)
        print(f"汇总报告已保存到: {report_file}")

if __name__ == "__main__":
    crawler = SZSEDetailedCrawler()

    # 爬取所有类别的数据（限制每类5只用于测试）
    print("开始爬取深交所所有板块详细数据...")
    # all_data = crawler.crawl_all_categories("szse_detailed_data", max_per_category=5)

    # 如果要爬取所有股票，去掉max_per_category参数
    all_data = crawler.crawl_all_categories("szse_detailed_data")

    print(f"\n数据爬取完成! 共获取 {sum(len(data) for data in all_data.values())} 只股票的数据")