# 股票财务数据爬虫 - A股量化数据收集工具QuantDataCollector

## 项目简介

QuantDataCollector 是一个功能强大的A股量化数据收集工具，支持从多个数据源（Baostock、东方财富、同花顺）获取A股市场数据和批量获取与自定义股票查询。该工具专门设计用于收集主板、创业板和科创板的财务指标数据，并提供丰富的数据处理和格式化功能，同时也具有高效的数据采集、处理和保存功能。

## 功能特性

- 📊 **多数据源支持**：自动切换Baostock、东方财富、同花顺等多个数据源
- 🎯 **全面财务指标**：收集市盈率、市净率、每股收益、营业总收入等20+关键财务指标
- 📈 **板块分类**：自动识别主板、创业板、科创板股票
- ⚡ **智能数据处理**：自动计算缺失指标，确保数据完整性
- 💾 **数据导出**：支持CSV格式导出，方便后续分析
- 🖥️ **交互式界面**：提供命令行交互菜单，操作简便
- 🔄 **容错机制**：自动重试和故障转移，提高数据获取成功率

## 环境要求

### 系统要求
- Windows 10/11 或 Linux (Ubuntu/CentOS等)
- Python 3.13.5

### Python依赖
- baostock
- pandas
- numpy
- requests
- beautifulsoup4
- tqdm

## 安装步骤

### 1. 安装Python 3.13.5

#### Windows系统
1. 访问 [Python官网](https://www.python.org/downloads/)
2. 下载Python 3.13.5 Windows安装包
3. 运行安装程序，勾选"Add Python to PATH"选项
4. 完成安装

#### Linux系统 (Ubuntu/Debian)
```bash
# 添加deadsnakes PPA (适用于Ubuntu/Debian)
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update

# 安装Python 3.13.5
sudo apt install python3.13 python3.13-venv python3.13-dev
```

#### Linux系统 (CentOS/RHEL)
```bash
# 安装开发工具
sudo yum groupinstall "Development Tools"
sudo yum install openssl-devel bzip2-devel libffi-devel

# 下载并编译Python 3.13.5
wget https://www.python.org/ftp/python/3.13.5/Python-3.13.5.tgz
tar xzf Python-3.13.5.tgz
cd Python-3.13.5
./configure --enable-optimizations
make -j 8
sudo make altinstall
```

### 2. 创建虚拟环境 (推荐)

#### Windows
```cmd
python -m venv quant_env
quant_env\Scripts\activate
```

#### Linux
```bash
python3.13 -m venv quant_env
source quant_env/bin/activate
```

### 3. 安装依赖包

```bash
pip install baostock pandas numpy requests beautifulsoup4 tqdm
```

### 4. 下载代码

将提供的 `aqu142.py` 文件保存到您的项目目录中。

## 使用方法

### 基本使用

1. 激活虚拟环境 (如果已创建)
2. 运行程序：
   ```bash
   python aqu142.py
   ```

### 功能菜单说明

程序启动后，会显示以下功能菜单：

1. **按板块分别收集数据**：分别获取主板、创业板、科创板的数据并保存为单独文件
2. **收集所有股票合并数据**：获取所有股票数据并合并为一个文件
3. **快速测试模式**：仅处理少量股票进行测试
4. **搜索并获取特定股票数据**：根据关键词搜索股票并获取数据
5. **自定义股票列表数据获取**：手动输入股票代码列表获取数据
6. **根据股票代码列表获取数据**：输入逗号分隔的股票代码获取数据
0. **退出**：退出程序

### 数据字段说明

程序收集的财务指标包括：

1. 市盈率(TTM)
2. 市净率(最新)
3. 每股收益(计算)
4. 每股净资产(计算)
5. 营业总收入
6. 总营收同比
7. 归母净利润
8. 归母净利同比
9. 扣非净利润
10. 扣非净利同比
11. 毛利率
12. 净利率
13. 资产负债率
14. 净资产收益率
15. 商誉净资产比
16. 质押总股本比
17. 股息率
18. 股利支付率(静)

## 配置选项

在代码中可以通过修改 `QuantDataCollector` 类的初始化参数来自定义行为：

```python
# 创建数据收集器实例
collector = QuantDataCollector(
    request_delay=0.2,      # 请求间隔时间（秒）
    data_source='baostock'  # 首选数据源
)
```

## 数据源说明

1. **Baostock** (主要数据源)：免费、稳定的A股数据接口
2. **东方财富** (备用数据源)：国内知名财经网站的数据接口
3. **同花顺** (备用数据源)：另一知名财经数据提供商

程序会自动在数据源之间切换，确保数据获取的成功率。

## 常见问题

### 1. 数据获取失败

如果遇到数据获取失败的情况，程序会自动尝试切换数据源。如果所有数据源都失败，程序会使用合理的默认值填充缺失数据。

### 2. 网络连接问题

确保您的网络连接正常，能够访问Baostock、东方财富等同花顺等网站。

### 3. 依赖安装失败

如果安装依赖时遇到问题，可以尝试以下解决方案：

- 使用国内镜像源：`pip install -i https://pypi.tuna.tsinghua.edu.cn/simple package_name`
- 更新pip：`pip install --upgrade pip`
- 使用conda环境管理工具

### 4. 内存不足

处理大量股票数据时可能会占用较多内存。如果遇到内存不足的问题，可以：

- 使用 `max_stocks_per_board` 或 `max_total_stocks` 参数限制处理数量
- 增加系统虚拟内存
- 分批处理数据

## 性能优化建议

1. **调整请求延迟**：根据网络状况调整 `request_delay` 参数
2. **限制处理数量**：使用 `max_stocks_per_board` 参数限制每个板块的处理数量
3. **使用缓存**：程序会自动缓存股票基本信息，减少重复查询
4. **分批处理**：对于大量数据，可以分多次运行程序处理不同批次的股票

## 输出文件

程序会生成以下格式的CSV文件：

- `主板_财务指标_YYYYMMDD_HHMMSS.csv`
- `创业板_财务指标_YYYYMMDD_HHMMSS.csv`
- `科创板_财务指标_YYYYMMDD_HHMMSS.csv`
- `合并_全部板块_财务指标_YYYYMMDD_HHMMSS.csv`
- `全部股票_财务指标_YYYYMMDD_HHMMSS.csv`

## 注意事项

1. 数据仅供参考，不构成投资建议
2. 财务数据可能有延迟，非实时数据
3. 部分指标为计算值，可能与官方数据有差异
4. 使用过程中请遵守各数据源的使用条款

## 更新日志

### v1.0.0 (2025-08-31)
- 初始版本发布
- 支持多数据源自动切换
- 实现20+财务指标收集
- 提供交互式命令行界面

## 技术支持

如有问题或建议，请联系开发团队或提交GitHub Issue。

## 许可证

本项目采用MIT许可证，详情请参阅LICENSE文件。
