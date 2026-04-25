import baostock as bs
import pandas as pd

print("正在连接 Baostock 量化数据中心...")
# 登录系统（免费且匿名）
lg = bs.login()
if lg.error_code == '0':
    print("✅ 服务器连接成功！完全没有加密阻拦！")
else:
    print(f"❌ 连接失败: {lg.error_msg}")

print("\n开始获取 平安银行(sz.000001) 的日线数据...")

# baostock 要求的股票代码带有 sh 或 sz 前缀
# adjustflag="2" 代表前复权
rs = bs.query_history_k_data_plus("sz.000001",
    "date,code,open,high,low,close,volume,amount,turn,pctChg",
    start_date='2025-10-01', end_date='2026-04-05',
    frequency="d", adjustflag="2")

data_list = []
while (rs.error_code == '0') & rs.next():
    # 获取一条记录，将记录合并在一起
    data_list.append(rs.get_row_data())

# 转换成 pandas 的 DataFrame
result = pd.DataFrame(data_list, columns=rs.fields)

if not result.empty:
    print(f"\n✅ 完美获取！共拿到 {len(result)} 条数据。")
    print(result.head(3))
else:
    print("返回数据为空，请检查日期或代码。")

# 登出系统
bs.logout()