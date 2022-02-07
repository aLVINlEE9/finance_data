import FinanceDataReader as fdr
import pandas_datareader as pdr
from ..marcap import marcap_data
from query_manager import QueryManager
import pandas as pd
from datetime import datetime

class DataCollector(QueryManager):

	def __init__(self):
		super().__init__()



	def update_raw_price_info(self):
		raw_price_info_df = marcap_data('1995-05-02', '2021-12-31')
		cnt = 0
		for r in raw_price_info_df.itertuples():
			self.replace_raw_price_info_table(r)
			print('[{}] #{:04d} ({}) : {} rows > REPLACE INTO raw_price_info [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'),
																				cnt+1, r.Index, len(raw_price_info_df)))
			cnt+=1


	def update_cur_comp_info(self):
		df_krx = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download', header=0)[0]
		market = fdr.StockListing('KRX')
		length = len(df_krx)

		print(length)
		cnt = 0
		for r in df_krx.itertuples():
			code = str(r.종목코드).zfill(6)
			market_id = market[market["Symbol"] == code]["Market"].values[0]
			industry = self.str_exception_out(str(r.주요제품))
			query = f"REPLACE INTO cur_comp_info VALUES('{code}', '{market_id}', \
					'{r.회사명}', '{r.업종}', '{industry}', '{r.상장일}', \
					'{r.홈페이지}')"
			result_proxy = self.connection.execute(query)
			print(f"updating cur_comp_info {code} : {cnt}/{length}")
			result_proxy.close()
			cnt+=1


	def update_price_info(self):
		pass


if __name__ == "__main__":
	dc = DataCollector()
	dc.update_comp_info()