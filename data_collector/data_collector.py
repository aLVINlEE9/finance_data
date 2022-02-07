import FinanceDataReader as fdr
import pandas_datareader as pdr
from ..marcap import marcap_data
from query_manager import QueryManager
import pandas as pd
from datetime import datetime

class DataCollector(QueryManager):

	def __init__(self):
		super().__init__()


	def get_raw_price_info(self):
		raw_price_info_df = marcap_data('1995-05-02', '2021-12-31')
		total = len(raw_price_info_df)
		at = 0
		for r in raw_price_info_df.itertuples():
			self.replace_raw_price_info_table(r, at, total)
			at+=1


	def get_cur_comp_info(self):
		df_krx = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download', header=0)[0]
		total = len(df_krx)
		at = 0
		for r in df_krx.itertuples():
			self.replace_cur_comp_info_table(r, at, total)
			at+=1


	def update_price_info(self):
		pass


if __name__ == "__main__":
	dc = DataCollector()
	dc.update_comp_info()