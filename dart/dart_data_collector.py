import pandas as pd
import numpy as np
from connect import SQLAlchemyConnector
import dart_fss as dart
from dart_fss import get_corp_list, fs

class DartDataCollecter(SQLAlchemyConnector):
	def __init__(self):
		super().__init__()
		self.codes = None

		self.get_authorize()
		self.create_table()
		self.get_code()
		self.len = len(self.codes)
		

	def get_authorize(self):
		api_key = 'api_key'
		dart.set_api_key(api_key=api_key)


	def create_table(self):
		query_1 = """
			CREATE TABLE IF NOT EXISTS dart_crpcode ( 
				corp_code VARCHAR(20),
				corp_name VARCHAR(20),
				stock_code VARCHAR(20),
				modify_date VARCHAR(20),
				sector VARCHAR(200),
				product VARCHAR(200),
				corp_cls VARCHAR(20),
				PRIMARY KEY (corp_code, stock_code))
			"""
		result_proxy = self.connection.execute(query_1)
		result_proxy.close()
		

	def get_code(self):
		query = f"SELECT * FROM unique_code"
		df = pd.read_sql(query, con = self.engine)
		code_np = df.to_numpy()
		self.codes = code_np


	def apostrophe_out(self, str):
		str = str.replace("'", " ")
		str = str.replace("`", " ")
		str = str.replace("%", ".pct")
		return (str)


	def get_crp_code(self):
		query = f"SELECT corp_code,stock_code FROM dart_crpcode"
		df = pd.read_sql(query, con = self.engine)
		corp_np = df.to_numpy()
		return (corp_np)




	def update_crp_code(self):
		cnt = 0
		for code in self.codes:
			try:
				crp_list = get_corp_list()
				crp = crp_list.find_by_stock_code(f'{code.item()}')
				crp_list = list(crp.to_dict().values())

				query = f"REPLACE INTO dart_crpcode VALUES ('{crp_list[0]}', '{crp_list[1]}', \
						'{crp_list[2]}', '{crp_list[3]}', '{crp_list[4]}', \
						'{self.apostrophe_out(crp_list[5])}', '{crp_list[6]}')"
				result_proxy = self.connection.execute(query)
				# print(f"updating crp_code {code} : {cnt}/{code_len}")
				result_proxy.close()
			except Exception as e:
				print(f"{e} : {code} {cnt}/{self.len}")

			cnt += 1


	def update_financial_statement(self):
		cnt = 0
		for code in self.get_crp_code():
			pass
				


if __name__ == "__main__":
	udcl = DartDataCollecter()
	udcl.update_crp_code()

