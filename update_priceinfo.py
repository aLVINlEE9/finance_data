import pandas as pd
from data_collector import SQLAlchemyConnector
from pandas_datareader import data

class Update_priceinfo(SQLAlchemyConnector):
	def __init__(self):
		super().__init__()
		self.code_list = self.get_code()
		self.create_table()


	def create_table(self):
		print("CREATE TABLE price_info AS SELECT * FROM raw_price_info")
		query_1 = """
			CREATE TABLE IF NOT EXISTS adjclose ( 
				Date DATE,
				Code VARCHAR(20),
				AdjClose FLOAT(20),
				PRIMARY KEY (Date, Code))
			"""
		result_proxy = self.connection.execute(query_1)
		result_proxy.close()
		print("CREATED!")


	def get_code(self):
		query_1 = f"SELECT Code FROM unique_code ORDER BY Code"
		code_df = pd.read_sql(query_1, con = self.engine)
		code_df = code_df["Code"]
		code_list = code_df.tolist()
		return (code_list)


	def put_adjclose(self, adjclose, code):
		length = len(adjclose)
		cnt = 0
		for date, adj in adjclose.items():
			query = f"INSERT INTO adjclose VALUES('{date}', '{code}', {adj})"
			result_proxy = self.connection.execute(query)
			print(f"ㄴ {code} : {date} at {cnt} / {length}")
			result_proxy.close()
			cnt += 1
		

	def update(self):
		length = len(self.code_list)
		cnt = 0
		cnt_ex = 0
		for code in self.code_list:
			print(f"=====updating : {code} at {cnt} / {length} + error {cnt_ex}=====")
			try:
				df = data.DataReader(f'{code}.KQ','yahoo','2000-01-04','2022-02-02')
				adjclose = df["Adj Close"]
				self.put_adjclose(adjclose, code)
				cnt += 1
			except Exception as e:
				print(f"{code} : {e}")
				cnt_ex += 1

if __name__ == "__main__":
	"""kospi adj close data"""
	udpi = Update_priceinfo()
	udpi.update()