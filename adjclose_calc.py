from tkinter import Y
from pandas_datareader import get_markets_iex
from data_collector import SQLAlchemyConnector
import sqlalchemy as db
import pandas as pd
from data_collector import SQLAlchemyConnector

class AdjClose_Calc(SQLAlchemyConnector):

	def __init__(self):
		super().__init__()
		self.create_table()
		self.buff_date = 0
		self.rate = 1

	def create_table(self):
		print("CREATE TABLE price_info AS SELECT * FROM raw_price_info")
		query_1 = """
			CREATE TABLE price_info AS SELECT * FROM raw_price_info
			"""
		result_proxy = self.connection.execute(query_1)
		result_proxy.close()
		print("CREATED!")
		print("ALTER TABLE price_info ADD COLUMN AdjClose FLOAT(20) AFTER Close")
		query_2 = """
			ALTER TABLE price_info ADD COLUMN AdjClose FLOAT(20) AFTER Close
			"""
		result_proxy = self.connection.execute(query_2)
		result_proxy.close()
		print("ADDED!")


	def get_market_open(self):
		query_1 = f"SELECT Code FROM unique_code ORDER BY Code"
		query_2 = f"SELECT Code, Date FROM market_open_info ORDER BY Code"
		query_3 = f"SELECT Date, Code, Close, Stocks FROM price_info ORDER BY Code"
		code_df = pd.read_sql(query_1, con = self.engine)
		market_open_df = pd.read_sql(query_2, con = self.engine)
		info_df = pd.read_sql(query_3, con = self.engine)
		return (code_df, market_open_df, info_df)


	def update_adj(self, code, date, now_close):
		query = f"UPDATE price_info SET AdjClose = {now_close} "\
			f"WHERE Date = '{date}' AND Code = '{code}'"
		result_proxy = self.connection.execute(query)
		result_proxy.close()


	def check_adj(self, code, date, info_df):
		if (self.buff_date == 0):
			self.buff_date = date
			return
		else:
			print(date, info_df["Date"][0])
			now = info_df[(info_df["Date"] == date) & (info_df["Code"] == code)]
			next = info_df[(info_df["Date"] == self.buff_date) & (info_df["Code"] == code)]
			now_stock = now["Stocks"].values[0]
			next_stock = next["Stocks"].values[0]
			now_close = now["Close"].values[0]
			rate = now_stock / next_stock
			if (rate != 1):
				self.rate = rate
			now_close *= self.rate
			self.update_adj(code, date, now_close)
			self.buff_date = date
				

	def get_date(self, code, market_open_df):
		df = market_open_df[market_open_df["Code"] == code]
		df = df.sort_values(by='Date', ascending=False)
		df = df["Date"]
		date_list = df.tolist()
		return (date_list)


	def calc(self):
		code_df, market_open_df, info_df = self.get_market_open()
		code_len = len(code_df)
		cnt_1 = 1
		for n in code_df.itertuples():
			date_list = self.get_date(n.Code, market_open_df)
			date_len = len(date_list)
			cnt_2 = 1
			for date in date_list:
				self.check_adj(n.Code, date, info_df)
				print(f"CALC: Code at [{cnt_1} / {code_len}] Date at {cnt_2} / {date_len}", end = '\r')
				cnt_2 += 1
			self.buff_date = 0
			self.rate = 1
			cnt_1 += 1


if __name__ == "__main__":
	"""takes long time!!"""
	# adj = AdjClose_Calc()
	# adj.calc()