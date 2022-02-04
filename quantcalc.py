from random import seed
from turtle import st
import numpy as np
import pandas as pd
from datetime import datetime, date
import sqlalchemy as db


class SQLAlchemyConnector:
	
	def __init__(self):
		self.engine = db.create_engine('mysql+pymysql://root:passwd@localhost/db_name')
		self.connection = self.engine.connect()


class ProfitCalc(SQLAlchemyConnector):

	def __init__(self, term, codes):
		super().__init__()
		self.term = term
		self.codes = codes
		self.start_date = []
		self.end_date = []
		self.prices = []
		self.get_profit()


	def get_marketopen_date(self):
		query = f"SELECT Date FROM unique_date WHERE Date >= '{self.start_date}' AND "\
				f"Date <= '{self.end_date}'"
		code_df = pd.read_sql(query, con = self.engine)
		code_np = code_df.to_numpy()
		return (code_np[0], code_np[-1])


	def is_valid_date(self, start_date, end_date):
		r_start_date, r_end_date = self.get_marketopen_date()
		if (r_start_date == start_date and r_end_date == end_date):
			return (True)
		else:
			return (False)


	def rebalancing(self):
		if self.term < 12:
			[f"{date(2021, month, 31)}" if 12 - month % self.term else month for month in range(1, 12, -1)]
		elif self.term >= 12:
			[f"{date(year, month, 31)}" if self.term % 12 else year if 12 - month % self.term else month for year, month in zip(range(x), range(1, self.term + 1, -1)]

	def get_price(self, code, start_date, end_date):
		query = f"SELECT AdjClose, Date FROM adjclose WHERE Code = '{code}'"
		code_df = pd.read_sql(query, con = self.engine)
		code_np = code_df.to_numpy()
		code_np = code_np[(code_np[:, 1] >= start_date) & (code_np[:, 1] <= end_date)]
		start_date = code_np[0][1]
		end_date = code_np[-1][1]
		if self.is_valid_date(start_date, end_date):
			return (code_np[0][0], code_np[-1][0])
		return (False)


	def get_profit(self):
		self.start_date, self.end_date = self.rebalancing()
		for term in 
		for code in self.codes:
			self.prices.append(self.get_price(code, self.start_date, self.end_date))
		for code, price in zip(self.codes, self.prices):
			profit = False
			if price != False:
				profit = price[1] / price[0] * 100 - 100
			print(code, profit, "%")


if __name__ == "__main__":
	codes = ['005930', '395400', '259960', '026890']
	pc = ProfitCalc(12, codes)
