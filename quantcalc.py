from random import seed
from turtle import st
import matplotlib.pyplot as plt 
import numpy as np
import pandas as pd
from statistics import geometric_mean
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import sqlalchemy as db


class SQLAlchemyConnector:
	
	def __init__(self):
		self.engine = db.create_engine('mysql+pymysql://root:passwd@localhost/db_name')
		self.connection = self.engine.connect()


class ProfitCalc(SQLAlchemyConnector):

	def __init__(self, end_date, start_date, term, codes):
		super().__init__()
		self.end_date = end_date
		self.start_date = start_date
		self.term = term
		self.codes = codes
		self.ref_date = []
		self.prices = []
		self.mean = []
		self.get_profit()


	def get_marketopen_date(self, start_date, end_date):
		query = f"SELECT Date FROM unique_date WHERE Date >= '{start_date}' AND "\
				f"Date <= '{end_date}'"
		code_df = pd.read_sql(query, con = self.engine)
		code_np = code_df.to_numpy()
		return (code_np[0], code_np[-1])


	def is_valid_date(self, start_date, end_date, d_start_date, d_end_date):
		r_start_date, r_end_date = self.get_marketopen_date(start_date, end_date)
		if (r_start_date == d_start_date and r_end_date == d_end_date):
			return (True)
		else:
			return (False)


	def rebalancing(self):
		num_months = (self.end_date.year - self.start_date.year) * 12 + (self.end_date.month - self.start_date.month)
		n_month = self.end_date.month
		while(self.start_date < self.end_date):
			temp = self.end_date
			self.end_date -= relativedelta(months = self.term)
			self.ref_date.append([self.end_date, temp - timedelta(days = 1)])


	def get_price(self, code, start_date, end_date):
		query = f"SELECT AdjClose, Date FROM adjclose WHERE Code = '{code}'"
		code_df = pd.read_sql(query, con = self.engine)
		code_np = code_df.to_numpy()
		try:
			code_np = code_np[(code_np[:, 1] >= start_date) & (code_np[:, 1] <= end_date)]
			d_start_date = code_np[0][1]
			d_end_date = code_np[-1][1]
			if self.is_valid_date(start_date, end_date, d_start_date, d_end_date) and \
				(code_np[0][0] != None or code_np[-1][0]):
				return (code_np[-1][0] / code_np[0][0] * 100 - 100 , d_end_date)
			return (False)
		except Exception as e:
			print(f"{e} {code} : {start_date}, {end_date}")


	def grapher(self, data):
		plt.plot([row[1] for row in data], [row[0] for row in data])
		plt.show()


	def get_profit(self):
		self.rebalancing()
		for rf_start, rf_end in self.ref_date:
			sum = 0
			cnt = 0
			for code in self.codes:
				get_price = self.get_price(code, rf_start, rf_end)
				if get_price != None and get_price != False:
					sum += get_price[0]
					cnt += 1
					date = get_price[1]
					self.prices.append([get_price[0], get_price[1], code])
			self.mean.append([sum / cnt, date])
		self.grapher(self.mean)
		print(geometric_mean([row[0] for row in self.mean]))



if __name__ == "__main__":
	end_date = date(2021, 12, 31)
	start_date = date(2019, 12, 31)
	codes = ['005930', '395400', '259960', '026890']
	pc = ProfitCalc(end_date, start_date, 5, codes)