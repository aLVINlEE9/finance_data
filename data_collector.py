import numpy as np
import pandas as pd
from datetime import datetime
from pandas_datareader import DataReader
import pymysql
import sqlalchemy as db
from marcap import marcap_data


class SQLAlchemyConnector:
	
	def __init__(self):
		self.engine = db.create_engine('mysql+pymysql://root:passwd@localhost/db_name')
		self.connection = self.engine.connect()

class MysqlConnector:
	def __init__(self):
		self.conn = pymysql.connect(host='localhost', user='root', 
			password='passwd', db='db_name', charset='utf8')

		with self.conn.cursor() as curs:

			sql = """
			CREATE TABLE IF NOT EXISTS raw_price_info (
				Date DATE,
				Code VARCHAR(20),
				Name VARCHAR(40)
				Market VARCHAR(20)
				MarketId VARCHAR(20),
				Open BIGINT(20),
				High BIGINT(20),
				Low BIGINT(20),
				Close BIGINT(20),
				ChangeCode BIGINT(20),
				Changes BIGINT(20), 
				ChagesRatio FLOAT(20),
				Volume FLOAT(20),
				Amount BIGINT(20),
				Marcap BIGINT(20),
				Stocks BIGINT(20),
				Ranks BIGINT(20),
				PRIMARY KEY (Date, Code, Market))
			"""
			curs.execute(sql)
		self.conn.commit()
	
	def __del__(self):
		"""Destructor"""
		self.conn.close() # disconnect with sql server

	def to_date(self, param):
		return (str(param)[:10])

	

class DataCollector(MysqlConnector):

	def __init__(self):
		super().__init__()


	def update_market_db(self, df_data):
		cnt = 0
		with self.conn.cursor() as curs:
			for r in df_data.itertuples():
				cnt += 1
				sql = f"REPLACE INTO raw_price_info VALUES ('{self.to_date(r.Index)}', "\
					f"'{r.Code}', '{r.Name}', '{r.Market}', '{r.MarketId}', {r.Open}, {r.High}, "\
					f"{r.Low}, {r.Close}, {r.ChangeCode}, {r.Changes}, {r.ChagesRatio}, {r.Volume}, "\
					f"{r.Amount}, {r.Marcap}, {r.Stocks}, {r.Rank})"
				curs.execute(sql)
				print('[{}] #{:04d} ({}) : {} rows > REPLACE INTO raw_price_info [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'),
																				cnt+1, r.Index, len(df_data)))
			self.conn.commit()


	def execute_updater(self):
		df = marcap_data('1995-05-02', '2021-12-31')
		self.update_market_db(df)


class MarketOpenDate(SQLAlchemyConnector):
	
	def __init__(self):
		super().__init__()
		self.create_table()

	def create_table(self):
		query = """
			CREATE TABLE IF NOT EXISTS market_open_info ( 
				Date DATE,
				Code VARCHAR(20),
				PRIMARY KEY (Date, Code))
			"""
		result_proxy = self.connection.execute(query)
		result_proxy.close()

	def market_open_date(self):
		query_1 = f"SELECT Date,Code FROM raw_price_info ORDER BY Date"
		df = pd.read_sql(query_1, con = self.engine)
		tt = len(df)
		cnt = 1
		for r in df.itertuples():
			query_2 = f"INSERT INTO market_open_info VALUES ('{r.Date}', '{r.Code}')"
			result_proxy = self.connection.execute(query_2)
			print(cnt / tt)
			cnt += 1
			result_proxy.close()


class MarketOpenUnique(SQLAlchemyConnector):

	def __init__(self):
		super().__init__()
		self.create_table()

	def create_table(self):
		query_1 = """
			CREATE TABLE IF NOT EXISTS unique_code ( 
				Code VARCHAR(20))
			"""
		result_proxy = self.connection.execute(query_1)
		result_proxy.close()

		query_2 = """
			CREATE TABLE IF NOT EXISTS unique_date ( 
				Date DATE)
			"""
		result_proxy = self.connection.execute(query_2)
		result_proxy.close()

	def unique_code(self):
		query_1 = f"SELECT Code FROM market_open_info ORDER BY Code"
		df = pd.read_sql(query_1, con = self.engine)
		unique_code_list = df["Code"].unique()
		unique_code_df = pd.DataFrame (unique_code_list, columns = ['unique_code'])
		tt = len(unique_code_df)
		cnt = 1
		for r in unique_code_df.itertuples():
			query_2 = f"INSERT INTO unique_code VALUES ('{r.unique_code}')"
			result_proxy = self.connection.execute(query_2)
			print(f"unique_code_df : {cnt / tt * 100} %")
			cnt += 1
			result_proxy.close()

	def unique_date(self):
		query_1 = f"SELECT Date FROM market_open_info ORDER BY Date"
		df = pd.read_sql(query_1, con = self.engine)
		unique_date_list = df["Date"].unique()
		unique_date_df = pd.DataFrame (unique_date_list, columns = ['unique_date'])
		tt = len(unique_date_df)
		cnt = 1
		for r in unique_date_df.itertuples():
			query_2 = f"INSERT INTO unique_date VALUES ('{r.unique_date}')"
			result_proxy = self.connection.execute(query_2)
			print(f"unique_date_df : {cnt / tt * 100} %")
			cnt += 1
			result_proxy.close()


	def execute(self):
		self.unique_code()
		self.unique_date()

if __name__ ==  '__main__':
	"""csv -> db_table(raw_price_info)"""
	dtct = DataCollector()
	dtct.execute_updater()

	"""marcket open date -> db_table(market_open_info)"""
	# mod = MarketOpenDate()
	# mod.market_open_date()
	
	"""unique open date -> db_table(unique_date, unique_code)"""
	# mou = MarketOpenUnique()
	# mou.execute()