import sqlalchemy as db
import pymysql
import pandas as pd
import matplotlib.pyplot as plt
from data_collector import SQLAlchemyConnector

class DataRead(SQLAlchemyConnector):

	def __init__(self):
		super().__init__()

	def read(self, code):
		query = f"SELECT * FROM raw_price_info WHERE Code = {code} ORDER BY Date"
		df = pd.read_sql(query, con = self.engine)
		return (df)

	def grapher(self, code):
		df = self.read(code)
		df = df.set_index("Date")
		df = df["Close"][0]
		print(df)
		# plt.figure()
		# df.plot()
		# plt.show()

if __name__ == "__main__":
	"""read objecta"""
	dr = DataRead()
	dr.grapher("005930")