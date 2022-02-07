import FinanceDataReader as fdr
from datetime import datetime

class DataPreprocessor:
	
	def __init__(self):
		self.market = fdr.StockListing('KRX')


	def print_replace_status(self, table, at, total, status_1=None, status_2=None):

		print('[{}] : ({}, {}) #{:04d} / {} rows > REPLACE INTO {} [OK]'.\
				format(datetime.now().strftime('%Y-%m-%d %H:%M'), status_1, \
				status_2, at+1, total, table))
		

	def print_create_status(self, table):

		print('[{}] : CREATE TABLE IF NOT EXISTS {} [OK]'.\
				format(datetime.now().strftime('%Y-%m-%d %H:%M'), table))


	def str_exception_out(self, string):
		string = str(string)
		string = string.replace("'", " ")
		string = string.replace("`", " ")
		string = string.replace("%", ".pct")
		return (string)


	def to_date(self, param):
		return (str(param)[:10])

	def util_zfill(self, code):
		return (str(code).zfill(6))

	def util_symbol(self, code):
		return (self.market[self.market["Symbol"] == code]["Market"].values[0])