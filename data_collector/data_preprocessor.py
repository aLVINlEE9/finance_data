class DataPreprocessor:
	
	def __init__(self):
		pass

	def print_status(self):
		pass


	def str_exception_out(self, string):
		string = str(string)
		string = string.replace("'", " ")
		string = string.replace("`", " ")
		string = string.replace("%", ".pct")
		return (string)


	def to_date(self, param):
		return (str(param)[:10])