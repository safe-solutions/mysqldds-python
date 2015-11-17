#!/usr/bin/python

import sys, MySQLdb, getpass

class Db:
	def __init__(self, password):
		self.__con = MySQLdb.connect(host='localhost', user='root', passwd=password)
		self.__cur = self.__con.cursor()
		self.query("SET NAMES `utf8`")
	
	def get(self, table, fields, params, limit = None):
		return self.getbyquery("SELECT `%s` FROM `%s` WHERE %s %s" % ('`, `'.join(fields), table, ' AND '.join(["`%s`='%s'" % (k, v) for k, v in params.items()]), 'LIMIT ' + str(limit) if limit else ''))
	
	def set(self, table, params = None, where = None):
		if params and not where:
			return self.insert(table, params)
		elif where and not params:
			return self.delete(table, where)
		else:
			return self.update(table, params, where)

	def getbyquery(self, query):
		self.query(query)
		names = [x[0].lower() for x in self.__cur.description]
		for row in self.__cur:
			yield dict(zip(names, row))

	def getrow(self, query):
		res = self.getbyquery(query)
		for row in res:
			return row
		return False

	def getvalue(self, query):
		self.query(query)
		for row in self.__cur:
			return row[0]
		return False
	
	def getvalues(self, query):
		self.query(query)
		result = []
		for row in self.__cur:
			result.append(row[0])
		return result

	def query(self, query):
		#print query
		self.__cur.execute(query)
	
	def insert(self, table, params):
		for v in params:
			params[v] = params[v].replace("'", "")
			#params[v] = params[v].replace("\\", "\\\\")
		self.query("INSERT IGNORE INTO `%s` (`%s`) VALUES ('%s')" % (table, "`, `".join(params.keys()), "', '".join(params.values())))
		return self.__con.insert_id()
	
	def update(self, table, params, where):
		for v in params:
			params[v] = params[v].replace("'", "")
			#params[v] = params[v].replace("\\", "\\\\")
		self.query("UPDATE %s SET %s WHERE %s" % (table, ', '.join(["`%s`='%s'" % (k, v) for k, v in params.items()]), where.compile()))
		return self.__cur.rowcount
	
	def delete(self, table, where):
		self.query("DELETE FROM `%s` WHERE %s" % (table, where.compile()))
		return self.__cur.rowcount()
	
	def selectorinsert(self, table, params):
		return self.__con.insert_id() if self.insert(table, params) else self.getvalue("SELECT id FROM %s WHERE %s" % (table, ' AND '.join(["`%s`='%s'" % (k, v) for k, v in params.items()])))
	
	def close(self):
		self.__cur.close()
		self.__con.close()

cls_count = 0

def star_string(title):
	global cls_count
	cls_count += 1
	len_title = len(title)
	star_count = (80 - len_title - 2) / 2
	res = '*' * star_count
	res = res + ' ' + title + ' ' + res
	if (len(res) < 80):
		res += '*'
	return res

def star_msg(title):
	global cls_count
	cls_count += 1
	len_title = len(title)
	star_count = (80 - len_title - 4) / 2
	res = ' ' * star_count
	res = '*' + res + ' ' + title + ' ' + res
	res += ' *' if len(res) < 79 else '*'
	return res

def star_msg_left(title):
	global cls_count
	cls_count += 1
	len_title = len(title)
	star_count = 77 - len_title
	res = ' ' * star_count
	res = '* ' + title + res + '*'
	return res

def stars():
	global cls_count
	cls_count += 1
	return '*' * 80

def cls():
	global cls_count
	cls_count = 0

def end_cls():
	global cls_count
	while (cls_count < 22):
		print star_msg_left('')
	print stars()

def raw_input_ex(msg):
	res = raw_input(msg)
	if (res == 'quit'):
		sys.exit()
	return res

def init_screen():
	global check_repair, check_optimize
	cls()
	print star_string("MySQL DDS")
	print star_string("Welcome to MySQL Databases Diagnostics System")
	print stars()
	print star_msg_left("Type 'quit' in any menu to quit program")
	print star_msg_left("You can also use Ctrl-C")
	print star_msg_left("Current configuration:")
	print star_msg_left("Check for errors: " + check_repair)
	print star_msg_left("Check for unused disk space: " + check_optimize)
	print star_msg_left("Please note that unused disk space checking will run only for MyISAM tables.")
	print star_msg_left("")
	print star_msg_left("To change configuration press <Enter>")
	end_cls()

exclude_db = ['information_schema']
check_repair = 'yes'
check_optimize = 'yes'
repair_tables = []
optimize_tables = []

while True:
	init_screen()
	password = getpass.getpass("Enter ROOT password: ")
	if (password == ''):
		answer = raw_input_ex("Do you want to check for errors? [y/n] ")
		if (answer == 'n'):
			check_repair = 'no'
		answer = raw_input_ex("Do you want to check for unused disk space? [y/n] ")
		if (answer == 'n'):
			check_optimize = 'no'
	else:
		break

db = Db(password)
print "Connected to localhost."
if check_repair == 'yes':
	databases = db.getvalues("SHOW DATABASES WHERE `Database` NOT IN ('" + "','".join(exclude_db) + "')")
	for db_name in databases:
		db.query("USE `" + db_name + "`")
		tables = db.getvalues("SHOW TABLES")
		for table_name in tables:
			print "Checking `" + db_name + "`.`" + table_name + "`"
			res = db.getbyquery("CHECK TABLE `" + table_name + "`")
			for v in res:
				status = v['msg_text']
				if (status == 'Corrupt'):
					repair_tables.append("`" + db_name + "`.`" + table_name + "`")
					print status

if check_optimize == 'yes':
	tables = db.getbyquery("SELECT * FROM `information_schema`.`TABLES` WHERE `ENGINE`='MyISAM' AND `DATA_FREE`>0")
	for table in tables:
		if table['data_free'] > 0:
			df = table['data_free']
			if df > 1048576:
				df = str(df / 1048576) + ' MB'
			elif df > 1024:
				df = str(df / 1024) + ' KB'
			else:
				df = str(df) + ' B'
			optimize_tables.append({'db': table['table_schema'], 'table': table['table_name'], 'name': "`" + table['table_schema'] + "`.`" + table['table_name'] + "`", 'data_free': df})

len_rt = len(repair_tables)
if (len_rt > 0):
	answer = raw_input_ex("There are " + str(len_rt) + " tables to repair. Repair? [y/n] ")
	if (answer == 'y'):
		for table in repair_tables:
			print "Processing table", table
			res = db.getbyquery("REPAIR TABLE " + table)
			for v in res:
				print v['msg_text']
		getpass.getpass("Press <Enter>")
else:
	print "There are not tables to repair."

len_rt = len(optimize_tables)
if (len_rt > 0):
	prev_db = ''
	cls()
	print star_string("MySQL DDS")
	print star_string("Welcome to MySQL Databases Diagnostics System")
	print star_msg("Use <quit> in any menu to quit program")
	print stars()
	for table in optimize_tables:
		if prev_db != table['db']:
			prev_db = table['db']
			print star_msg_left(table['db'])
		print star_msg_left('|- ' + table['table'] + ' (unused ' + table['data_free'] + ')')
	end_cls()
	answer = raw_input_ex("There are " + str(len_rt) + " tables to optimize. Optimize? [y/n] ")
	if (answer == 'y'):
		for table in optimize_tables:
			print "Processing table", table['name']
			res = db.getbyquery("OPTIMIZE TABLE " + table['name'])
			for v in res:
				print v['msg_text']
else:
	print "There are not tables to optimize."
