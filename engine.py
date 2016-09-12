#!/bin/python


import csv,sys,re,itertools
from collections import Counter
from copy import deepcopy


def join_tables(a,b):
	row = [[]]
	for i in a:
		for j in b:
			rows.append([j,i])
	return rows

def distinct(seq): 
   # order preserving
   checked = []
   for e in seq:
       if e not in checked:
           checked.append(e)
   return checked

#-----------------------------------------------------------------------------------------------------#
output = []
metadata = []
i = 0
D = {}
table_name = []
t = 0
keywords = ['SELECT','FROM','WHERE','AND','OR']
functions = ['sum', 'average', 'max' , 'min', 'SUM', 'AVERAGE','MAX','MIN']

#-----------------------------------------------------------------------------------------------------#

with open('metadata.txt') as inputfile:
    for row in csv.reader(inputfile):
        metadata.append(row)
while i < len(metadata):
	table = [[]]
	if metadata[i] == ['<begin_table>']:
		table_name.append(metadata[i+1][0])
		t = t + 1
		i = i + 2 #skip table name and <begin_table>
		k = 0
		while metadata[i] != ['<end_table>']:
			table[0].append(metadata[i][0])
			i = i + 1
			k = k + 1
	D[table_name[t-1]] = table
	i = i + 1

#Parse CSV files
#populate the tables
for i in range(len(table_name)):
	t_name = table_name[i]
	with open(t_name+'.csv') as inputfile:
		for row in csv.reader(inputfile):
			row = [ int(x) for x in row ]
			D[t_name].append(row)

#take input for query
sql = (sys.argv[1]).split(" ")
if sql[0] not in ['SELECT','select']:
	print('ERROR: Must start with SELECT')
	sys.exit()

#-----------------------------------------------------------------------------------------------------#
# error handling
i = sql.count('')
while i!=0:
	sql.remove('')
	i = i - 1

i = sql.count(';')
if i != 1:
	print('Missing ;')

# Parsing
for i in sql:
	if i.upper() in keywords :
		keyword = i.upper()
		sql[sql.index(i)] = keyword

select_index = sql.index('SELECT') + 1
from_index = sql.index('FROM')

cols = []

# select what all 
while select_index != from_index:
	cols.append(sql[select_index].split(","))
	select_index += 1

cond = []
first = []
second = []
where_index = len(sql)

if 'WHERE' in sql:
	where_index = sql.index('WHERE')
	cond = sql[where_index+1 : ]

	first = []
	second = []

	if 'AND' in cond:
		and_index = cond.index('AND')
		first = cond[0 : and_index ]
		second = cond[and_index+ 1 : ]

	elif 'OR' in cond:
		or_index = cond.index('OR')
		first = cond[0 : or_index ]
		second = cond[or_index+ 1 : ]

# extract relevant table names
seperate_tables = []
table_names = sql[ from_index + 1 : where_index ]
for i in table_names:
	seperate_tables.extend(i.split(","))

#-----------------------------------------------------------------------------------------------------#
# Processing Queries

# function handling
#for i in cols:
#	if e.startswith( 'sum' ):

d_cols = []
if 'WHERE' not in sql:

	#select * from t1
	if sql[1] == '*' and len(sql) == 4:
		output = D[seperate_tables[0]]
		for i in output:
			print (', '.join(str(x) for x in i))

	# select * from t1,t2
	#elif sql[1] == '*' and len(seperate_tables) > 1 and len(sql) > 4:
		
	#	while 
	#		a = tables_contents.pop()
	#		b = tables_contents.pop()
	#		c = project(a, b)
	#		tables_contents.append(c)

	#	header = []
	#	for i in tables:
	#		header.extend(get_attributes(i))
	#	print ",  ".join(header)

	#	for i in tables_contents[0]:
	#		print i
		#print len(tables_contents[0])
	
	#select distinct(A,B..) from t1
	elif len(cols[0]) == 1 and len(seperate_tables) == 1:
		if cols[0][0].startswith('distinct(') or cols[0][0].startswith('DISTINCT('):
			col_split = re.split(r'(\,|\(|\))\s*',cols[0][0])
			col_split.remove(',')
			start_index = col_split.index('(') + 1
			end_index = col_split.index(')') - 1
			f_row = []
			t_name = seperate_tables[0]
			for i in range(start_index,end_index+1):
				f_row.append(t_name + '.' + col_split[i])
			print(', '.join(f_row))

			#error handling
			for i in col_split[start_index:end_index + 1]:
				if i not in D[t_name][0]:
					print('ERROR: Attribute ' + i +' not in table')

			flag = 0
			all_pairs = [[]]
			for i in col_split[start_index:end_index + 1]:
				index =  D[t_name][0].index(i)
				for k in range(1,len(D[t_name])):
					if flag == 0:
						all_pairs.append([])
					all_pairs[k-1].append(D[t_name][k][index])
				flag = 1
			dis_op = distinct(all_pairs)
			
			for i in dis_op:
				print (', '.join(str(x) for x in i))


	
	#select A,B from t1
	if len(cols[0]) >= 1 and len(seperate_tables) == 1:
		f_row = []
		for i in range(len(cols[0])):
			f_row.append(seperate_tables[0] + '.' + cols[0][i])

		output.append((f_row))
		

		t_name = seperate_tables[0]
		flag = 0
		op_flag = 0
		for i in cols[0]:
			#select func(A) from t1
			if i.startswith(tuple(functions)):
				x = []
				op_flag = 1
				if i.startswith('sum') or i.startswith('SUM'):
					col_split = re.split(r'(\,|\(|\))\s*',i)
					start_index = col_split.index('(') + 1
					index =  D[t_name][0].index(col_split[start_index])

					for k in range(1,len(D[t_name])):
						x.append(D[t_name][k][index])
					output.append([])
					output[1].append(sum(x))
				elif i.startswith('average') or i.startswith('AVERAGE'):
					col_split = re.split(r'(\,|\(|\))\s*',i)
					start_index = col_split.index('(') + 1
					index =  D[t_name][0].index(col_split[start_index])

					for k in range(1,len(D[t_name])):
						x.append(D[t_name][k][index])
					output.append([])
					output[1].append(sum(x)/len(x))				
				elif i.startswith('max') or i.startswith('MAX'):
					col_split = re.split(r'(\,|\(|\))\s*',i)
					start_index = col_split.index('(') + 1
					index =  D[t_name][0].index(col_split[start_index])

					for k in range(1,len(D[t_name])):
						x.append(D[t_name][k][index])
					output.append([])
					output[1].append(max(x))				
				elif i.startswith('min') or i.startswith('MIN'):
					col_split = re.split(r'(\,|\(|\))\s*',i)
					start_index = col_split.index('(') + 1
					index =  D[t_name][0].index(col_split[start_index])

					for k in range(1,len(D[t_name])):
						x.append(D[t_name][k][index])
					output.append([])
					output[1].append(min(x))				
			else:			
		#error handling
				for i in cols[0]:
					if i not in D[seperate_tables[0]][0]:
						print('ERROR: Attribute' + i +'not in table')

				index =  D[t_name][0].index(i)
				for k in range(1,len(D[t_name])):
					if flag == 0:
						output.append([])
					output[k].append(D[t_name][k][index])
					if op_flag == 1:
						break
				flag = 1

		for i in output:
			print (', '.join(str(x) for x in i))

	#select A,B,t1.A from t1,t2	
	elif len(cols[0]) > 1 and len(seperate_tables) > 1 and '*' not in cols[0]:
		col_op = []
		f_row = []
		for i in range(len(cols[0])):
			f_row.append(seperate_tables[0] + '.' + cols[0][i])
		output.append((f_row))

		for i in range(len(cols[0])):
			col_name = cols[0][i].split('.')
			if len(col_name)>1:
				if col_name[1] not in D[col_name[0]][0]:
					print('ERROR: Attribute' + i +'not in table')
		rows = []
		for k in range(len(cols[0])):
			col_op.append([])
			col_name = cols[0][k].split('.')
			# of type tablename.colname
			if len(col_name)>1:
				t_name = col_name[0]
				col = col_name[1]
				index =  D[t_name][0].index(col)
				for t in range(1,len(D[t_name])):
					col_op[k].append(D[t_name][t][index])
			
			else:
				print('Column '+ col_name[0]+' in field list is ambiguous')

		print(', '.join(str(x) for x in f_row))
		for j in range(len(col_op[i])):
			line = []
			for i in range(len(col_op)):
				line.append(col_op[i][j])
			print(', '.join(str(x) for x in line))