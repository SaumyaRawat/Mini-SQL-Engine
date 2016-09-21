#!/bin/python


import csv,sys,re,itertools
from collections import Counter
from copy import deepcopy
import operator



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
functions = ['sum', 'average', 'max' , 'min', 'SUM', 'AVERAGE','MAX','MIN','avg','AVG']
ops = {"=": operator.eq,
       "<": operator.lt,
       "<=": operator.le,
       ">": operator.gt,
       ">=": operator.ge,
       "!=": operator.ne,
       "<>": operator.ne }
opkeys = ['=', '>', '<>', '<=', '<', '>=', '!=']
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

if sql[0] not in ['SELECT','select','Select']:
	print('ERROR: Must start with SELECT')
	sys.exit()
#-----------------------------------------------------------------------------------------------------#
# error handling
i = sql.count('')
while i!=0:
	sql.remove('')
	i = i - 1


# Parsing
for i in sql:
	if i.upper() in keywords :
		keyword = i.upper()
		sql[sql.index(i)] = keyword
	if ';' in i :
		sql[sql.index(i)] = sql[sql.index(i)][0:len(sql[sql.index(i)])-1]


select_index = sql.index('SELECT') + 1
from_index = sql.index('FROM')

cols = []
tcols = sql[select_index:from_index]
for x in tcols:
	if (x.startswith('distinct(') and x.startswith('DISTINCT(') and x.startswith(tuple(functions))) is False:
		cols.extend(x.split(','))
	elif x.endswith(','):
		cols.append(x[:-1])
	elif x != '' and x!= ',':
		cols.append(x)	


for i in range(cols.count('')):
	cols.remove('')

cond = []
first = []
second = []
where_index = len(sql)

if 'WHERE' in sql:
	where_index = sql.index('WHERE')
	cond = sql[where_index+1 : ]

	#error handling
	if cond == []:
		print('No condition for WHERE!')
		sys.exit()

	first = []
	second = []
	and_index = -1
	or_index = -1
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

for i in seperate_tables:
     if i not in D.keys():
            print('Error! table doesnt exist!')
            sys.exit()


#-----------------------------------------------------------------------------------------------------#
# Processing Queries

d_cols = []
if 'WHERE' not in sql:
	# ********************************************************************************************************** #
	#select * from t1
	if sql[1] == '*' and len(sql) == 4:
		output = D[seperate_tables[0]]
		for i in output:
			print (', '.join(str(x) for x in i))

	# ********************************************************************************************************** #
	# select * from t1,t2
	elif sql[1] == '*' and len(seperate_tables) > 1 and len(sql) > 4:
		f_row = []
		for k in range(len(seperate_tables)):
			row1 = deepcopy(D[t1][0])
			for x in row1:
				row1[row1.index(x)] = t1 + '.' + x
			f_row = row + row1

		output.append((f_row))

		for ind in range(1,len(D[t1])):
			if op_func(D[t1][ind][c_ind], D[t2][ind][c2_ind]):
				output.append(D[t1][ind][:]+D[t2][ind][:])
		for i in output:
			print (', '.join(str(x) for x in i))


	# ********************************************************************************************************** #	
	#select distinct(A,B..) from t1
	elif len(cols) == 1 and len(seperate_tables) == 1 and cols[0].startswith('distinct(') or cols[0].startswith('DISTINCT('):
		col_split = re.split(r'(\,|\(|\))\s*',cols[0])
		if ',' in col_split:
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
				sys.exit()
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

	# ********************************************************************************************************** #	
	#select A,B from t1
	elif len(cols) >= 1 and len(seperate_tables) == 1:
		f_row = []
		for i in range(len(cols)):
			f_row.append(seperate_tables[0] + '.' + cols[i])

		output.append((f_row))
		

		t_name = seperate_tables[0]
		flag = 0
		op_flag = 0
		for i in cols:
	# ********************************************************************************************************** #
			#select func(A) from t1
			col_name = i.split('.')
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
				elif i.startswith('average') or i.startswith('AVERAGE') or i.startswith('avg') or i.startswith('AVG'):
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
			elif len(col_name)>1:
				t_name = col_name[0]
				if col_name[1] not in D[t_name][0]:
						print('ERROR: Attribute ' + i +' not in table')
						sys.exit()
				c_name = col_name[1]
				index =  D[t_name][0].index(c_name)
				for k in range(1,len(D[t_name])):
					if flag == 0:
						output.append([])
					output[k].append(D[t_name][k][index])
					if op_flag == 1:
						break
				flag = 1

			else:			
		#error handling
				for i in cols:
					if i not in D[seperate_tables[0]][0] and '(' not in i:
						print('ERROR: Attribute ' + i +' not in table')
						sys.exit()

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
	# ********************************************************************************************************** #
	#select A,B,t1.A from t1,t2	
	elif len(cols) > 1 and len(seperate_tables) > 1 and '*' not in cols:
		col_op = []
		f_row = []
		for i in range(len(cols)):
			f_row.append(seperate_tables[0] + '.' + cols[i])
		output.append((f_row))

		for i in range(len(cols)):
			col_name = cols[i].split('.')
			if len(col_name)>1:
				if col_name[1] not in D[col_name[0]][0]:
					print('ERROR: Attribute' + i +'not in table')
					sys.exit()
		rows = []
		for k in range(len(cols)):
			col_op.append([])
			col_name = cols[k].split('.')
			# of type tablename.colname
			if len(col_name)>1:
				t_name = col_name[0]
				col = col_name[1]
				index =  D[t_name][0].index(col)
				for t in range(1,len(D[t_name])):
					col_op[k].append(D[t_name][t][index])
			
			elif len(col_name)==1:
				for t_name in seperate_tables:
					if col_name[0] in D[t_name][0]:
							index = D[t_name][0].index(col_name[0])
							for t in range(1,len(D[t_name])):
								col_op[k].append(D[t_name][t][index])
		if col_op[0]==[]:
			for c in cols:
				print('Column '+ c+' in field list is ambiguous')

		print(', '.join(str(x) for x in f_row))
		for j in range(len(col_op[i])):
			line = []
			for i in range(len(col_op)):
				line.append(col_op[i][j])
			print(', '.join(str(x) for x in line))

## Queries with where
elif 'WHERE' in sql:	
	if cond!= [] and first == [] and second == [] and sql[1] == '*':
		cond = "".join(cond)
		f_row = []
		if len(cond.split("<="))>1:
			#error handling
			if len(cond.split("<="))>2:
				print('Error: Condition is invalid ')
				sys.exit()
			condition = cond.split("<=")
			op_func = ops["<="]

		elif len(cond.split(">="))>1:
			#error handling
			if len(cond.split("<="))>2:
				print('Error: Condition is invalid ')
				sys.exit()
			condition = cond.split(">=")
			op_func = ops[">="]

		elif len(cond.split("!="))>1:
			#error handling
			if len(cond.split("<="))>2:
				print('Error: Condition is invalid ')
				sys.exit()
			condition = cond.split("!=")
			op_func = ops["!="]

		elif len(cond.split("<>"))>1:
			#error handling
			if len(cond.split("<="))>2:
				print('Error: Condition is invalid ')
				sys.exit()
			condition = cond.split("<>")
			op_func = ops["<>"]

		elif len(cond.split("="))>1:
			#error handling
			if len(cond.split("<="))>2:
				print('Error: Condition is invalid ')
				sys.exit()
			condition = cond.split("=")
			op_func = ops["="]

		elif len(cond.split("<"))>1:
			#error handling
			if len(cond.split("<="))>2:
				print('Error: Condition is invalid ')
				sys.exit()
			condition = cond.split("<")
			op_func = ops["<"]


		elif len(cond.split(">"))>1:
			#error handling
			if len(cond.split("<="))>2:
				print('Error: Condition is invalid ')
				sys.exit()
			condition = cond.split(">")
			op_func = ops[">"]

		term1 = condition[0]
		term2 = condition[1]


		if len(term1.split('.'))!=1:
			t1 = term1.split('.')[0]
			c1 = term1.split('.')[1]
		else:
			c1 = term1.split('.')[0]
			if len(seperate_tables)==1:
				t1 = seperate_tables[0]
			#error handling
			else:
				print('Error : More than one table called and no reference table for column mentioned!')
				sys.exit()
		
		c_ind = D[t1][0].index(c1)
		if term2.startswith('-'):
			val=term2[1:]
		else:
			val='empty'
		if term2.isdigit() or val.isdigit():
			# ********************************************************************************************************** #
			#select * from t1 where t1.A <op> NUMBER
			f_row.extend(D[t1][0])
			output.append(f_row)

			for ind in range(1,len(D[t1])):
				if op_func(D[t1][ind][c_ind], term2):
					output.append(D[t1][ind][:])

		else:
			# ********************************************************************************************************** #
			#select * from t1,t2 where t1.A <op> t2.B
			c2 = term2.split('.')[1]
			t2 = term2.split('.')[0]
			
			#error handling
			c2_ind = D[t2][0].index(c2) if c2 in D[t2][0] else -1
			if c2_ind == -1:
			   	print('Error : Attribute not found')
			   	sys.exit()
			
			row1 = D[t1][0]
			for x in row1:
				row1[row1.index(x)] = t1 + '.' + x

			row2 = D[t2][0]
			for x in row2:
				row2[row2.index(x)] = t2 + '.' + x				

			f_row.extend(row1 + row2)
			output.append((f_row))

			for ind in range(1,len(D[t1])):
				if op_func(D[t1][ind][c_ind], D[t2][ind][c2_ind]):
					output.append(D[t1][ind][:]+D[t2][ind][:])
		for i in output:
			print (', '.join(str(x) for x in i))

	elif cond!= [] and first == [] and second == []:
			# ********************************************************************************************************** #
			#select A,B from t1,t2 where t1.A <op> t2.B
			cond = "".join(cond)
			f_row = []
			if len(cond.split("<="))>1:
				#error handling
				if len(cond.split("<="))>2:
					print('Error: Condition is invalid ')
					sys.exit()
				condition = cond.split("<=")
				op_func = ops["<="]

			elif len(cond.split(">="))>1:
				#error handling
				if len(cond.split(">="))>2:
					print('Error: Condition is invalid ')
					sys.exit()
				condition = cond.split(">=")
				op_func = ops[">="]

			elif len(cond.split("!="))>1:
				#error handling
				if len(cond.split("!="))>2:
					print('Error: Condition is invalid ')
					sys.exit()
				condition = cond.split("!=")
				op_func = ops["!="]

			elif len(cond.split("<>"))>1:
				#error handling
				if len(cond.split("<>"))>2:
					print('Error: Condition is invalid ')
					sys.exit()
				condition = cond.split("<>")
				op_func = ops["<>"]

			elif len(cond.split("="))>1:
				#error handling
				if len(cond.split("="))>2:
					print('Error: Condition is invalid ')
					sys.exit()
				condition = cond.split("=")
				op_func = ops["="]

			elif len(cond.split("<"))>1:
				#error handling
				if len(cond.split("<"))>2:
					print('Error: Condition is invalid ')
					sys.exit()
				condition = cond.split("<")
				op_func = ops["<"]


			elif len(cond.split(">"))>1:
				#error handling
				if len(cond.split(">"))>2:
					print('Error: Condition is invalid ')
					sys.exit()
				condition = cond.split(">")
				op_func = ops[">"]

			term1 = condition[0]
			term2 = condition[1]

			if len(term1.split('.'))!=1:
				t1 = term1.split('.')[0]
				c1 = term1.split('.')[1]
			else:
				c1 = term1.split('.')[0]
				if len(seperate_tables)==1:
					t1 = seperate_tables[0]
				#error handling
				else:
					print('Error : More than one table called and no reference table for column mentioned!')
					sys.exit()
			
			c_ind = D[t1][0].index(c1)
			if term2.startswith('-'):
				val=term2[1:]
			else:
				val='empty'
			if term2.isdigit() or val.isdigit():
				# ********************************************************************************************************** #
				#select A,B from t1,t2 where t1.A <op> NUMBER
				
				col_op = []
				f_row.extend(cols)
				output.append(f_row)
				rows = []
				for k in range(len(cols)):
					col_op.append([])
					col_name = cols[k].split('.')
					# of type tablename.colname
					if len(col_name)>1:
						t_name = col_name[0]
						col = col_name[1]
						index =  D[t_name][0].index(col)
						for ind in range(1,len(D[t1])):
							if op_func(D[t1][ind][c_ind], int(term2)):
								col_op[k].append(D[t_name][ind][index])
					elif len(col_name)==1:
						for t_name in seperate_tables:
							if col_name[0] in D[t_name][0]:
									index = D[t_name][0].index(col_name[0])
									for t in range(1,len(D[t_name])):
										col_op[k].append(D[t_name][t][index])
				if col_op[0]==[]:
					for c in cols:
						print('Column '+ c+' in field list is ambiguous')
						sys.exit()


			else:
				# ********************************************************************************************************** #
				#select A,B from t1,t2 where t1.A <op> t2.B
				c2 = term2.split('.')[1]
				t2 = term2.split('.')[0]
				
				#error handling
				c2_ind = D[t2][0].index(c2) if c2 in D[t2][0] else -1
				if c2_ind == -1:
				   	print('Error : Attribute not found')
				   	sys.exit()
				f_row = []
				for i in range(len(cols)):
					f_row.append(cols[i])
				output.append((f_row))

				col_op = []

				for i in range(len(cols)):
					col_name = cols[i].split('.')
					if len(col_name)>1:
						if col_name[1] not in D[col_name[0]][0]:
							print('ERROR: Attribute ' + col_name[1] +' not in ' + col_name[0])
							sys.exit()
				rows = []
				for k in range(len(cols)):
					col_op.append([])
					col_name = cols[k].split('.')
					# of type tablename.colname
					if len(col_name)>1:
						t_name = col_name[0]
						col = col_name[1]
						index =  D[t_name][0].index(col)
						for ind in range(1,len(D[t1])):
							if op_func(D[t1][ind][c_ind], D[t2][ind][c2_ind]):
								col_op[k].append(D[t_name][ind][index])
					elif len(col_name)==1:
						for t_name in seperate_tables:
							if col_name[0] in D[t_name][0]:
									c_ind = D[t_name][0].index(col_name[0])
									for ind in range(1,len(D[t_name])):
										if op_func(D[t1][ind][c_ind], D[t2][ind][c2_ind]):
											col_op[k].append(D[t_name][ind][c_ind])
				if col_op[0]==[]:
					for c in cols:
						print('Column '+ c+' in field list is ambiguous')
						sys.exit()

			print(', '.join(str(x) for x in f_row))
			for count_j in range(len(col_op[0])):
				line = []
				for i in range(len(col_op)):
					line.append(col_op[i][count_j])
				print(', '.join(str(x) for x in line))
	# and / or
	elif first!=[] and second!=[]:
		first = "".join(first)
		second = "".join(second)
		# ********************************************************************************************************** #
		#select A,B from t1,t2 where t1.A <op> t2.B
		f_row = []
		# FIRST
		if len(first.split("="))>2:
			print('Error: Condition is invalid ')
			sys.exit()
		condition1 = first.split("=")
		op_func = ops["="]

		term1 = condition1[0]
		term2 = condition1[1]


		if len(term1.split('.'))>1:
			c1 = term1.split('.')[1]
			t1 = term1.split('.')[0]
		elif len(seperate_tables)==1:
			c1 = term1.split('.')[0]
			t1 = seperate_tables[0]
		else:
			print('Error : Ambiguity!')
			sys.exit()

		c1_ind = D[t1][0].index(c1)

		#SECOND
		if len(second.split("="))>2:
			print('Error: Condition is invalid ')
			sys.exit()
		condition2 = second.split("=")
		op_func = ops["="]

		term3 = condition2[0]
		term4 = condition2[1]


		if len(term3.split('.'))>1:
			c3 = term3.split('.')[1]
			t3 = term3.split('.')[0]
		elif len(seperate_tables)==1:
			c3 = term3.split('.')[0]
			t3 = seperate_tables[0]
		else:
			print('Error : Ambiguity!')
			sys.exit()
		c3_ind = D[t3][0].index(c3)

		col_op = [[]]
		rows = []
		snitch = 0
			
		for k in range(len(cols)):
			col_name = cols[k].split('.')
			# of type tablename.colname
			if len(col_name)>1:
				t_name = col_name[0]
				col = col_name[1]
				index =  D[t_name][0].index(col)
				for ind in range(1,len(D[t1])): #assumes all have same no of rows
					col_op.append([])
					if term2.startswith('-'):
						val=term2[1:]
					else:
						val='empty'
					if term2.isdigit() or val.isdigit():
						res1=op_func(D[t1][ind][c1_ind], int(term2))
						col1_op = D[t_name][ind][index]

					else:
						c2 = term2.split('.')[1]
						t2 = term2.split('.')[0]
						res1=op_func(D[t1][ind][c1_ind], D[t2][ind][c2_ind])
						
					if term4.startswith('-'):
						val=term4[1:]
					else:
						val='empty'
					if term4.isdigit() or val.isdigit():
						res2=op_func(D[t3][ind][c3_ind], int(term4))

					else:
						c4 = term4.split('.')[1]
						t4 = term4.split('.')[0]
						res2=op_func(D[t1][ind][c3_ind], D[t2][ind][c4_ind])
						col2_op=D[t_name][ind][index]

					if and_index!=-1 and or_index==-1:
					#AND
						if res1 and res2:
							col_op[ind].append(D[t_name][ind][index])

					elif or_index!=-1 and and_index == -1:
					#OR
						if res1 or res2:
							col_op[ind].append(D[t_name][ind][index])
			elif len(seperate_tables)==1:
				t_name = seperate_tables[0]
				col = col_name[0]
				index =  D[t_name][0].index(col)
				for ind in range(1,len(D[t_name])): #assumes all have same no of rows
					col_op.append([])
					if term2.startswith('-'):
						val=term2[1:]
					else:
						val='empty'
					if term2.isdigit() or val.isdigit():
						res1=op_func(D[t_name][ind][c1_ind], int(term2))
						col1_op = D[t_name][ind][index]

					if term4.startswith('-'):
						val=term4[1:]
					else:
						val='empty'
					if term4.isdigit() or val.isdigit():
						res2=op_func(D[t3][ind][c3_ind], int(term4))
	
					if and_index!=-1 and or_index==-1:
					#AND
						if res1 and res2:
							col_op[ind].append(D[t_name][ind][index])

					elif or_index!=-1 and and_index == -1:
					#OR
						if res1 or res2:
							col_op[ind].append(D[t_name][ind][index])				


		count_j = 0

		for snitch in range(col_op.count([])):
			col_op.remove([])
	
		print(', '.join(str(x) for x in cols))
#		print(', '.join(str(x) for x in col_op ))

		for count_j in range(len(col_op[0])):
			line = []
			for i in range(len(col_op)):
				line.append(col_op[i][count_j])
			print(', '.join(str(x) for x in line))