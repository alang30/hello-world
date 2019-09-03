import pandas as pd
import io
from pandas import ExcelWriter
from pandas import ExcelFile
import psycopg2
import csv

def loadSQL(params):
		#extract the applicantData tab from Applicant_Data.xlsx, save it to a CSV, and save the csv contents to the variable applicantcsv
	#TESTING
	#dm - 1/16/18 - changed file name because it was broken.
	filename = input('Is your file named formerworker.xlsx? Y/N\n')
	#loadTables = 'y'
	if filename.lower() == 'y':
		filename = 'formerworker.xlsx'
	else: 
		filename = input('Please type out the excel file name with .xlsx\n')
	print(filename)
	#filename = 'Former_Worker_DGW.xlsx'
	jobdetailscsv = io.StringIO()
	personaldatacsv = io.StringIO()
	namecsv = io.StringIO()
	addresscsv = io.StringIO()
	phonecsv = io.StringIO()
	emailcsv = io.StringIO()

	jobdetails = pd.read_excel(filename,sheet_name='Former Worker Job Details', usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,21],skiprows=[0,1,3],na_filter=False,dtype=object)
	jobdetails.to_csv(jobdetailscsv)
	jobdetailscsv.seek(0)

	personalfile = pd.read_excel(filename,sheet_name='Former Worker Personal Data',usecols=[0,1,2,3,4,5,6,7,8,9,10,11],skiprows=[0,1,3],na_filter=False,dtype=object)
	personalfile.to_csv(personaldatacsv)
	personaldatacsv.seek(0)

	namefile = pd.read_excel(filename,sheet_name='Former Worker Name',usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,21,22,23,24,25,26,27],skiprows=[0,1,3],na_filter=False,dtype=object)
	namefile.to_csv(namecsv)
	namecsv.seek(0)

	emailfile = pd.read_excel(filename,sheet_name='Former Worker Email',usecols=[0,1,2,3],skiprows=[0,1,3],na_filter=False,dtype=object)
	emailfile.to_csv(emailcsv)
	emailcsv.seek(0)

	addressfile = pd.read_excel(filename,sheet_name='Former Worker Address',usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,21,22],skiprows=[0,1,3],na_filter=False,dtype=object)
	addressfile.to_csv(addresscsv)
	addresscsv.seek(0)

	phonefile = pd.read_excel(filename,sheet_name='Former Worker Phone',usecols=[0,1,2,3,4,5,6,7,8],skiprows=[0,1,3],na_filter=False,dtype=object)
	phonefile.to_csv(phonecsv)
	phonecsv.seek(0)
	#applicantcsv = open("locationdatacsv.csv",'r')
		
	# addressfile = pd.read_excel(filename,sheet_name='Pre-Hire Address',skiprows=[0,1,3],na_filter=False,dtype=object)
	# addressfile.to_csv(addresscsv)
	# addresscsv.seek(0)
	# #addresscsv = open("prehireaddress.csv",'r')
		
	# prehirephonefile = pd.read_excel(filename,sheet_name='Pre-Hire Phone',skiprows=[0,1,3],na_filter=False,dtype=object)
	# prehirephonefile.to_csv(phonecsv)
	# phonecsv.seek(0)
	# #phonecsv = open("prehirephone.csv",'r')

	# prehireemailfile = pd.read_excel(filename,sheet_name='Pre-Hire Email',skiprows=[0,1,3],na_filter=False,dtype=object)
	# prehireemailfile.to_csv(emailcsv)
	# emailcsv.seek(0)
	# #emailcsv = open("prehireemail.csv",'r')
	
	
		

		#configure variables for connection via the serverconnect.py script
	conn = psycopg2.connect(params)
	cur = conn.cursor()

		#remove the locationdata table from the sql server
	cur.execute("DROP TABLE IF EXISTS formerworkerjob")
		#create the prehirdata table with variables to match the columns in the file
	cur.execute("""CREATE TABLE formerworkerjob
						(index SERIAL PRIMARY KEY,
						former_worker_id VARCHAR(255),
						former_worker_type VARCHAR(255),
						last_termination_date VARCHAR(255),
						recent_hire_date VARCHAR(255),
						original_hire_date VARCHAR(255),
						continuous_service_date VARCHAR(255),
						last_termination_reason VARCHAR(255),
						manager VARCHAR(255),
						cost_center VARCHAR(255),
						job_title VARCHAR(255),
						job_code VARCHAR(255),
						job_profile VARCHAR(255),
						job_level VARCHAR(255),
						time_type VARCHAR(255),
						location VARCHAR(255),
						base_pay VARCHAR(255),
						currency VARCHAR(255),
						frequency VARCHAR(255),
						scheduled_weekly_hours VARCHAR(255),
						eligible_for_rehire VARCHAR(255),
						performance_rating VARCHAR(255));""")
	conn.commit()
		#load the data from the csv file into the locationdata table
	cur.copy_expert("""COPY formerworkerjob
						 (index, former_worker_id, former_worker_type, last_termination_date, recent_hire_date, original_hire_date, continuous_service_date, last_termination_reason, manager, cost_center, job_title, job_code, job_profile, job_level, time_type, location, base_pay, currency, frequency, scheduled_weekly_hours, eligible_for_rehire, performance_rating)
						 from stdin delimiter ',' csv header quote '\"';""",jobdetailscsv)
	conn.commit()
	
	cur.execute("DROP TABLE IF EXISTS formerworkerpersonal")
	cur.execute("""CREATE TABLE formerworkerpersonal
						(index SERIAL PRIMARY KEY,
						former_worker_id VARCHAR(255),
						dateofbirth VARCHAR(255),
						hispanic_or_latino VARCHAR(255),
						ethnicity_code BOOLEAN,
						national_id VARCHAR(255),
						national_id_type VARCHAR(255),
						national_id_iso VARCHAR(255),
						issued_date VARCHAR(255),
						expiration_date VARCHAR(255),
						verification_date VARCHAR(255),
						series VARCHAR(255),
						issuing_agency VARCHAR(255));""")
	conn.commit()

	cur.copy_expert("""COPY formerworkerpersonal
						 (index,former_worker_id, dateofbirth, hispanic_or_latino, ethnicity_code, national_id, national_id_type, national_id_iso, issued_date, expiration_date, verification_date, series, issuing_agency)
						 from stdin delimiter ',' csv header quote '\"';""",personaldatacsv)
	conn.commit()


	cur.execute("DROP TABLE IF EXISTS formerworkername")
	cur.execute("""CREATE TABLE formerworkername
						(index SERIAL PRIMARY KEY,
						former_worker_id VARCHAR(255),
						countrycode VARCHAR(255),
						legal_title VARCHAR(255),
						legal_first_name VARCHAR(255),
						legal_middle_name VARCHAR(255),
						legal_last_name VARCHAR(255),
						legal_secondary_name VARCHAR(255),
						legal_social_suffix VARCHAR(255),
						legal_hereditary_suffix VARCHAR(255),
						legal_local_first_name VARCHAR(255),
						legal_local_first_name2 VARCHAR(255),
						legal_local_middle_name VARCHAR(255),
						legal_local_last_name VARCHAR(255),
						legal_local_last_name2 VARCHAR(255),
						legal_local_secondary_name VARCHAR(255),
						preferred_title VARCHAR(255),
						preferred_first_name VARCHAR(255),
						preferred_middle_name VARCHAR(255),
						preferred_last_name VARCHAR(255),
						preferred_secondary_name VARCHAR(255),
						preferred_social_suffix VARCHAR(255),
						preferred_hereditary_suffix VARCHAR(255),
						preferred_local_first_name VARCHAR(255),
						preferred_local_first_name2 VARCHAR(255),
						preferred_local_middle_name VARCHAR(255),
						preferred_local_last_name VARCHAR(255),
						preferred_local_last_name2 VARCHAR(255),
						preferred_local_secondary_name VARCHAR(255));""")
	conn.commit()

	cur.copy_expert("""COPY formerworkername
						 (index,former_worker_id,countrycode,legal_title,legal_first_name,legal_middle_name,legal_last_name,legal_secondary_name,legal_social_suffix,legal_hereditary_suffix,legal_local_first_name,legal_local_first_name2,legal_local_middle_name,legal_local_last_name,legal_local_last_name2,legal_local_secondary_name,preferred_title,preferred_first_name,preferred_middle_name,preferred_last_name,preferred_secondary_name,preferred_social_suffix,preferred_hereditary_suffix,preferred_local_first_name,preferred_local_first_name2,preferred_local_middle_name,preferred_local_last_name,preferred_local_last_name2,preferred_local_secondary_name)
						 from stdin delimiter ',' csv header quote '\"';""",namecsv)
	conn.commit()
	

	cur.execute("DROP TABLE IF EXISTS formerworkeremail")
	cur.execute("""CREATE TABLE formerworkeremail
						(index SERIAL PRIMARY KEY,
						former_worker_id VARCHAR(255),
						email VARCHAR(255),
						usage_type VARCHAR(255),
						primaryemail BOOLEAN);""")
	conn.commit()

	cur.copy_expert("""COPY formerworkeremail
						 (index,former_worker_id, email, usage_type, primaryemail)
						 from stdin delimiter ',' csv header quote '\"';""",emailcsv)
	conn.commit()
	


	cur.execute("DROP TABLE IF EXISTS formerworkeraddress")
	cur.execute("""CREATE TABLE formerworkeraddress
						(index SERIAL PRIMARY KEY,
						former_worker_id VARCHAR(255),
						countrycode VARCHAR(255),
						address_line_1 VARCHAR(255),
						address_line_2 VARCHAR(255),
						address_line_3 VARCHAR(255),
						address_line_4 VARCHAR(255),
						address_line_5 VARCHAR(255),
						address_line_6 VARCHAR(255),
						address_line_7 VARCHAR(255),
						address_line_8 VARCHAR(255),
						address_line_9 VARCHAR(255),
						city VARCHAR(255),
						city_subdivision1 VARCHAR(255),
						city_subdivision2 VARCHAR(255),
						region VARCHAR(255),
						region_subdivision1 VARCHAR(255),
						region_subdivision2 VARCHAR(255),
						address_line_1_local VARCHAR(255),
						address_line_2_local VARCHAR(255),
						city_local VARCHAR(255),
						postalcode VARCHAR(255),
						usage_type VARCHAR(255),
						primaryaddress BOOLEAN);""")
	conn.commit()

	cur.copy_expert("""COPY formerworkeraddress
						 (index,former_worker_id,countrycode,address_line_1,address_line_2,address_line_3,address_line_4,address_line_5,address_line_6,address_line_7,address_line_8,address_line_9,city,city_subdivision1,city_subdivision2,region,region_subdivision1,region_subdivision2,address_line_1_local,address_line_2_local,city_local,postalcode,usage_type,primaryaddress)
						 from stdin delimiter ',' csv header quote '\"';""",addresscsv)


	cur.execute("DROP TABLE IF EXISTS formerworkerphone")
	cur.execute("""CREATE TABLE formerworkerphone
						(index SERIAL PRIMARY KEY,
						former_worker_id VARCHAR(255),
						countrycode VARCHAR(255),
						international_phone_code VARCHAR(255),
						area_code VARCHAR(255),
						phone_number VARCHAR(255),
						phone_extension VARCHAR(255),
						device_type VARCHAR(255),
						usage_type VARCHAR(255),
						primaryphone BOOLEAN);""")
	conn.commit()

	cur.copy_expert("""COPY formerworkerphone
						 (index,former_worker_id,countrycode,international_phone_code,area_code,phone_number,phone_extension,device_type,usage_type,primaryphone)
						 from stdin delimiter ',' csv header quote '\"';""",phonecsv)

def loadReference(reference,params,table):
	conn = psycopg2.connect(params)
	cur = conn.cursor()

	cur.execute("DROP TABLE IF EXISTS {0}""".format(table))
	cur.execute("""CREATE TABLE {0}
			(index SERIAL PRIMARY KEY,
			value VARCHAR(255),
			id VARCHAR(255));""".format(table))
	conn.commit()
	sql = """INSERT INTO {0} (value,id) VALUES """.format(table)
	

	for index,row in reference.items():
		sql += """('{0}','{1}'),""".format(row.replace("'","''"),index.replace("'","''"))

	sql = sql[:-1]
	sql+=';'
	
	if len(reference) > 0:
		cur.execute(sql)
		conn.commit()

def loadPredefinedNameComponentsSQL(nameComponents,params):
	conn = psycopg2.connect(params)
	cur = conn.cursor()

	cur.execute("DROP TABLE IF EXISTS translate_title")
	cur.execute("""CREATE TABLE translate_title
			(index SERIAL PRIMARY KEY,
			countrycode VARCHAR(255),
			titletype VARCHAR(255),
			value VARCHAR(255),
			id VARCHAR(255));""")
	conn.commit()
	sql = """INSERT INTO translate_title (countrycode,titletype,value,id) VALUES """

	for index,row in nameComponents.items():
		sql += """('{0}','{1}','{2}','{3}'),""".format(row[0].replace("'","''"),row[1].replace("'","''"),row[2].replace("'","''"),index.replace("'","''"))

	sql = sql[:-1]
	sql += ';'

	cur.execute(sql)
	conn.commit()
