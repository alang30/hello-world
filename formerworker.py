import configparser
import pandas as pd
#make sure to change below to folder containing the loadSQL.py
from conversion import formerworker
from conversion import workday
import sys
import csv
from pathlib import Path
import sqlalchemy
import os
import io
import xml.etree.ElementTree as ET

#make sure to change below to folder containing the loadSQL.py
def loadFormerWorker(engine):
	"""Sends request to Workday and parses response for the formerworker loads."""
	#make sure to change below to folder containing the loadSQL.py
	#request,order = formerworker.loadWorkday(engine)
	request,order = formerworker.loadWorkday(engine)
	print(request)
	response,ext = wd.loadAPI(request,'Former Worker',validate=1,bulk=1,maxErrors=1000)
		
	if len(response) != 0:
		with open('logs/Former Worker Workday Validation Report.csv','w',newline='') as csvfile:
			loadReport = csv.writer(csvfile,delimiter=',',quoting=csv.QUOTE_ALL)
			loadReport.writerow(['Result','Former Worker ID','Error'])
			for	index,value in enumerate(order):
					try:
						for error in response[index]:
							loadReport.writerow([error[0],value,error[1]])
					except(KeyError):
						loadReport.writerow(['Success',value])
						
		print('Workday validation complete - errors found. Please review Former Worker Workday Validation Report.csv')
	else:
		commit = input('Workday validation complete and no errors found - commit load? (Note - selecting N quits the process). Y/N\n')
		if commit.lower() == 'y':
			#change loadAPI request here
			response,ext = wd.loadAPI(request,'Former Worker',validate=0,bulk=1)
		
			with open('logs/Former Worker Workday Load Report.csv','w',newline='') as csvfile:
				loadReport = csv.writer(csvfile,delimiter=',',quoting=csv.QUOTE_ALL)
				loadReport.writerow(['Result','Former Worker ID','Error'])
				for	index,value in enumerate(order):
						try:
							for error in response[index]:
								loadReport.writerow([error[0],value,error[1]])
						except(KeyError):
							loadReport.writerow(['Success',value])

		
		print('Former Worker load complete - please review Former Worker Workday Load Report.csv')

def getPredefinedNameComponents(ns):
	request = '''<wd:Country_Predefined_Name_Component_Values_GetAll xmlns:wd="urn:com.workday/bsvc" wd:version="31.2"></wd:Country_Predefined_Name_Component_Values_GetAll>'''

	response = ET.fromstring(wd.commit(request,'HCM_Implementation_Service'))

	result = {}

	for child in response[0][0].findall('wd:Country_Predefined_Name_Component_Values_Data',ns):
		key = child.find('wd:ID',ns).text
		result[key] = []

		result[key].append(child.find('wd:Country_Reference/wd:Country_ISO_Code',ns).text)
		result[key].append(child.find('wd:Person_Name_Component_Type_Predefined_Reference/wd:ID[@wd:type="Predefined_Name_Component_Type_ID"]',ns).text)
		result[key].append(child.find('wd:Value',ns).text)

	return result

if __name__ == "__main__":

	configFileBool = Path("config.ini")
	if configFileBool.is_file():
		config = configparser.RawConfigParser()
		config.read('config.ini')
		try:
			psqlUsername = config.get('postgresql','user')
			psqlPassword = config.get('postgresql','password')
			psqlDatabase = config.get('postgresql','dbname')
			psqlHost = config.get('postgresql','host')
			username = config.get('workday','username')
			url = config.get('workday','url')
			password = config.get('workday','password')
		except Exception as e:
			print('Possible incorrect config file setup. Please double check and delete file for regeneration if necessary.\n')
			print(e)
		print(psqlUsername)
	else:
		cfgfile = open('config.ini','w')
		config = configparser.RawConfigParser()
		config.add_section('postgresql')
		config.set('postgresql','user','psqlUsername')
		config.set('postgresql','password','psqlPassword')
		config.set('postgresql','dbname','psqlDatabase')
		config.set('postgresql','host','psqlHost')
		config.add_section('workday')
		config.set('workday','username','usernameToTenant')
		config.set('workday','url','urlToTenant')
		config.set('workday','password','passwordForLogin')
		config.write(cfgfile)
		cfgfile.close()
		print('Config file created - please populate credentials and rerun load.')
		sys.exit()
	
	ns = {'wd':'urn:com.workday/bsvc','env':'http://schemas.xmlsoap.org/soap/envelope/'}		
	wd = workday.soap(username,password,url)
	engine = sqlalchemy.create_engine("postgresql+psycopg2://{0}:{1}@{2}:5432/{3}".format(psqlUsername,psqlPassword,psqlHost,psqlDatabase))
	
	params = "dbname={0} user={1} host={2} password={3}".format(psqlDatabase,psqlUsername,psqlHost,psqlPassword)
	
	#begins load from loadSQL.py house in onetimepay folder
	formerworker.loadSQL(params)

	
	doLoadReference = input('Refresh tables with Workday reference data? Y/N\n')
	if doLoadReference.lower() == 'y':
	
	#Call custom webservices to pull various ref IDs.
		print('Loading custom webservice references.')
		#Call getReferences and load various ref IDs.
		print('Loading standard references.')
		#Get Reference IDS for GEneral Event Subcat and One time Payment plan types
		formerworker.loadReference(wd.getReferences('Former_Worker_ID'),params,'get_former_worker_id')
		#formerworker.loadReference(wd.getReferences('formerworker_Usage_ID'),params,'get_formerworker_usage_id')
		formerworker.loadReference(wd.getReferences('Former_Worker_Type_ID'),params,'get_former_worker_type_id')
		formerworker.loadReference(wd.getReferences('Time_Profile_ID'),params,'get_time_profile_id')
		formerworker.loadReference(wd.getReferences('Locale_ID'),params,'get_locale_id')
		formerworker.loadReference(wd.getReferences('Language_ID'),params,'get_language_id')
		formerworker.loadReference(wd.getReferences('Currency_ID'),params,'get_currency_id')
		formerworker.loadReference(wd.getReferences('Country_Region_ID'),params,'get_country_region_id')
		formerworker.loadReference(wd.getReferences('Communication_Usage_Type_ID'),params,'get_communication_usage_type_id')
		#formerworker.loadReference(wd.getReferences('Communication_Usage_Behavior_ID'),params,'get_communication_usage_behavior_type_id')
		formerworker.loadReference(wd.getReferences('Phone_Device_Type_ID'),params,'get_phone_device_type_id')
		#formerworker.loadReference(doGetPayGroupformerworker(ns),params,'get_paygroup_formerworker_id')
		#formerworker.loadReference(doGetPayGroup(ns),params,'get_Org_id')
		#formerworker.loadReference(doGetWorkShift(ns),params,'get_workshift_id')
		#formerworker.loadReference(doGetformerworkerID(ns),params,'get_formerworker_id')
		#formerworker.loadReference(doGetformerworkerSiteID(ns),params,'get_formerworker_site_id')
		#formerworker.loadformerworkerSQL(wd.getReport('VAC_Workspace_Validation'),params)
		#formerworker.loadEmployeeTypeSQL(doGetEmployeeType(ns),params,'get_employee_type_id')
		#formerworker.loadJobClassificationSQL(doGetJobClassifications(ns),params,'get_job_class_id')
		#Call getReferences and load various ref IDs.
		#formerworker.loadReference(wd.getReferences('Employee_Type_ID'),params,'employee_type_id')
		formerworker.loadReference(wd.getReferences('Job_Profile_ID'),params,'get_job_profile_id')
		#formerworker.loadReference(wd.getReferences('formerworker_ID'),params,'formerworker_id')
		formerworker.loadReference(wd.getReferences('Position_Time_Type_ID'),params,'get_position_time_id')
		#formerworker.loadReference(wd.getReferences('Work_Shift_ID'),params,'work_shift_id')
		formerworker.loadReference(wd.getReferences('Pay_Rate_Type_ID'),params,'get_pay_rate_id')
		formerworker.loadReference(wd.getReferences('Company_Insider_Type_ID'),params,'get_company_insider_id')
		formerworker.loadReference(wd.getReferences('Workers_Compensation_ID'),params,'get_workers_compensation_id')
		formerworker.loadReference(wd.getReferences('General_Event_Subcategory_ID'),params,'get_general_event_subcat_id')
		formerworker.loadReference(wd.getReferences('Contingent_Worker_Type_ID'),params,'get_contingent_worker_type_id')
		formerworker.loadReference(wd.getReferences('Supplier_ID'),params,'get_supplier_id')
		#formerworker.loadReference(wd.getReferences('Integration_System_ID'),params,'formerworker_id_full')
		#formerworker.loadReference(wd.getReferences('Job_Classification_Reference_ID'),params,'job_class_id')
		#formerworker.loadReference(wd.getReferences('Organization_Reference_ID'),params,'org_reference_id')
		#formerworker.loadReference(wd.getReferences('Organization_Type_ID'),params,'org_type_id')
		#formerworker.loadReference(wd.getReferences('National_ID_Type_Code'),params,'issuing_agency')


	print('Running SQL Validations.')
	Errors = formerworker.validate(engine)
	
	#Errors
	if len(Errors) != 0:
		with open ('logs/Former Worker Validation Errors.csv','w',newline='') as csvfile:
			loadReport = csv.writer(csvfile,delimiter=',',quoting=csv.QUOTE_ALL)
			loadReport.writerow(['Key','Error'])
			for key, value in Errors.items():
				for msg in value:
					loadReport.writerow([key,msg])
		
		print('Errors found - please review former worker Validation Errors.csv')
	else:
		print('No validation errors found. Starting Workday load for former worker.')
		commit = input('Validate formerworkers to Workday? (Note - selecting N starts the load process). Y/N\n')
		if commit.lower() == 'y':
			loadFormerWorker(engine)
		