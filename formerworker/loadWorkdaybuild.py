import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import sqlalchemy
import requests
import sys
import datetime
import os
import time

# the variable "engine" is for the sqlalchemy module that connects the Pandas module to the SQL server on AWS

def loadWorkday(engine):
	#read the data from the table
	#DM - 1/12/18; need to declare each column for this SQL query due to title translation. See tt1."ID" and tt2."ID" in select statement below.

	formerworker = pd.read_sql_query("""SELECT  former_worker_id,pd.countrycode,tt1."id" as legal_title,legal_first_name,legal_middle_name,legal_last_name,legal_secondary_name,tt2."id" as legal_social_suffix,tt3."id" as legal_hereditary_suffix,legal_local_first_name,legal_local_first_name2,legal_local_middle_name,legal_local_last_name,legal_local_last_name2,legal_local_secondary_name,tt6."id" as preferred_title,preferred_first_name,preferred_middle_name,preferred_last_name,preferred_secondary_name,preferred_local_first_name,preferred_local_first_name2,preferred_local_middle_name,preferred_local_last_name,preferred_local_last_name2,preferred_local_secondary_name,tt4."id" as preferred_social_suffix,tt5."id" as preferred_hereditary_suffix

					FROM formerworkername as pd
					LEFT JOIN translate_title as tt1
					ON pd.countrycode = tt1."countrycode" and pd."legal_title" = tt1."value"
					LEFT JOIN translate_title as tt2
					ON pd.countrycode = tt2."countrycode" and pd."legal_social_suffix" = tt2."value"
					LEFT JOIN translate_title as tt3
                    ON pd.countrycode = tt3."countrycode" and pd."legal_hereditary_suffix" = tt3."value"
					LEFT JOIN translate_title as tt4
                    ON pd.countrycode = tt4."countrycode" and pd."preferred_social_suffix" = tt4."value"
					LEFT JOIN translate_title as tt5
                    ON pd.countrycode = tt5."countrycode" and pd."preferred_hereditary_suffix" = tt5."value"
					LEFT JOIN translate_title as tt6
					ON pd.countrycode = tt6."countrycode" and pd."preferred_title" = tt6."value"
					ORDER BY pd.former_worker_id ASC;""",engine)
	
	formerworker = formerworker.fillna('')
	job = pd.read_sql_query("""SELECT * FROM formerworkerjob;""",engine).fillna('')
	personal = pd.read_sql_query("""SELECT * FROM formerworkerpersonal;""",engine).fillna('')
	name = pd.read_sql_query("""SELECT * FROM formerworkername;""",engine).fillna('')
	address = pd.read_sql_query("""SELECT * FROM formerworkeraddress;""",engine).fillna('')
	phone = pd.read_sql_query("""SELECT * FROM formerworkerphone;""",engine).fillna('')
	email = pd.read_sql_query("""SELECT * FROM formerworkeremail;""",engine).fillna('')
	formerworkerws = '''<wd:Put_Former_Worker_Request xmlns:wd="urn:com.workday/bsvc" wd:version="v30.0">'''
	#print(formerworker)
	#print("\n---")
	#print(formerworker.iloc[0]['legalsuffix'])
	#print("\n---")
	#for loop steps row by row through the prehire data
	for x in range (0,len(formerworker.index)):
		#print(len(formerworker.index))
		#create variables for each cell of data in the row
		formerworkerws += '''<wd:Former_Worker_Data>'''

		formerworkerws += '''<wd:Former_Worker_ID>{0}</wd:Former_Worker_ID>'''.format(formerworker.iloc[x]['former_worker_id'])

		formerworkerws +='''<wd:Worker_Type_Reference> <wd:ID wd:type="Worker_Type_ID">{0}</wd:ID></wd:Worker_Type_Reference>'''.format(formerworker.iloc[x]['former_worker_id'])

        if job.iloc[x]['last_termination_date'] != '':
        	formerworkerws +='''<wd:Last_Termination_Date>{0}</wd:Last_Termination_Date>'''.format(job.iloc[x]['last_termination_date'])
        if job.iloc[x]['recent_hire_date'] != '':
        	formerworkerws +='''<wd:Most_Recent_Hire_Date>{0}</wd:Most_Recent_Hire_Date>'''.format(job.iloc[x]['recent_hire_date'])
        if job.iloc[x]['original_hire_date']:
        	formerworkerws +='''<wd:Original_Hire_Date>{0}</wd:Original_Hire_Date>'''.format(job.iloc[x]['original_hire_date'])
        if job.iloc[x]['continuous_service_date'] != '':
        	formerworkerws +='''<wd:Continuous_Service_Date>{0}</wd:Continuous_Service_Date>'''.format(job.iloc[x]['continuous_service_date'])
		#Start legal name section of webservice.
		formerworkerws += '''<wd:Personal_Information_Data>
		<wd:Name_Data>
			<wd:Legal_Name_Data>
				<wd:Name_Detail_Data>
					<wd:Country_Reference>
						<wd:ID wd:type="ISO_3166-1_Alpha-3_Code">{0}</wd:ID>
					</wd:Country_Reference>'''.format(formerworker.iloc[x]['countrycode'])
		
		formerworkerID = formerworker.iloc[x]['former_worker_id']
		
		if formerworker.iloc[x]['legal_title']!='':
			formerworkerws += '''<wd:Prefix_Data><wd:Title_Reference><wd:ID wd:type=\"Predefined_Name_Component_ID\">{0}</wd:ID></wd:Title_Reference></wd:Prefix_Data>'''.format(formerworker.iloc[x]['legaltitle'])
							
		formerworkerws += '''<wd:First_Name>{0}</wd:First_Name>'''.format(formerworker.iloc[x]['legal_first_name'])
		
		if formerworker.iloc[x]['legal_middle_name'] != '':
			formerworkerws += '''<wd:Middle_Name>{0}</wd:Middle_Name>'''.format(formerworker.iloc[x]['legal_middle_name'])
		
		formerworkerws += '''<wd:Last_Name>{0}</wd:Last_Name>'''.format(formerworker.iloc[x]['legal_last_name'])
		
		if formerworker.iloc[x]['legal_secondary_name'] != '':
			formerworkerws += '''<wd:Secondary_Last_Name>{0}</wd:Secondary_Last_Name>'''.format(formerworker.iloc[x]['legal_secondary_name'])
			
		if (formerworker.iloc[x]['legal_local_first_name']+formerworker.iloc[x]['legal_local_first_name2']+formerworker.iloc[x]['legal_local_middle_name']+formerworker.iloc[x]['legallocallastname']+formerworker.iloc[x]['legallocallastname2']+formerworker.iloc[x]['legallocalsecondaryname'] != ''):
			formerworkerws += '''<wd:Local_Name_Detail_Data>'''
			if (formerworker.iloc[x]['legal_local_first_name'] != ''):
				formerworkerws += '''<wd:First_Name>{0}</wd:First_Name>'''.format(formerworker.iloc[x]['legal_local_first_name'])
			
			if (formerworker.iloc[x]['legal_local_middle_name'] != ''):
				formerworkerws += '''<wd:Middle_Name>{0}</wd:Middle_Name>'''.format(formerworker.iloc[x]['legallocalmiddlename'])
			
			if formerworker.iloc[x]['legal_local_last_name'] != '':
				formerworkerws += '''<wd:Last_Name>{0}</wd:Last_Name>'''.format(formerworker.iloc[x]['legal_local_last_name'])
				
			if formerworker.iloc[x]['legal_local_secondary_name'] != '':
				formerworkerws += '''<wd:Secondary_Last_Name>{0}</wd:Secondary_Last_Name>'''.format(formerworker.iloc[x]['legal_local_secondary_name'])
				
			if formerworker.iloc[x]['legal_local_first_name2'] != '':
				formerworkerws += '''<wd:First_Name_2>{0}</wd:First_Name_2>'''.format(formerworker.iloc[x]['legal_local_first_name2'])
			
			#DM - 1/29 - Field to be added to the DGW.
			#if formerworker.iloc[x]['legallocalmiddlename2'] != '':
			#	formerworkerws += '''<wd:Middle_Name_2>{0}</wd:Middle_Name_2>'''.format(formerworker.iloc[x]['legallocalmiddlename2'])
			
			if formerworker.iloc[x]['legal_local_last_name2'] != '':
				formerworkerws += '''<wd:Last_Name_2>{0}</wd:Last_Name_2>'''.format(formerworker.iloc[x]['legal_local_last_name2'])
				
			formerworkerws += '''</wd:Local_Name_Detail_Data>'''
		
		if formerworker.iloc[x]['legal_social_suffix']+formerworker.iloc[x]['legal_hereditary_suffix'] != '':
			formerworkerws += '''<wd:Suffix_Data>'''
		
			if formerworker.iloc[x]['legal_social_suffix'] != '':
				formerworkerws += '''<wd:Social_Suffix_Reference><wd:ID wd:type="Predefined_Name_Component_ID">{0}</wd:ID></wd:Social_Suffix_Reference>'''.format(formerworker.iloc[x]['legal_social_suffix'])
			
			if formerworker.iloc[x]['legal_hereditary_suffix'] != '':
				formerworkerws += '''<wd:Hereditary_Suffix_Reference><wd:ID wd:type="Predefined_Name_Component_ID">{0}</wd:ID></wd:Hereditary_Suffix_Reference>'''.format(formerworker.iloc[x]['legal_hereditary_suffix'])
			
			formerworkerws += '''</wd:Suffix_Data>'''
		
		formerworkerws += '''</wd:Name_Detail_Data></wd:Legal_Name_Data>'''
		#End legal name section of webservice.
		#------------------------------------------		
		#Start preferred name section of webservice.
		if formerworker.iloc[x]['preferred_title']+formerworker.iloc[x]['preferred_first_name']+formerworker.iloc[x]['preferred_middle_name']+formerworker.iloc[x]['preferred_last_name']+formerworker.iloc[x]['preferred_secondary_name']+formerworker.iloc[x]['preferred_local_first_name']+formerworker.iloc[x]['preferred_local_first_name2']+formerworker.iloc[x]['preferred_local_middle_name']+formerworker.iloc[x]['preferred_local_last_name']+formerworker.iloc[x]['preferred_local_last_name2']+formerworker.iloc[x]['preferred_local_secondary_name']+formerworker.iloc[x]['preferred_social_suffix']+formerworker.iloc[x]['preferred_hereditary_suffix'] != '':
			formerworkerws += '''<wd:Preferred_Name_Data>
					<wd:Name_Detail_Data>
						<wd:Country_Reference>
							<wd:ID wd:type="ISO_3166-1_Alpha-3_Code">{0}</wd:ID>
						</wd:Country_Reference>'''.format(formerworker.iloc[x]['countrycode'])
					
			if formerworker.iloc[x]['preferred_title']!='':
				formerworkerws += '''<wd:Prefix_Data><wd:Title_Reference><wd:ID wd:type="Predefined_Name_Component_ID">{0}</wd:ID></wd:Title_Reference></wd:Prefix_Data>'''.format(formerworker.iloc[x]['preferred_title'])

			if formerworker.iloc[x]['preferred_first_name'] != '':
				formerworkerws += '''<wd:First_Name>{0}</wd:First_Name>'''.format(formerworker.iloc[x]['preferred_first_name'])

			if formerworker.iloc[x]['preferred_middle_name'] != '':
				formerworkerws += '''<wd:Middle_Name>{0}</wd:Middle_Name>'''.format(formerworker.iloc[x]['preferred_middle_name'])
		
			if formerworker.iloc[x]['preferred_last_name'] != '':
				formerworkerws += '''<wd:Last_Name>{0}</wd:Last_Name>'''.format(formerworker.iloc[x]['preferred_last_name'])

			if formerworker.iloc[x]['preferred_secondary_name'] != '':
				formerworkerws += '''<wd:Secondary_Last_Name>{0}</wd:Secondary_Last_Name>'''.format(formerworker.iloc[x]['preferred_secondary_name'])
		
			if formerworker.iloc[x]['preferred_local_first_name']+formerworker.iloc[x]['preferred_local_first_name2']+formerworker.iloc[x]['preferred_local_middle_name']+formerworker.iloc[x]['preferred_local_last_name']+formerworker.iloc[x]['preferred_local_last_name2']+formerworker.iloc[x]['preferred_local_secondary_name'] != '':
				formerworkerws += '''<wd:Local_Name_Detail_Data>'''
			
				if formerworker.iloc[x]['preferred_local_first_name'] != '':
					formerworkerws += '''<wd:First_Name>{0}</wd:First_Name>'''.format(formerworker.iloc[x]['preferred_local_first_name'])
				
				if formerworker.iloc[x]['preferred_local_middle_name'] != '':
					formerworkerws += '''<wd:Middle_Name>{0}</wd:Middle_Name>'''.format(formerworker.iloc[x]['preferred_local_middle_name'])
				
				if formerworker.iloc[x]['preferred_local_last_name'] != '':
					formerworkerws += '''<wd:Last_Name>{0}</wd:Last_Name>'''.format(formerworker.iloc[x]['preferred_local_last_name'])
					
				if formerworker.iloc[x]['preferred_local_secondary_name'] != '':
					formerworkerws += '''<wd:Secondary_Last_Name>{0}</wd:Secondary_Last_Name>'''.format(formerworker.iloc[x]['preferred_local_secondary_name'])
					
				if formerworker.iloc[x]['preferred_local_first_name2'] != '':
					formerworkerws += '''<wd:First_Name_2>{0}</wd:First_Name_2>'''.format(formerworker.iloc[x]['preferred_local_first_name2'])
				
				#DM - 1/29 - Field to be added to the DGW.
				#if formerworker.iloc[x]['preferredlocalmiddlename2'] != '':
				#	formerworkerws += '''<wd:Middle_Name_2>{0}</wd:Middle_Name_2>'''.format(formerworker.iloc[x]['preferredlocalmiddlename2'])
			
				if formerworker.iloc[x]['preferred_local_last_name2'] != '':
					formerworkerws += '''<wd:Last_Name_2>{0}</wd:Last_Name_2>'''.format(formerworker.iloc[x]['preferred_local_last_name2'])
			
				formerworkerws += '''</wd:Local_Name_Detail_Data>'''
			
			if formerworker.iloc[x]['preferred_social_suffix']+formerworker.iloc[x]['preferred_hereditary_suffix'] != '':
				formerworkerws += '''<wd:Suffix_Data>'''
				
				if formerworker.iloc[x]['preferredsocialsuffix'] != '':
					formerworkerws += '''<wd:Social_Suffix_Reference><wd:ID wd:type="Predefined_Name_Component_ID">{0}</wd:ID></wd:Social_Suffix_Reference>'''.format(formerworker.iloc[x]['preferred_social_suffix'])
				
				if formerworker.iloc[x]['preferred_hereditary_suffix'] != '':
					formerworkerws += '''<wd:Hereditary_Suffix_Reference><wd:ID wd:type="Predefined_Name_Component_ID">{0}</wd:ID></wd:Hereditary_Suffix_Reference>'''.format(formerworker.iloc[x]['preferred_hereditary_suffix'])
		
				formerworkerws += '''</wd:Suffix_Data>'''
	
			formerworkerws += '''</wd:Name_Detail_Data></wd:Preferred_Name_Data>'''
		formerworkerws += '''</wd:Name_Data>'''

		#Start contact data section of webservice.
		formerworkerws += '''<wd:Contact_Data>'''
		
		#@@@ - DM 1/27 - See below note on open item with Colin.
		#DM - 1/28 - Resolved.
		formerworkerws += parsePersonal(personal[])
		formerworkerws += parseAddress(address[address['former_worker_id'] == formerworkerID])
		formerworkerws += parsePhone(phone[phone['former_worker_id'] == formerworkerID])
		formerworkerws += parseEmail(email[email['former_worker_id'] == formerworkerID])
		
		formerworkerws += '''</wd:Contact_Data>'''

		formerworkerws += parseNationalID(personal[personal['former_worker_id'] == formerworkerID]))	
		formerworkerws += '''</wd:Personal_Data>'''

		if job.iloc[x]['last_termination_reason'] != '':
			formerworkerws += '''<wd:Last_Termination_Reason>{0}</wd:Last_Termination_Reason>'''.format(job.iloc[x]['last_termination_reason'])
		if job.iloc[x]['manager'] != '':
			formerworkerws += '''<wd:Manager>{0}</wd:Manager>'''.format(job.iloc[x]['manager'])
		if job.iloc[x]['cost_center'] != '':
        	formerworkerws += '''<wd:Cost_Center>{0}</wd:Cost_Center>'''.format(job.iloc[x]['cost_center'])
        if job.iloc[x]['job_title'] != '':
        	formerworkerws += '''<wd:Job_Title>{0}</wd:Job_Title>'''.format(job.iloc[x]['job_title'])
        if job.iloc[x]['job_code'] != '':
        	formerworkerws += '''<wd:Job_Code>{0}</wd:Job_Code>'''.format(job.iloc[x]['job_code'])
        if job.iloc[x]['job_profile'] != '':
        	formerworkerws += '''<wd:Job_Profile_Text>{0}</wd:Job_Profile_Text>'''.format(job.iloc[x]['job_profile'])
        if job.iloc[x]['job_level'] != '':
        	formerworkerws += '''<wd:Job_Level>{0}</wd:Job_Level>'''.format(job.iloc[x]['job_level'])
        if job.iloc[x]['time_type'] != '':
        	formerworkerws += '''<wd:Time_Type>{0}</wd:Time_Type>'''.format(job.iloc[x]['time_type'])
        if job.iloc[x]['location'] != '':
        	formerworkerws += '''<wd:Location>{0}</wd:Location>'''.format(job.iloc[x]['location'])
        if job.iloc[x]['base_pay'] != '':
        	formerworkerws += '''<wd:Base_Pay>{0}</wd:Base_Pay>'''.format(job.iloc[x]['base_pay'])
        if job.iloc[x]['currency'] != '':
        	formerworkerws += '''<wd:Currency_Reference><wd:ID wd:type="Currency_ID">{0}</wd:ID></wd:Currency_Reference>'''.format(job.iloc[x]['currency'])
        if job.iloc[x]['frequency'] != '':
        	formerworkerws += '''<wd:Frequency_Reference><wd:ID wd:type="Frequency_ID">{0}</wd:ID></wd:Frequency_Reference>'''.format(job.iloc[x]['frequency'])
        if job.iloc[x]['scheduled_weekly_hours'] != '':
        	formerworkerws += '''<wd:Scheduled_Weekly_Hours>{0}</wd:Scheduled_Weekly_Hours>'''.format(job.iloc[x]['scheduled_weekly_hours'])
        if job.iloc[x]['eligible_for_rehire'] != '':
	        formerworkerws += '''<wd:Eligible_for_Rehire_Reference>'''
	        if job.iloc[x]['eligible_for_rehire'].upper() = 'Y':
	        	formerworkerws += '''<wd:ID wd:type="Yes_No_Type_ID">Yes</wd:ID>'''
	        elif job.iloc[x]['eligible_for_rehire'].upper() = 'N':
	        	formerworkerws += '''<wd:ID wd:type="Yes_No_Type_ID">No</wd:ID>'''
        	formerworkerws += '''</wd:Eligible_for_Rehire_Reference>'''
        if job.iloc[x]['performance_rating'] != '':
        	formerworkerws += '''<wd:Performance_Rating>{0}</wd:Performance_Rating>'''.format(job.iloc[x]['performance_rating'])
        #formerworkerws += '''<wd:Former_Worker_Related_Data>'''
        #formerworkerws += '''<wd:Rehired>{0}</wd:Rehired>'''.format(job.iloc[x]['last_termination_reason'])
        #formerworkerws += '''<wd:Applicant_Reference><wd:ID wd:type="Applicant_ID">{0}</wd:ID></wd:Applicant_Reference>'''.format(job.iloc[x]['last_termination_reason'])
        #formerworkerws += '''<wd:Worker_Reference><wd:ID wd:type="Contingent_Worker_ID">{0}</wd:ID></wd:Worker_Reference>'''.format(job.iloc[x]['last_termination_reason'])
        #formerworkerws += '''</wd:Former_Worker_Related_Data>'''

		formerworkerws += '''</wd:Former_Worker_Data>'''

		#End individual formerworker personal data section of webservice.
		#-------------------------------------------
		#Start recruiting data section of webservice.

		#End recruiting data section of webservice.
		#------------------------------------------- 
		#Close out individual formerworker on webservice.	
		
		
		# The code below will be commented because it is not functional yet.
		#formerworkersource = ''
		#if formerworker.iloc[x]['formerworkersource']!='':
		#	formerworkersource = '<wd:Recruiting_Data><wd:formerworker_Source_Reference><wd:ID wd:type="formerworker_Source_ID">%s</wd:ID></wd:formerworker_Source_Reference></wd:Recruiting_Data>' % formerworker.iloc[x]['formerworkersource']
		
	#
	#print(formerworkerws)
	#push the web service string through to Workday via HTTPS POST
	"""r = requests.post('https://wd2-impl-services1.workday.com/ccx/service/Recruiting',data=formerworkerws,allow_redirects=True,headers={'X-Validate-Only':'1'})
	#print(r)
	
	#error handling
	if "faultcode" not in r.text:
		result = "SUCCESS"
		error=""
	else:
		#print("ERROR: %s not created or already exists" % formerworkerID)
		result = "ERROR"
		error = r.text.split("Validation error occurred.")
		error = error[1].split("</faultstring>")
		error = error[0]
	
	output = open("PreHire Load.csv",'a')
	output.write("\n%s,%s,%s" %(result,formerworkerID,error))
	output.close()"""
	formerworkerws += '''</wd:Put_Former_Worker_Request>'''
	formerworkerws = formerworkerws.replace('&','&amp;')
	f = open('webservice.xml','w')
	f.write(formerworkerws)
	f.close()
	return formerworkerws
		#print(r.text)

def parsePersonal(personal):
	output = ''
	if len(personal) > 0:
		for x in range(0,len(personal.index)):
			if personal.iloc[x]['dateofbirth'] != '':
				output += '''<wd:Birth_Date>{0}</wd:Birth_Date>'''.format(personal.iloc[x]['dateofbirth'])
            if personal.iloc[x]['ethnicity_code'] != '':
            	output += '''<wd:Ethnicity_Reference><wd:ID wd:type="Ethnicity_ID">{0}</wd:ID></wd:Ethnicity_Reference>'''.format(personal.iloc[x]['ethnicity_code'])
	        if personal.iloc[x]['hispanic_or_latino'] != '':	
	            if personal.iloc[x]['hispanic_or_latino'].upper() = 'Y':
	            	output += '''<wd:Hispanic_or_Latino>True</wd:Hispanic_or_Latino>'''
	            elif personal.iloc[x]['hispanic_or_latino'].upper() = 'N':
	            	output += '''<wd:Hispanic_or_Latino>False</wd:Hispanic_or_Latino>'''
            	
	return output

def parseNationalID(personal):
	output = ''
	if len(personal) > 0:
		for x in range(0,len(personal.index)):
			if personal.iloc[x]['national_id'] != '':
				output += '<wd:National_Identifier_Data wd:Delete="false">'
				output += '<wd:National_ID_Data>'
				output += '<wd:ID>{0}</wd:ID>'.format(personal.iloc[x]['national_id'])
				output += '<wd:ID_Type_Reference><wd:ID wd:type="National_ID_Type_Code">{0}</wd:ID></wd:ID_Type_Reference>'.format(personal.iloc[x]['national_id_type'])		
				output += '<wd:Country_Reference><wd:ID wd:type="ISO_3166-1_Alpha-3_Code">{0}</wd:ID></wd:Country_Reference>'.format(personal.iloc[x]['countrycode'])		
				if personal.iloc[x]['issue_date'] != '':
					output += '<wd:Issued_Date>{0}</wd:Issued_Date>'.format(personal.iloc[x]['issue_date'])
				if personal.iloc[x]['expiration_date'] != '':
					output += '<wd:Expiration_Date>{0}</wd:Expiration_Date>'.format(personal.iloc[x]['expiration_date'])
				if personal.iloc[x]['verification_date'] != '':
					output += '<wd:Verification_Date>{0}</wd:Verification_Date>'.format(personal.iloc[x]['verification_date'])
				if personal.iloc[x]['series'] != '':
					output += '<wd:Series>{0}</wd:Series>'.format(personal.iloc[x]['series'])
				if personal.iloc[x]['issuing_agency'] != '':
					output += '<wd:Issuing_Agency>{0}</wd:Issuing_Agency>'.format(personal.iloc[x]['issuing_agency'])
				output += '</wd:National_ID_Data>'
				output += '</wd:National_ID>'
	return output

def parseAddress(address):
	output = ''
	if len(address) > 0:
		for x in range(0,len(address.index)):
			output += '''<wd:Address_Data wd:Effective_Date='1900-01-01'>
			<wd:Country_Reference>
				<wd:ID wd:type="ISO_3166-1_Alpha-3_Code">{0}</wd:ID>
			</wd:Country_Reference>'''.format(address.iloc[x]['countrycode'])
			
			if address.iloc[x]['address_line_1'] != '':
				output += '''<wd:Address_Line_Data wd:Type='Address_Line_1'>{0}</wd:Address_Line_Data>'''.format(address.iloc[x]['address_line_1'])
			
			if address.iloc[x]['address_line_2'] != '':
				output += '''<wd:Address_Line_Data wd:Type='Address_Line_2'>{0}</wd:Address_Line_Data>'''.format(address.iloc[x]['address_line_2'])

			if address.iloc[x]['address_line_3'] != '':
				output += '''<wd:Address_Line_Data wd:Type='Address_Line_3'>{0}</wd:Address_Line_Data>'''.format(address.iloc[x]['address_line_3'])
			
			if address.iloc[x]['address_line_4'] != '':
				output += '''<wd:Address_Line_Data wd:Type='Address_Line_4'>{0}</wd:Address_Line_Data>'''.format(address.iloc[x]['address_line_4'])
				
			if address.iloc[x]['address_line_5'] != '':
				output += '''<wd:Address_Line_Data wd:Type='Address_Line_5'>{0}</wd:Address_Line_Data>'''.format(address.iloc[x]['address_line_5'])
			
			if address.iloc[x]['address_line_6'] != '':
				output += '''<wd:Address_Line_Data wd:Type='Address_Line_6'>{0}</wd:Address_Line_Data>'''.format(address.iloc[x]['address_line_6'])
			
			if address.iloc[x]['address_line_7'] != '':
				output += '''<wd:Address_Line_Data wd:Type='Address_Line_7'>{0}</wd:Address_Line_Data>'''.format(address.iloc[x]['address_line_7'])
			
			if address.iloc[x]['address_line_8'] != '':
				output += '''<wd:Address_Line_Data wd:Type='Address_Line_8'>{0}</wd:Address_Line_Data>'''.format(address.iloc[x]['address_line_8'])
			
			if address.iloc[x]['address_line_9'] != '':
				output += '''<wd:Address_Line_Data wd:Type='Address_Line_9'>{0}</wd:Address_Line_Data>'''.format(address.iloc[x]['address_line_9'])
			
			if address.iloc[x]['address_line_1_local'] != '':
				output += '''<wd:Address_Line_Data wd:Type='ADDRESS_LINE_1_LOCAL'>{0}</wd:Address_Line_Data>'''.format(address.iloc[x]['address_line_1_local'])
				
			if address.iloc[x]['address_line_2_local'] != '':
				output += '''<wd:Address_Line_Data wd:Type='ADDRESS_LINE_2_LOCAL'>{0}</wd:Address_Line_Data>'''.format(address.iloc[x]['address_line_2_local'])
			
			if address.iloc[x]['city'] != '':
				output += '''<wd:Municipality>{0}</wd:Municipality>'''.format(address.iloc[x]['city'])	
			
			if address.iloc[x]['city_subdivision1'] != '':
				output += '''<wd:Submunicipality_Data wd:Type='CITY_SUBDIVISION_1'>{0}</wd:Submunicipality_Data>'''.format(address.iloc[x]['city_subdivision1'])
			
			if address.iloc[x]['city_subdivision2'] != '':
				output += '''<wd:Submunicipality_Data wd:Type='CITY_SUBDIVISION_2'>{0}</wd:Submunicipality_Data>'''.format(address.iloc[x]['city_subdivision2'])
			
			if address.iloc[x]['region'] != '':
				output += '''<wd:Country_Region_Reference><wd:ID wd:type='Country_Region_ID'>{0}</wd:ID></wd:Country_Region_Reference>'''.format(address.iloc[x]['region'])

			if address.iloc[x]['region_subdivision1'] != '':
				output += '''<wd:Subregion_Data wd:Type='REGION_SUBDIVISION_1'>{0}</wd:Subregion_Data>'''.format(address.iloc[x]['region_subdivision1'])
			
			if address.iloc[x]['region_subdivision2'] != '':
				output += '''<wd:Subregion_Data wd:Type='REGION_SUBDIVISION_2'>{0}</wd:Subregion_Data>'''.format(address.iloc[x]['region_subdivision2'])
			
			if address.iloc[x]['postalcode'] != '':
				output += '''<wd:Postal_Code>{0}</wd:Postal_Code>'''.format(address.iloc[x]['postalcode'])
			
			if address.iloc[x]['usage'].upper() == 'WORK':
				public = 'true'
			else:
				public = 'false'
			
			output+='''<wd:Usage_Data wd:Public='{0}'><wd:Type_Data wd:Primary='{1}'><wd:Type_Reference><wd:ID wd:type="Communication_Usage_Type_ID">{2}</wd:ID></wd:Type_Reference></wd:Type_Data></wd:Usage_Data>'''.format(public,str(address.iloc[x]['primaryaddress']).lower(),address.iloc[x]['usage'].upper())
			#DM 1/27/2018 - Unsure how these will map to the web service fields. Need to speak with Colin.
			#DM 1/28/2018 - City == Municipality, region == Country_Region_Reference, all the rest fall to Address_Line_Data's
			
			if address.iloc[x]['city_local'] != '':
				output += '''<wd:Municipality_Local>{0}</wd:Municipality_Local>'''.format(address.iloc[x]['city_local'])
			
			output += '''</wd:Address_Data>'''
			
	return output

def parsePhone(phone):
	output = ''
	if len(phone) > 0:
		for x in range(0,len(phone.index)):
			output += '''<wd:Phone_Data>'''
			
			output += '''<wd:Country_ISO_Code>{0}</wd:Country_ISO_Code>'''.format(phone.iloc[x]['countrycode'])
			
			if phone.iloc[x]['international_phone_code'] != '':
				output += '''<wd:International_Phone_Code>{0}</wd:International_Phone_Code>'''.format(phone.iloc[x]['international_phone_code'])
			
			if phone.iloc[x]['area_code'] != '':
				output += '''<wd:Area_Code>{0}</wd:Area_Code>'''.format(phone.iloc[x]['area_code'])
			
			if phone.iloc[x]['phone_number'] != '':
				output += '''<wd:Phone_Number>{0}</wd:Phone_Number>'''.format(phone.iloc[x]['phone_number'])
			
			if phone.iloc[x]['phone_extension'] != '':
				output += '''<wd:Phone_Extension>{0}</wd:Phone_Extension>'''.format(phone.iloc[x]['phone_extension'])
				
			if phone.iloc[x]['device_type'] != '':
				output += '''<wd:Phone_Device_Type_Reference><wd:ID wd:type='Phone_Device_Type_ID'>{0}</wd:ID></wd:Phone_Device_Type_Reference>'''.format(phone.iloc[x]['device_type'])
			
			if phone.iloc[x]['communication_type'].upper() == 'WORK':
				public = 'true'
			else:
				public = 'false'
			
			output += '''<wd:Usage_Data wd:Public='{0}'><wd:Type_Data wd:Primary='{1}'><wd:Type_Reference><wd:ID wd:type="Communication_Usage_Type_ID">{2}</wd:ID></wd:Type_Reference></wd:Type_Data></wd:Usage_Data>'''.format(public,str(phone.iloc[x]['primary_phone']).lower(),phone.iloc[x]['communication_type'].upper())
			
			output += '''</wd:Phone_Data>'''
	
	return output

def parseEmail(email):
	output = ''
	if len(email) > 0:
		for x in range(0,len(email.index)):
			if email.iloc[x]['usage_type'].upper() == 'WORK':
				public = 'true'
			else:
				public = 'false'
			
			output += '''<wd:Email_Address_Data><wd:Email_Address>{0}</wd:Email_Address><wd:Usage_Data wd:Public='{1}'><wd:Type_Data wd:Primary='{2}'><wd:Type_Reference><wd:ID wd:type='Communication_Usage_Type_ID'>{3}</wd:ID></wd:Type_Reference></wd:Type_Data></wd:Usage_Data></wd:Email_Address_Data>'''.format(email.iloc[x]['email'],public,str(email.iloc[x]['primary']).lower(),email.iloc[x]['usage_type'].upper())
			
			
	return output



def parseJob(job):
	output = ''
	if len(job) > 0:
		for x in range(0,len(job.index)):
			

			
	return output