import pandas as pd
import sqlalchemy
import time



#def load():
#    start_time = time.time()
#    formerworkerfile = pd.read_excel('formerworker_Data.xlsx',sheet_name='formerworkerData')
#    addressdata = pd.read_excel('formerworker_Data.xlsx',sheet_name='formerworkerAddress')
#    phonedata = pd.read_excel('formerworker_Data.xlsx',sheet_name='formerworkerPhone')
#    iddata = pd.read_excel('formerworker_Data.xlsx',sheet_name='formerworkerID')
#    titleTranslate = pd.read_excel('Title_Data.xlsx',sheet_name='Sheet1')
#    
#    formerworkerfile.to_sql("formerworkerdata",engine,if_exists="replace")
#    addressdata.to_sql("addressdata",engine,if_exists="replace")
#    phonedata.to_sql("phonedata",engine,if_exists="replace")
#    iddata.to_sql("iddata",engine,if_exists="replace")
#    titleTranslate.to_sql("translate_title",engine,if_exists="replace")
#    print("--- %s seconds ---" % (time.time() - start_time))


def validate(engine):
    errors = {}
    start_time = time.time()

#===============================================
#VALIDATION: Does the Former Worker have an empty country code?
    try:
        formerworker = pd.read_sql_query("""SELECT *
                                        FROM formerworkername
                                        WHERE \"countrycode\" IS NULL OR \"countrycode\" = '';"""
                                        ,engine)
    except Exception as e:
        print(e)
        raise
   
    for index, row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker is missing country code.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker is missing country code.')

#================================================
#VALIDATION: Does the Former Worker have an empty legal first name?
#DM - 1/31 - Per Colin this is a duplicate.
    '''try:
        formerworker = pd.read_sql_query("""SELECT *
                                        FROM formerworkername
                                        WHERE \"legalfirstname\" is NULL OR \"legalfirstname\" = '';"""
                                        , engine)
    except Exception as e:
        print(e)
        raise

    for index, row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker is missing Legal First Name')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker is missing Legal First Name')'''


#================================================
#VALIDATION: Does the Former Worker have an invalid legal title?
#DM 1/30 - Artifact from before titles had a type associated with them.
    '''try:
        formerworker = pd.read_sql_query("""SELECT *
                                    FROM (SELECT *
                                        FROM formerworkername
                                        WHERE \"legal_title\" is not null) as ad
                                    LEFT JOIN translate_title as tt
                                    ON tt.\"value\" = ad.\"legal_title\"
                                        AND tt.\"countrycode\" = ad.\"countrycode\"
                                    WHERE tt.\"id\" is null;"""
                                    ,engine)
    except Exception as e:
        print(e)
        raise

    for index, row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker does not have a valid title.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('formerworker does not have a valid title.')'''

  #================================================
#VALIDATION: Does the Former Worker have an empty phone number?
    try:
        formerworker = pd.read_sql_query("""SELECT ad.\"former_worker_id\"
                                        FROM formerworkername as ad
                                        LEFT JOIN formerworkerphone as pd
                                        ON pd.\"former_worker_id\" = ad.\"former_worker_id\"
                                        WHERE pd.\"phone_number\" is null and pd.\"former_worker_id\" is not null;"""
                                        ,engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker has a phone data line with no phone number.')  
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker has a phone data line with no phone number.')  

#================================================
#VALIDATION: Does the Former Worker have a missing device type for phone?
    try:
        formerworker = pd.read_sql_query("""SELECT *
                                        FROM formerworkerphone
                                        WHERE \"device_type\" is null;"""
                                        ,engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker is missing a device type for their phone number.')  
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker is missing a device type for their phone number.')  

#================================================
# #VALIDATION: Does the Former Worker hav a missing communication type for phone?
#     try:
#         formerworker = pd.read_sql_query("""SELECT *
#                                         FROM formerworkerphone
#                                         WHERE \"usage_type\" is null;"""
#                                         ,engine)
#     except Exception as e:
#         print(e)

#     for index,row in formerworker.iterrows():
#         try:
#             errors[row['former_worker_id']].append('Former Worker is missing a communication type for their phone number.')  
#         except:
#             errors[row['former_worker_id']] = []
#             errors[row['former_worker_id']].append('Former Worker is missing a communication type for their phone number.')

#================================================			
#VALIDATION: Does the Former Worker have a missing primary flag for phone?
    try:
        formerworker = pd.read_sql_query("""SELECT pd.former_worker_id FROM formerworkerphone as pd
										WHERE pd.primaryphone IS NULL ;"""
                                                                        ,engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker is missing a primary flag for phone data.')  
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker is missing a primary flag for phone data.')

#================================================
#VALIDATION:Former Worker does not have exactly one primary phone number. 
    try:
        formerworker = pd.read_sql_query("""SELECT *
                                        FROM formerworkername as ad
                                        WHERE ad.\"former_worker_id\" in (SELECT \"former_worker_id\"
                                                                        FROM formerworkerphone as add
                                                                        GROUP BY \"former_worker_id\",\"usage_type\"
                                                                        HAVING count(case when add.\"primaryphone\" = 'True' then 1 else null end) <> 1);"""
                                                                        ,engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker does not have exactly one primary phone number per communication type.')  
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker does not have exactly one primary phone number per communication type.')

#================================================
#VALIDATION: Does the Former Worker exist in phone data but not in name data?
    try: formerworker = pd.read_sql_query("""SELECT \"former_worker_id\"
                                          FROM formerworkerphone
                                          WHERE former_worker_id NOT IN (SELECT former_worker_id from formerworkername);"""
					,engine)
    except Exception as e:
        print(e)


    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker appears in phone data tab with no entry in the Former Worker name tab.')  
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker appears in phone data tab with no entry in the Former Worker name tab.')

#================================================
#VALIDATION: Does the Former Worker exist in email data but not in name data?
    try: formerworker = pd.read_sql_query("""SELECT \"former_worker_id\"
					FROM formerworkeremail
					WHERE former_worker_id NOT IN (SELECT former_worker_id from formerworkername);"""
					,engine)
    except Exception as e:
        print(e)


    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker appears in email data tab with no entry in the Former Worker name tab.')  
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker appears in email data tab with no entry in the Former Worker name tab.')

#================================================
#VALIDATION: Does the Former Worker exist in address data but not in name data?
    try: formerworker = pd.read_sql_query("""SELECT \"former_worker_id\"
					FROM formerworkeraddress
					WHERE former_worker_id NOT IN (SELECT former_worker_id from formerworkername);"""
					,engine)
    except Exception as e:
        print(e)


    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker appears in address data tab with no entry in the Former Worker name tab.')  
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker appears in address data tab with no entry in the Former Worker name tab.')

#================================================
#VALIDATION: Does the Former Worker have no contact information?
    try:
        formerworker = pd.read_sql_query("""SELECT former_worker_id FROM formerworkername
					WHERE former_worker_id NOT IN (SELECT former_worker_id FROM formerworkeremail)
					AND former_worker_id NOT IN (SELECT former_worker_id FROM formerworkerphone)
					AND former_worker_id NOT IN (SELECT former_worker_id FROM formerworkeraddress);"""
					,engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker does not have any contact information.')  
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker does not have any contact information.')

# #================================================
# #VALIDATION: Does the Former Worker phone have a valid communication usage_type type?
#     try:
#         formerworker = pd.read_sql_query("""SELECT *
#                                         FROM (SELECT *
#                                             FROM formerworkerphone
#                                             WHERE \"usage_type\" is not null) as ad
#                                         LEFT JOIN get_communication_usage_type_type as tt
#                                         ON upper(tt.\"id\") = upper(ad.\"usage_type\")
#                                         WHERE tt.\"id\" is null;"""
#                                         ,engine)
#     except Exception as e:
#         print(e)

#     for index,row in formerworker.iterrows():
#         try:
#             errors[row['former_worker_id']].append('Former Worker does not have a valid communication usage_type type for their phone data.')  
#         except:
#             errors[row['former_worker_id']] = []
#             errors[row['former_worker_id']].append('Former Worker does not have a valid communication usage_type type for their phone data.')

#================================================
#VALIDATION: Does the Former Worker phone have a valid device type?
    # try:
    #     formerworker = pd.read_sql_query("""SELECT *
    #                                     FROM (SELECT *
    #                                         FROM formerworkerphone
    #                                         WHERE \"device_type\" is not null) as ad
    #                                     LEFT JOIN get_phone_device_type as tt
    #                                     ON upper(tt.\"id\") = upper(ad.\"device_type\")
    #                                     WHERE tt.\"id\" is null;"""
    #                                     ,engine)
    # except Exception as e:
    #     print(e)

    # for index,row in formerworker.iterrows():
    #     try:
    #         errors[row['former_worker_id']].append('Former Worker does not have a valid device type for their phone data.')  
    #     except:
    #         errors[row['former_worker_id']] = []
    #         errors[row['former_worker_id']].append('Former Worker does not have a valid device type for their phone data.')

#================================================
#VALIDATION: Does Former Worker have exactly one primary address per usage_type?
    try:
        formerworker = pd.read_sql_query("""SELECT *
                                        FROM formerworkername as ad
                                        WHERE ad.\"former_worker_id\" in (SELECT \"former_worker_id\"
                                                                    FROM formerworkeraddress as add
                                                                    GROUP BY \"former_worker_id\",\"usage_type\"
                                                                    HAVING count(case when add.\"primaryaddress\" = 'True' then 1 else null end) <> 1);"""
                                        ,engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker does not have exactly one primary address per usage_type.')  
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker does not have exactly one primary address per usage_type.')  

#================================================
#VALIDATION: Does Former Worker have a missing communication type for address?
#     try:
#         formerworker = pd.read_sql_query("""SELECT *
#                                         FROM formerworkeraddress
#                                         WHERE \"usage_type\" is null;"""
#                                         ,engine)
#     except Exception as e:
#         print(e)

#     for index,row in formerworker.iterrows():
#         try:
#             errors[row['former_worker_id']].append('Former Worker is missing a communication usage_type type for their address.')  
#         except:
#             errors[row['former_worker_id']] = []
#             errors[row['former_worker_id']].append('Former Worker is missing a communication usage_type type for their address.')

# #================================================
# #VALIDATION: Does Former Worker have a valid communication type for phone?
#     try:
#         formerworker = pd.read_sql_query("""SELECT *
#                                         FROM (SELECT *
#                                                 FROM formerworkeraddress
#                                                 WHERE \"usage_type\" is not null) as ad
#                                         LEFT JOIN get_communication_usage_type_type as tt
#                                         ON upper(tt.\"id\") = upper(ad.\"usage_type\")
#                                         WHERE tt.\"id\" is null;"""
#                                         ,engine)
#     except Exception as e:
#         print(e)

#     for index,row in formerworker.iterrows():
#         try:
#             errors[row['former_worker_id']].append('Former Worker does not have a valid communication usage_type type for their address data.')  
#         except:
#             errors[row['former_worker_id']] = []
#             errors[row['former_worker_id']].append('Former Worker does not have a valid communication usage_type type for their address data.')

# #================================================
# #VALIDATION: Does Former Worker have a communication type on their email?
#     try:
#         formerworker = pd.read_sql_query("""SELECT ad.\"former_worker_id\"
#                                         FROM formerworkername as ad
#                                         LEFT JOIN formerworkeremail as phed
#                                         ON phed.\"former_worker_id\" = ad.\"former_worker_id\"
#                                         WHERE phed.\"email\" IS NOT null
#                                             AND phed.\"usage_type\" IS null;"""
#                                         ,engine)
#     except Exception as e:
#         print(e)

#     for index,row in formerworker.iterrows():
#         try:
#             errors[row['former_worker_id']].append('Former Worker is missing a communication type on their email.')  
#         except:
#             errors[row['former_worker_id']] = []
#             errors[row['former_worker_id']].append('Former Worker is missing a communication type on their email.')  
#================================================
#VALIDATION: Does Former Worker have a 3 digit ISO countrycode in (Former Workeraddress)
    try:
        formerworker = pd.read_sql_query("""SELECT ad.*
                                        FROM formerworkername as ad
                                        INNER JOIN formerworkeraddress as add
                                        ON add.\"former_worker_id\" = ad.\"former_worker_id\"
                                        WHERE character_length(add.\"countrycode\") != 3;"""
                                        ,engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker does not have a 3 byte address country code.')  
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker does not have a 3 byte address country code.') 

#================================================            
#VALIDATION: Former Workers w/o 3 digit ISO countrycode in (formerworkername)
    try:
        formerworker = pd.read_sql_query("""SELECT ad.*
                                        FROM formerworkername as ad
                                        WHERE character_length(ad.\"countrycode\") != 3;"""
                                        ,engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker does not have a 3 byte country code in Former Worker name data.')  
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker does not have a 3 byte country code in Former Worker name data.') 
            
#================================================
#VALIDATION: formerworkers w/o 3 digit ISO countrycode in (formerworkerphone)
    try:
        formerworker = pd.read_sql_query("""SELECT ad.*
                                        FROM formerworkername as ad
                                        INNER JOIN formerworkerphone as pd
                                        ON pd.\"former_worker_id\" = ad.\"former_worker_id\"
                                        WHERE character_length(pd.\"countrycode\") != 3;"""
                                        ,engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker does not have a 3 byte phone country code.')  
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker does not have a 3 byte phone country code.')

#================================================
#VALIDATION: formerworkers w/o 1 primary email per usage_type            
    # try:
    #     formerworker = pd.read_sql_query("""SELECT * FROM formerworkername as ad
    #                                     WHERE ad.\"former_worker_id\" in 
    #                                     (SELECT \"former_worker_id\"
    #                                     FROM formerworkeremail as ed
    #                                     GROUP BY \"former_worker_id\",\"usage_type\"
    #                                     HAVING count(case when ed.\"primaryemail\" = 'True' then 1 else null end) <> 1);"""
    #                                     ,engine)
    # except Exception as e:
    #     print(e)

    # for index,row in formerworker.iterrows():
    #     try:
    #         errors[row['former_worker_id']].append('Former Worker does not have exactly one primary email address per communication type.')  
    #     except:
    #         errors[row['former_worker_id']] = []
    #         errors[row['former_worker_id']].append('Former Worker does not have exactly one primary email address per communication type.')

#================================================
#formerworker with missing primary email indicator
    try:
        formerworker = pd.read_sql_query("""SELECT *
                                    FROM formerworkeremail
                                    WHERE \"primaryemail\" is NULL;"""
                                    , engine)
  
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker email address missing primary indicator.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker email address missing primary indicator.')
            
#================================================
#formerworker email w/o valid format
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT distinct former_worker_id
                                      FROM formerworkeremail
                                      WHERE email !~ '^[A-Za-z0-9._%-]+@[A-Za-z0-9.-]+[.][A-Za-z]+$';""")
                                    , engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker email not in valid format ex. xxx@yyy.com')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker email not in valid format ex. xxx@yyy.com')

#================================================
# #formerworker email w/o valid usage_type type
#     try:
#         formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT DISTINCT former_worker_id
#                                                          FROM formerworkeremail 
#                                                          WHERE usage_type IS NOT NULL
#                                                          AND upper(usage_type) NOT IN(SELECT upper(tt."id") from get_communication_usage_type_type as tt);""")
#                                     , engine)
  
#     except Exception as e:
#         print(e)

#     for index,row in formerworker.iterrows():
#         try:
#             errors[row['former_worker_id']].append('Former Worker email has invalid usage_type type')
#         except:
#             errors[row['former_worker_id']] = []
#             errors[row['former_worker_id']].append('Former Worker email has invalid usage_type type')
			
#### LEGAL NAME VALIDATIONS START ####
#================================================
###Legal Title validity by country
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legaltitle = '-'
                                                                    and pd.legal_title IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal title is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal title is invalid for country.')

#================================================
#Legal title required or not.
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legaltitle = 'R'
                                                                    and pd.legal_title IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal title is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal title is required for country.')

#================================================
###Legal first name valid for country
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legalfirstname = '-'
                                                                    and pd.legal_first_name IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal first name is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal first name is invalid for country.')

#================================================
#Legal first name required for country.
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legalfirstname = 'R'
                                                                    and pd.legal_first_name IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal first name is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal first name is required for country.')

#================================================
##Legal middle name validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legalmiddlename = '-'
                                                                    and pd.legal_middle_name IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal middle name is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal middle name is invalid for country.')

#================================================
#Legal middle name required for country.
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legalmiddlename = 'R'
                                                                    and pd.legal_middle_name IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal middl ename is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal middle name is required for country.')

#================================================
###Legal last name validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legallastname = '-'
                                                                    and pd.legal_last_name IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal last name is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal last name is invalid for country.')

#================================================
#Legal last name required

    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legallastname = 'R'
                                                                    and pd.legal_last_name IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal last name is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal last name is required for country.')

#================================================
###Legal secondary name validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legalsecondaryname = '-'
                                                                    and pd.legal_secondary_name IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal secondary name is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal secondary name is invalid for country.')

#================================================
#Legal secondary name required for country
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legalsecondaryname = 'R'
                                                                    and pd.legal_secondary_name IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal secondary name is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal secondary name is required for country.')

#================================================
###Legal social suffix validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legalsocialsuffix = '-'
                                                                    and pd.legal_social_suffix IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal social suffix is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal social suffix is invalid for country.')

#================================================
#Legal social suffix required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legalsocialsuffix = 'R'
                                                                    and pd.legal_social_suffix IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal social suffix is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal social suffix is required for country.')

#================================================
###legal_hereditary_suffix validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legalhereditarysuffix = '-'
                                                                    and pd.legal_hereditary_suffix IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal hereditary suffix is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal hereditary suffix is invalid for country.')

#================================================
#Legal hereditary suffix required for country
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legalhereditarysuffix = 'R'
                                                                    and pd.legal_hereditary_suffix IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal hereditary suffix is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal hereditary suffix is required for country.')

#================================================
###legal_local_first_name validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legallocalfirstname = '-'
                                                                    and pd.legal_local_first_name IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal local first name is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal local first name is invalid for country.')

#================================================
#Legal local first name required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legallocalfirstname = 'R'
                                                                    and pd.legal_local_first_name IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal local first name is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal local first name is required for country.')

#================================================
###legal_local_first_name2 validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legallocalfirstname2 = '-'
                                                                    and pd.legal_local_first_name2 IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal local first name 2 is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal local first name 2 is invalid for country.')

#================================================
#Legal local first name 2 required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legallocalfirstname2 = 'R'
                                                                    and pd.legal_local_first_name2 IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal local first name 2 is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal local first name 2 is required for country.')

#===============================================
###legal_local_middle_name validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legallocalmiddlename = '-'
                                                                    and pd.legal_local_middle_name IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal local middle name is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal local middle name is invalid for country.')

#===============================================
#Legal local middlename required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legallocalmiddlename = 'R'
                                                                    and pd.legal_local_middle_name IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal local middle name is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal local middle name is required for country.')

#===============================================
###legal_local_last_name validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legallocallastname = '-'
                                                                    and pd.legal_local_last_name IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal local last name is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal local last name is invalid for country.')

#===============================================
#legal local last name required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legallocallastname = 'R'
                                                                    and pd.legal_local_last_name IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal local last name is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal local last name is required for country.')

#===============================================
###legal_local_last_name2 validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legallocallastname2 = '-'
                                                                    and pd.legal_local_last_name2 IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal local last name 2 is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal local last name 2 is invalid for country.')

#===============================================
#legal local last name 2 required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legallocallastname2 = 'R'
                                                                    and pd.legal_local_last_name2 IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal local last name 2 is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal local last name 2 is required for country.')

#===============================================
###legal_local_secondary_name validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legallocalsecondaryname = '-'
                                                                    and pd.legal_local_secondary_name IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal local secondary name is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal local secondary name is invalid for country.')

#===============================================
#legal local secondary name required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd
                                                                    INNER JOIN name_requirements as nr
                                                                    on pd.countrycode = nr.countrycode
                                                                    where nr.legallocalsecondaryname = 'R'
                                                                    and pd.legal_local_secondary_name IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal local secondary name is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal local secondary name is required for country.')            
#### END LEGAL NAME VALIDATIONS ####
#===============================================
#===============================================
#### START PHONE VALIDATIONS ####

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^\d{2}$' AND ppd.area_code !~ '^\d{2}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 2 digits, no special characters required.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 2 digits, no special characters required.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^[0]?\d{1,3}$' AND ppd.area_code !~ '^[0]?\d{1,3}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 1-3 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 1-3 digits, no special characters, may be preceded by 0.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^\d{3}$' AND ppd.area_code !~ '^\d{3}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 3 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 3 digits, no special characters.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^$' AND ppd.area_code !~ '^$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - area code not used.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - area code not used.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^\d{2,3}$' AND ppd.area_code !~ '^\d{2,3}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 2-3 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 2-3 digits, no special characters.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^[0]?\d{2,4}$' AND ppd.area_code !~ '^[0]?\d{2,4}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 2-4 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 2-4 digits, no special characters, may be preceded by 0.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^[0]?\d$' AND ppd.area_code !~ '^[0]?\d$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 1 digit, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 1 digit, no special characters, may be preceded by 0.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^\d{1,4}$' AND ppd.area_code !~ '^\d{1,4}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 1-4 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 1-4 digits, no special characters.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^\d{2,4}$' AND ppd.area_code !~ '^\d{2,4}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 2-4 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 2-4 digits, no special characters.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^[0]?[4]?\d{0,2}$' AND ppd.area_code !~ '^[0]?[4]?\d{0,2}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 0-2 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 0-2 digits, no special characters, may be preceded by 0.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^\d$' AND ppd.area_code !~ '^\d$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 1 digit, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 1 digit, no special characters.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^[0]?\d{2}$' AND ppd.area_code !~ '^[0]?\d{2}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 2 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 2 digits, no special characters, may be preceded by 0.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^[0]?\d{1,5}$' AND ppd.area_code !~ '^[0]?\d{1,5}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 1-5 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 1-5 digits, no special characters, may be preceded by 0.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^\d{1,2}$' AND ppd.area_code !~ '^\d{1,2}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 1-2 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 1-2 digits, no special characters.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^\d{1,3}$' AND ppd.area_code !~ '^\d{1,3}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 1-3 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 1-3 digits, no special characters.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^[0]?\d{1,2}$' AND ppd.area_code !~ '^[0]?\d{1,2}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 1-2 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 1-2 digits, no special characters, may be preceded by 0.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^\d{1}$' AND ppd.area_code !~ '^\d{1}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 1 digit, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 1 digit, no special characters.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^[0]?\d$' AND ppd.area_code !~ '^[0]?\d$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 1 digit, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 1 digit, may be preceded by 0.')
			
#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^[0]?\d{2,5}$' AND ppd.area_code !~ '^[0]?\d{2,5}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 2-5 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 2-5 digits, no special characters, may be preceded by 0.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^\d{3,4}$' AND ppd.area_code !~ '^\d{3,4}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 3-4 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 3-4 digits, no special characters/')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^[0]?\d{2,3}$' AND ppd.area_code !~ '^[0]?\d{2,3}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 2-3 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 2-3 digits, no special characters, may be preceded by 0.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^\d{0,3}$' AND ppd.area_code !~ '^\d{0,3}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 0-3 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 0-3 digits, no special characters.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^[0]?[6]?\d{2,3}$' AND ppd.area_code !~ '^[0]?[6]?\d{2,3}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 2-3 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 2-3 digits, no special characters, may be preceded by 0.')

#===============================================
### area code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.areacode_regex = '^[0]?\d{3}$' AND ppd.area_code !~ '^[0]?\d{3}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid area code for country - 3 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid area code for country - 3 digits, no special characters, may be preceded by 0.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{7}$' AND ppd.phone_number !~ '^\d{7}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 7 digits only, no special characters.') 
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 7 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{4,7}$' AND ppd.phone_number !~ '^\d{4,7}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 4-7 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 4-7 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{6}$' AND ppd.phone_number !~ '^\d{6}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 6 digits only, no special characters.') 
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 6 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^(\d{3})[-\.]?(\d{4})$' AND ppd.phone_number !~ '^(\d{3})[-\.]?(\d{4})$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 3 digits + optional dash (or dot) + 4 digits.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 3 digits + optional dash (or dot) + 4 digits.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{6,7}$' AND ppd.phone_number !~ '^\d{6,7}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 6-7 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 6-7 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{6,8}$' AND ppd.phone_number !~ '^\d{6,8}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 6-8 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 6-8 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{5,6}$' AND ppd.phone_number !~ '^\d{5,6}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 5-6 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 5-6 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{8}$' AND ppd.phone_number !~ '^\d{8}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 8 digits only, no special characters.') 
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 8 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{4,13}$' AND ppd.phone_number !~ '^\d{4,13}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 4-13 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 4-13 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{5,7}$' AND ppd.phone_number !~ '^\d{5,7}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 5-7 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 5-7 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{3,8}$' AND ppd.phone_number !~ '^\d{3,8}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 3-8 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 3-8 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{6,9}$' AND ppd.phone_number !~ '^\d{6,9}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 6-9 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 6-9 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{8,9}$' AND ppd.phone_number !~ '^\d{8,9}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 8-9 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 8-9 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{3,7}$' AND ppd.phone_number !~ '^\d{3,7}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 3-7 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 3-7 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{7,8}$' AND ppd.phone_number !~ '^\d{7,8}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 7-8 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 7-8 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^(\d{3,4})[-]?(\d{3,7})$' AND ppd.phone_number !~ '^(\d{3,4})[-]?(\d{3,7})$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 3-4 digits + optional dash + 3-7 digits')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 3-4 digits + optional dash + 3-7 digits')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{5}$' AND ppd.phone_number !~ '^\d{5}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 5 digits only, no special characters.') 
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 5 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{6,7}$' AND ppd.phone_number !~ '^\d{6,7}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 6-7 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 6-7 digits, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{6}$|^\d{8}$' AND ppd.phone_number !~ '^\d{6}$|^\d{8}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 6 or 8 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 6 or 8 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{5,8}$' AND ppd.phone_number !~ '^\d{5,8}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 5-8 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 5-8 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{4,8}$' AND ppd.phone_number !~ '^\d{4,8}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 4-8 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 4-8 digits only, no special characters.')

#===============================================
### phone number country validation
    # try:
    #     formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
    #                                                                 JOIN formerworkerphone ppd
    #                                                                 ON ppd.former_worker_id = pd.former_worker_id
    #                                                                 JOIN phone_requirements pr
    #                                                                 ON pr.countrycode = ppd.countrycode
    #                                                                 WHERE upper(ppd.device_type) != 'MOBILE'
    #                                                                 AND pr.phonenumber_regex = '^\d{8,10}$' AND ppd.phone_number !~ '^\d{8,10}$';"""),engine)
    # except Exception as e:
    #     print(e)

    # for index,row in formerworker.iterrows():
    #     try:
    #         errors[row['former_worker_id']].append('Invalid phone number for country - 8-10 digits only, no special characters.')
    #     except:
    #         errors[row['former_worker_id']] = []
    #         errors[row['former_worker_id']].append('Invalid phone number for country - 8-10 digits only, no special characters.')
# adsf
#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{3,11}$' AND ppd.phone_number !~ '^\d{3,11}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 3-11 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 3-11 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{4}$' AND ppd.phone_number !~ '^\d{4}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 4 digits only, no special characters.') 
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 4 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{6,10}$' AND ppd.phone_number !~ '^\d{6,10}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 6-10 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 6-10 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{6,12}$' AND ppd.phone_number !~ '^\d{6,12}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 6-12 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 6-12 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^(\d{2,4})[-]?(\d{2,4})$' AND ppd.phone_number !~ '^(\d{2,4})[-]?(\d{2,4})$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 2-4 digits + optional dash + 2-4 digits')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 2-4 digits + optional dash + 2-4 digits')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{7,9}$' AND ppd.phone_number !~ '^\d{7,9}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 7-9 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 7-9 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^(\d{3})[-]?(\d{4,7})$' AND ppd.phone_number !~ '^(\d{3})[-]?(\d{4,7})$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 3 digits + optional dash + 4-7 digits.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 3 digits + optional dash + 4-7 digits.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{9}$' AND ppd.phone_number !~ '^\d{9}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 9 digits only, no special characters.') 
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 9 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{4,6}$' AND ppd.phone_number !~ '^\d{4,6}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 4-6 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 4-6 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{7}|\d{9}$' AND ppd.phone_number !~ '^\d{7}|\d{9}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 7 or 9 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 7 or 9 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{10}$' AND ppd.phone_number !~ '^\d{10}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 10 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 10 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^\d{7,10}$' AND ppd.phone_number !~ '^\d{7,10}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 7-10 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 7-10 digits only, no special characters.')

#===============================================
### phone number country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) != 'MOBILE'
                                                                    AND pr.phonenumber_regex = '^(\d{2,3})[-\s]?(\d{2,3})[-\s]?(\d{2,3})[-\s]?(\d{1,2})$' AND ppd.phone_number !~ '^(\d{2,3})[-\s]?(\d{2,3})[-\s]?(\d{2,3})[-\s]?(\d{1,2})$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid phone number for country - 2-3 digits + optional dash (or space) + 2-3 digits + optional dash (or space) + 2-3 digits + optional dash (or space) + 1-2 digits.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid phone number for country - 2-3 digits + optional dash (or space) + 2-3 digits + optional dash (or space) + 2-3 digits + optional dash (or space) + 1-2 digits.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^\d{2,3}$' AND ppd.area_code !~ '^\d{2,3}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 2-3 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 2-3 digits, no special characters.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^[0]?\d{1,3}$' AND ppd.area_code !~ '^[0]?\d{1,3}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1-3 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1-3 digits, no special characters, may be preceded by 0.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^\d{1,3}$' AND ppd.area_code !~ '^\d{1,3}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1-3 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1-3 digits, no special characters.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^\d{3}$' AND ppd.area_code !~ '^\d{3}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 3 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 3 digits, no special characters.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^$' AND ppd.area_code !~ '^$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - area code not used.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - area code not used.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^[0]?\d{2,4}$' AND ppd.area_code !~ '^[0]?\d{2,4}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 2-4 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 2-4 digits, no special characters, may be preceded by 0.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^\d{2}$' AND ppd.area_code !~ '^\d{2}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 2 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 2 digits, no special characters.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^[0]?\d$' AND ppd.area_code !~ '^[0]?\d$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1 digit, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1 digit, no special characters, may be preceded by 0.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^\d{1,4}$' AND ppd.area_code !~ '^\d{1,4}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1-4 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1-4 digits, no special characters.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^\d{2,4}$' AND ppd.area_code !~ '^\d{2,4}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 2-4 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 2-4 digits, no special characters.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^[0]?[4]?\d{0,2}$' AND ppd.area_code !~ '^[0]?[4]?\d{0,2}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 0-2 digits, no special characters, may be preceded by 04.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 0-2 digits, no special characters, may be preceded by 04.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^\d$' AND ppd.area_code !~ '^\d$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1 digit, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1 digit, no special characters.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^[0]?\d{2}$' AND ppd.area_code !~ '^[0]?\d{2}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 2 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 2 digits, no special characters, may be preceded by 0.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^[0]?\d{2,3}$' AND ppd.area_code !~ '^[0]?\d{2,3}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 2-3 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 2-3 digits, no special characters, may be preceded by 0.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^\d{1,2}$' AND ppd.area_code !~ '^\d{1,2}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1-2 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1-2 digits, no special characters.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^[0]?\d{1,2}$' AND ppd.area_code !~ '^[0]?\d{1,2}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1-2 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1-2 digits, no special characters, may be preceded by 0.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^\d{1}$' AND ppd.area_code !~ '^\d{1}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1 digit, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1 digit, no special characters.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^[0]?\d$' AND ppd.area_code !~ '^[0]?\d$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1 digit, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1 digit, may be preceded by 0.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^\d{3,4}$' AND ppd.area_code !~ '^\d{3,4}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 3-4 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 3-4 digits, no special characters.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^[0]?\d{2,5}$' AND ppd.area_code !~ '^[0]?\d{2,5}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 2-5 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 2-5 digits, no special characters, may be preceded by 0.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^[0]?\d{3}$' AND ppd.area_code !~ '^[0]?\d{3}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 3 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 3 digits, no special characters, may be preceded by 0.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^[0]?\d{1,4}$' AND ppd.area_code !~ '^[0]?\d{1,4}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1-4 digits, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 1-4 digits, no special characters, may be preceded by 0.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^\d{0,3}$' AND ppd.area_code !~ '^\d{0,3}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 0-3 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 0-3 digits, no special characters.')

#===============================================
### mobile area_code country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_areacode_regex = '^[0]?[6]?\d{2,3}$|^[6]$' AND ppd.area_code !~ '^[0]?[6]?\d{2,3}$|^[6]$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 2-3 digits (or 1 digit if the digit is 6), no special characters, may be preceded by 06.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Mobile area code invalid for country - 2-3 digits (or 1 digit if the digit is 6), no special characters, may be preceded by 06.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{6,7}$' AND ppd.phone_number !~ '^\d{6,7}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6-7 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6-7 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{4,7}$' AND ppd.phone_number !~ '^\d{4,7}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 4-7 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 4-7 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{6,8}$' AND ppd.phone_number !~ '^\d{6,8}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6-8 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6-8 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^(\d{3})[-\.]?(\d{4})$' AND ppd.phone_number !~ '^(\d{3})[-\.]?(\d{4})$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 3 digits + optional dash (or dot) + 4 digits.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 3 digits + optional dash (or dot) + 4 digits.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{6}$' AND ppd.phone_number !~ '^\d{6}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^[0]?\d{8,9}$' AND ppd.phone_number !~ '^[0]?\d{8,9}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 8-9 digits only, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 8-9 digits only, no special characters, may be preceded by 0.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{4,13}$' AND ppd.phone_number !~ '^\d{4,13}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 4-13 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 4-13 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{7}$' AND ppd.phone_number !~ '^\d{7}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 7 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 7 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{8}$' AND ppd.phone_number !~ '^\d{8}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 8 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 8 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{4,8}$' AND ppd.phone_number !~ '^\d{4,8}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 4-8 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 4-8 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{6,9}$' AND ppd.phone_number !~ '^\d{6,9}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6-9 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6-9 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{8,9}$' AND ppd.phone_number !~ '^\d{8,9}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 8-9 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 8-9 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^(\d{3,4})[-]?(\d{3,7})$' AND ppd.phone_number !~ '^(\d{3,4})[-]?(\d{3,7})$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 3-4 digits + optional dash + 3-7 digits.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 3-4 digits + optional dash + 3-7 digits.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{5}$' AND ppd.phone_number !~ '^\d{5}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 5 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 5 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{6,7}$' AND ppd.phone_number !~ '^\d{6,7}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6-7 digits, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6-7 digits, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{7,8}$' AND ppd.phone_number !~ '^\d{7,8}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 7-8 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 7-8 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{5,8}$' AND ppd.phone_number !~ '^\d{5,8}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 5-8 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 5-8 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{8,10}$' AND ppd.phone_number !~ '^\d{8,10}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 8-10 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 8-10 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{5,6}$' AND ppd.phone_number !~ '^\d{5,6}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 5-6 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 5-6 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{6,11}$' AND ppd.phone_number !~ '^\d{6,11}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6-11 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6-11 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{3,7}$' AND ppd.phone_number !~ '^\d{3,7}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 3-7 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 3-7 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{4}$' AND ppd.phone_number !~ '^\d{4}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 4 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 4 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{6,10}$' AND ppd.phone_number !~ '^\d{6,10}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6-10 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6-10 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{6,12}$' AND ppd.phone_number !~ '^\d{6,12}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6-12 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6-12 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{5,7}$' AND ppd.phone_number !~ '^\d{5,7}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 5-7 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 5-7 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^(\d{2,4})[-]?(\d{2,4})$' AND ppd.phone_number !~ '^(\d{2,4})[-]?(\d{2,4})$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 2-4 digits + optional dash + 2-4 digits.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 2-4 digits + optional dash + 2-4 digits.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{7,9}$' AND ppd.phone_number !~ '^\d{7,9}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 7-9 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 7-9 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^[0]?\d{6,10}$' AND ppd.phone_number !~ '^[0]?\d{6,10}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6-10 digits only, no special characters, may be preceded by 0.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 6-10 digits only, no special characters, may be preceded by 0.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^(\d{3})[-]?(\d{4,7})$' AND ppd.phone_number !~ '^(\d{3})[-]?(\d{4,7})$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 3 digits + optional dash + 4-7 digits.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 3 digits + optional dash + 4-7 digits.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{9}$' AND ppd.phone_number !~ '^\d{9}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 9 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 9 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{5,9}$' AND ppd.phone_number !~ '^\d{5,9}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 5-9 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 5-9 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{7}|\d{9}$' AND ppd.phone_number !~ '^\d{7}|\d{9}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 7 or 9 digits only, no special characters.') 
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 7 or 9 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{10}$' AND ppd.phone_number !~ '^\d{10}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 10 digits only, no special characters.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 10 digits only, no special characters.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^(\d{2,3})[-\s]?(\d{2,3})[-\s]?(\d{2,3})[-\s]?(\d{2})$' AND ppd.phone_number !~ '^(\d{2,3})[-\s]?(\d{2,3})[-\s]?(\d{2,3})[-\s]?(\d{2})$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 2-3 digits + optional dash (or space) + 2-3 digits + optional dash (or space) + 2-3 digits + optional dash (or space) + 2 digits.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 2-3 digits + optional dash (or space) + 2-3 digits + optional dash (or space) + 2-3 digits + optional dash (or space) + 2 digits.')

#===============================================
### mobile phone country validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername pd
                                                                    JOIN formerworkerphone ppd
                                                                    ON ppd.former_worker_id = pd.former_worker_id
                                                                    JOIN phone_requirements pr
                                                                    ON pr.countrycode = ppd.countrycode
                                                                    WHERE upper(ppd.device_type) = 'MOBILE'
                                                                    AND pr.mobile_phonenumber_regex = '^\d{5,8}$|^[0]?\d{10}$' AND ppd.phone_number !~ '^\d{5,8}$|^[0]?\d{10}$';"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 5-8 or 10 digits, no special characters, may be preceded by 0 if followed by 10 digits.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Invalid mobile phone number for country - 5-8 or 10 digits, no special characters, may be preceded by 0 if followed by 10 digits.')
#### END PHONE NUMBER VALIDATIONS ####
#===============================================
#===============================================
#### START PREFERRED NAME VALIDATIONS ####

#===============================================
###preferred_title validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legaltitle = '-'
                                                            and pd.preferred_title IS NOT NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred title is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred title is invalid for country.')

#===============================================
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legaltitle = 'R'
                                                            and pd.preferred_title IS NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred title is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred title is required for country.')

#===============================================
###preferred_first_name validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legalfirstname = '-'
                                                            and pd.preferred_first_name IS NOT NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred first name is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred first name is invalid for country.')

#===============================================
#Preferred first name required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legalfirstname = 'R'
                                                            and pd.preferred_first_name IS NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred first name is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred first name is required for country.')

#===============================================
###preferred_middle_name validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legalmiddlename = '-'
                                                            and pd.preferred_middle_name IS NOT NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred middle name is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred middle name is invalid for country.')

#===============================================
#Preferred middle name required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legalmiddlename = 'R'
                                                            and pd.preferred_middle_name IS NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred middle name is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred middle name is required for country.')

#===============================================
###preferred_last_name validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legallastname = '-'
                                                            and pd.preferred_last_name IS NOT NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred last name is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred last name is invalid for country.')

#===============================================
#Preferred last name required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legallastname = 'R'
                                                            and pd.preferred_last_name IS NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred last name is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred last name is required for country.')

#===============================================
###preferred_secondary_name validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legalsecondaryname = '-'
                                                            and pd.preferred_secondary_name IS NOT NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred secondary name is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred secondary name is invalid for country.')

#===============================================
#Preferred secondary name required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legalsecondaryname = 'R'
                                                            and pd.preferred_secondary_name IS NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred secondary name is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred secondary name is required for country.')

#===============================================
###preferred_social_suffix validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legalsocialsuffix = '-'
                                                            and pd.preferred_social_suffix IS NOT NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred social suffix is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred social suffix is invalid for country.')

#===============================================
#Preferred social suffix required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legalsocialsuffix = 'R'
                                                            and pd.preferred_social_suffix IS NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred social suffix is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred social suffix is required for country.')

#===============================================
###preferred_hereditary_suffix validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legalhereditarysuffix = '-'
                                                            and pd.preferred_hereditary_suffix IS NOT NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred hereditary suffix is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred hereditary suffix is invalid for country.')

#===============================================
#Preferred hereditary suffix required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legalhereditarysuffix = 'R'
                                                            and pd.preferred_hereditary_suffix IS NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred hereditary suffix is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred hereditary suffix is required for country.')

#===============================================
###preferred_local_first_name validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legallocalfirstname = '-'
                                                            and pd.preferred_local_first_name IS NOT NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred local first name is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred local first name is invalid for country.')

#================================================
#Preferred local first name required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legallocalfirstname = 'R'
                                                            and pd.preferred_local_first_name IS NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred local first name is required for country')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred local first name is required for country')

#================================================
###preferred_local_first_name validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legallocalfirstname = '-'
                                                            and pd.preferred_local_first_name IS NOT NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred local first name is invalid for country')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred local first name is invalid for country')
			
#===============================================
#Preferred local first name required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legallocalfirstname = 'R'
                                                            and pd.preferred_local_first_name IS NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred local first name is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred local first name is required for country.')

#===============================================
###preferred_local_middle_name validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legallocalmiddlename = '-'
                                                            and pd.preferred_local_middle_name IS NOT NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred local middle name is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred local middle name is invalid for country.')

#===============================================
#Preferred local middle name required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legallocalmiddlename = 'R'
                                                            and pd.preferred_local_middle_name IS NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred local middle name is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred local middle name is required for country.')

#===============================================
###preferred_local_last_name validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legallocallastname = '-'
                                                            and pd.preferred_local_last_name IS NOT NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred local last name is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred local last name is invalid for country.')

#===============================================
#Preferred local last name
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legallocallastname = 'R'
                                                            and pd.preferred_local_last_name IS NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred local last name is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred local last name is required for country.')

#===============================================
###preferred_local_last_name2 validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legallocallastname2 = '-'
                                                            and pd.preferred_local_last_name2 IS NOT NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred local last name 2 is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred local last name 2 is invalid for country.')

#===============================================
#Preferred local last name 2 required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legallocallastname2 = 'R'
                                                            and pd.preferred_local_last_name2 IS NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred local last name 2 is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred local last name 2 is required for country.')

#===============================================
###preferred_local_secondary_name validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legallocalsecondaryname = '-'
                                                            and pd.preferred_local_secondary_name IS NOT NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred local secondary name is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred local secondary name is invalid for country.')

#===============================================
#Preferred local secondary name required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT T1.former_worker_id FROM
                                                            (SELECT former_worker_id FROM formerworkername
                                                            WHERE preferred_title IS NOT NULL OR preferred_title != ''
                                                            OR preferred_first_name IS NOT NULL OR preferred_first_name != ''
                                                            OR preferred_middle_name IS NOT NULL OR preferred_middle_name != ''
                                                            OR preferred_last_name IS NOT NULL OR preferred_last_name != ''
                                                            OR preferred_secondary_name IS NOT NULL OR preferred_secondary_name != ''
                                                            OR preferred_local_first_name IS NOT NULL OR preferred_local_first_name != ''
                                                            OR preferred_local_first_name2 IS NOT NULL OR preferred_local_first_name2 != ''
                                                            OR preferred_local_middle_name IS NOT NULL OR preferred_local_middle_name != ''
                                                            OR preferred_local_last_name IS NOT NULL OR preferred_local_last_name != ''
                                                            OR preferred_local_last_name2 IS NOT NULL OR preferred_local_last_name2 != ''
                                                            OR preferred_local_secondary_name IS NOT NULL OR preferred_local_secondary_name != ''
                                                            OR preferred_social_suffix IS NOT NULL OR preferred_social_suffix != ''
                                                            OR preferred_hereditary_suffix IS NOT NULL OR preferred_hereditary_suffix != ''
                                                            ) T1
                                                            JOIN
                                                            (SELECT pd.former_worker_id FROM formerworkername pd
                                                            JOIN name_requirements as nr
                                                            on pd.countrycode = nr.countrycode
                                                            where nr.legallocalsecondaryname = 'R'
                                                            and pd.preferred_local_secondary_name IS NULL) T2
                                                            on T1.former_worker_id = T2.former_worker_id;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred local secondary name is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred local secondary name is required for country.')

#===============================================
#### END PREFERRED NAME VALIDATIONS ####
#===============================================
#===============================================
#### START ADDRESS VALIDATIONS ####
#===============================================
###address_line_1 validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline1 = '-'
                                                                    and pa.address_line_1 IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 1 is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 1 is invalid for country.')

#===============================================
###address_line_1 required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline1 = 'R'
                                                                    and pa.address_line_1 IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 1 is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 1 is required for country.')

#===============================================
###address_line_2 validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline2 = '-'
                                                                    and pa.address_line_2 IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 2 is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 2 is invalid for country.')

#===============================================
###address_line_2 required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline2 = 'R'
                                                                    and pa.address_line_2 IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 2 is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 2 is required for country.')

#===============================================
###city validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.city = '-'
                                                                    and pa.city IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field city is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field city is invalid for country.')

#===============================================
###city required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.city = 'R'
                                                                    and pa.city IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field city is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field city is required for country.')

#===============================================
###region validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.region = '-'
                                                                    and pa.region IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field region is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field region is invalid for country.')

#===============================================
###region required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.region = 'R'
                                                                    and pa.region IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field region is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field region is required for country.')

#===============================================
###postalcode validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.postalcode = '-'
                                                                    and pa.postalcode IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field postal code is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field postal code is invalid for country.')

#===============================================
###postalcode required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.postalcode = 'R'
                                                                    and pa.postalcode IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field postal code is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field postal code is required for country.')

#===============================================
###address_line_3 validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline3 = '-'
                                                                    and pa.address_line_3 IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 3 is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 3 is invalid for country.')

#===============================================
###address_line_3 required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline3 = 'R'
                                                                    and pa.address_line_3 IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 3 is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 3 is required for country.')

#===============================================
###address_line_4 validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline4 = '-'
                                                                    and pa.address_line_4 IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 4 is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 4 is invalid for country.')

#===============================================
###address_line_4 required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline4 = 'R'
                                                                    and pa.address_line_4 IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 4 is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 4 is required for country.')

#===============================================
###address_line_5 validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline5 = '-'
                                                                    and pa.address_line_5 IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 5 is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 5 is invalid for country.')

#===============================================
###address_line_5 required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline5 = 'R'
                                                                    and pa.address_line_5 IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 5 is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 5 is required for country.')

#===============================================
###address_line_6 validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline6 = '-'
                                                                    and pa.address_line_6 IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 6 is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 6 is invalid for country.')

#===============================================
###address_line_6 required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline6 = 'R'
                                                                    and pa.address_line_6 IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 6 is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 6 is required for country.')

#===============================================
###address_line_7 validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline7 = '-'
                                                                    and pa.address_line_7 IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 7 is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 7 is invalid for country.')

#===============================================
###address_line_7 required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline7 = 'R'
                                                                    and pa.address_line_7 IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 7 is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 7 is required for country.')

#===============================================
###address_line_8 validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline8 = '-'
                                                                    and pa.address_line_8 IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 8 is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 8 is invalid for country.')

#===============================================
###address_line_8 required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline8 = 'R'
                                                                    and pa.address_line_8 IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 8 is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 8 is required for country.')

#===============================================
###address_line_9 validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline9 = '-'
                                                                    and pa.address_line_9 IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 9 is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 9 is invalid for country.')

#===============================================
###address_line_9 required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline9 = 'R'
                                                                    and pa.address_line_9 IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 9 is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 9 is required for country.')

#===============================================
###city_subdivision1 validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.city_subdivision1 = '-'
                                                                    and pa.city_subdivision1 IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field city subdivision 1 is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field city subdivision 1 is invalid for country.')

#===============================================
###city_subdivision1 required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.city_subdivision1 = 'R'
                                                                    and pa.city_subdivision1 IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field city subdivision 1 is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field city subdivision 1 is required for country.')

#===============================================
###city_subdivision2 validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.city_subdivision2 = '-'
                                                                    and pa.city_subdivision2 IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field city subdivision 2 is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field city subdivision 2 is invalid for country.')

#===============================================
###city_subdivision2 required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.city_subdivision2 = 'R'
                                                                    and pa.city_subdivision2 IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field city subdivision 2 is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field city subdivision 2 is required for country.')

#===============================================
###region_subdivision1 validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.regionsubdivision1 = '-'
                                                                    and pa.region_subdivision1 IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field region subdivision 1 is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field region subdivision 1 is invalid for country.')

#===============================================
###region_subdivision1 required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.regionsubdivision1 = 'R'
                                                                    and pa.region_subdivision1 IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field region subdivision 1 is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field region subdivision 1 is required for country.')

#===============================================
###region_subdivision2 validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.regionsubdivision2 = '-'
                                                                    and pa.region_subdivision2 IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field region subdivision 2 is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field region subdivision 2 is invalid for country.')

#===============================================
###requiredsubdivision2 required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.regionsubdivision2 = 'R'
                                                                    and pa.region_subdivision2 IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field region subdivision 2 is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field region subdivision 2 is required for country.')

#===============================================
###address_line_1_local validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline1_local = '-'
                                                                    and pa.address_line_1_local IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 1 local is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 1 local is invalid for country.')

#===============================================
###address_line_1_local required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline1_local = 'R'
                                                                    and pa.address_line_1_local IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 1 local is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 1 local is required for country.')

#===============================================
###address_line_2_local validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline2_local = '-'
                                                                    and pa.address_line_2_local IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 2 local is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 2 local is invalid for country.')

#===============================================
###address_line_2_local required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.addressline2_local = 'R'
                                                                    and pa.address_line_2_local IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field address line 2 local is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field address line 2 local is required for country.')

#===============================================
###city_local validation
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.city_local = '-'
                                                                    and pa.city_local IS NOT NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field city local is invalid for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field city local is invalid for country.')

#===============================================
###city_local required
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pa.former_worker_id FROM formerworkeraddress pa
                                                                    JOIN address_requirements ar
                                                                    on pa.countrycode = ar.countrycode
                                                                    where ar.city_local = 'R'
                                                                    and pa.city_local IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Address field city local is required for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Address field city local is required for country.')

#===============================================
#### END ADDRESS VALIDATIONS ####
#===============================================
#### START TITLE AND SUFFIX VALIDATIONS ####
#===============================================
#legal_social_suffix translate_title
    try:
        formerworker = pd.read_sql_query("""SELECT pd.former_worker_id, pd.legal_social_suffix,tt.value
                                         FROM (SELECT * FROM formerworkername
                                         WHERE legal_social_suffix is not null) as pd
                                         LEFT JOIN (SELECT * FROM translate_title
                                         WHERE titletype = 'SOCIAL') as tt
                                         ON pd.countrycode = tt.countrycode
                                         AND pd.legal_social_suffix = tt.value
                                         JOIN name_requirements as nr
                                         ON nr.countrycode = pd.countrycode and nr.legalsocialsuffix != '-'
                                         WHERE tt.id is null;"""
                                    ,engine)
    except Exception as e:
        print(e)

    for index, row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal social suffix is not a valid workday title for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal social suffix is not a valid workday title for country.')
            
#===============================================
#preferred_social_suffix translate_title
    try:
        formerworker = pd.read_sql_query("""SELECT pd.former_worker_id, pd.legal_social_suffix,tt.value
					FROM (SELECT * FROM formerworkername
					WHERE preferred_social_suffix is not null) as pd
					LEFT JOIN (SELECT * FROM translate_title
					WHERE titletype = 'SOCIAL') as tt
					ON pd.countrycode = tt.countrycode
					AND pd.preferred_social_suffix = tt.value
					JOIN name_requirements as nr
					ON nr.countrycode = pd.countrycode and nr.legalsocialsuffix != '-'
					WHERE tt.id is null;""",engine)
    except Exception as e:
        print(e)

    for index, row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred social suffix is not a valid workday title for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred social suffix is not a valid workday title for country.') 

#===============================================
#legal_hereditary_suffix translate_title
    try:
        formerworker = pd.read_sql_query("""SELECT pd.former_worker_id, pd.legal_social_suffix,tt.value
					FROM (SELECT * FROM formerworkername
					WHERE legal_hereditary_suffix is not null) as pd
					LEFT JOIN (SELECT * FROM translate_title
					WHERE titletype = 'HEREDITARY') as tt
					ON pd.countrycode = tt.countrycode
					AND pd.legal_hereditary_suffix = tt.value
					JOIN name_requirements as nr
					ON nr.countrycode = pd.countrycode and nr.legalhereditarysuffix != '-'
					WHERE tt.id is null""",engine)

    except Exception as e:
        print(e)

    for index, row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal hereditary suffix is not a valid workday title for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal hereditary suffix is not a valid workday title for country.')
            
#===============================================
#preferred_hereditary_suffix translate_title
    try:
        formerworker = pd.read_sql_query("""SELECT pd.former_worker_id
					FROM (SELECT * FROM formerworkername
					WHERE preferred_hereditary_suffix is not null) as pd
					LEFT JOIN (SELECT * FROM translate_title
					WHERE titletype = 'HEREDITARY') as tt
					ON pd.countrycode = tt.countrycode
					AND pd.preferred_hereditary_suffix = tt.value
					JOIN name_requirements as nr
					ON nr.countrycode = pd.countrycode and nr.legalhereditarysuffix != '-'
					WHERE tt.id is null""",engine)
    except Exception as e:
        print(e)

    for index, row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred hereditary suffix is not a valid workday title for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred hereditary suffix is not a valid workday title for country.')
            
#===============================================
#legal_title translate_title
    try:
        formerworker = pd.read_sql_query("""SELECT pd.former_worker_id
					FROM (SELECT * FROM formerworkername
					WHERE legal_title is not null) as pd
					LEFT JOIN (SELECT * FROM translate_title
					WHERE titletype = 'TITLE') as tt
					ON pd.countrycode = tt.countrycode
					AND pd.legal_title = tt.value
					JOIN name_requirements as nr
					ON nr.countrycode = pd.countrycode and nr.legaltitle != '-'
					WHERE tt.id is null;""",engine)
    except Exception as e:
        print(e)

    for index, row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker legal title is not a valid workday title for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker legal title is not a valid workday title for country.')

#===============================================
#preferred_title translate_title
    try:
        formerworker = pd.read_sql_query("""SELECT pd.former_worker_id
					FROM (SELECT * FROM formerworkername
					WHERE preferred_title is not null) as pd
					LEFT JOIN (SELECT * FROM translate_title
					WHERE titletype = 'TITLE') as tt
					ON pd.countrycode = tt.countrycode
					AND pd.preferred_title = tt.value
					JOIN name_requirements as nr
					ON nr.countrycode = pd.countrycode and nr.legaltitle != '-'
					WHERE tt.id is null;""",engine)
    except Exception as e:
        print(e)

    for index, row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker preferred title is not a valid workday title for country.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker preferred title is not a valid workday title for country.')

#===============================================
#### END TITLE AND SUFFIX VALIDATIONS ####
#===============================================
#### START COUNTRYCODE,formerworker ID, AND OTHER VALIDATIONS ####
#===============================================
#Duplicate formerworker ID on Former Worker
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""select former_worker_id from formerworkername
                                                            group by former_worker_id
                                                            having count(1) > 1;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Duplicate formerworker ID.')
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Duplicate formerworker ID.')

#===============================================        	 
#formerworkers ISO countrycode is invalid in (formerworkername)
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkername as pd 
							LEFT JOIN name_requirements as nr 
							on (pd.countrycode = nr.countrycode)
							WHERE nr.countrycode IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker does not have a valid country code in Former Worker name data.')  
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker does not have a valid country code in Former Worker name data.')
	
#===============================================
#formerworkers ISO countrycode is invalid in (formerworkeraddress)
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT ad.former_worker_id FROM formerworkeraddress as ad 
															LEFT JOIN name_requirements as nr 
															on (ad.countrycode = nr.countrycode)
															WHERE nr.countrycode IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker does not have a valid country code in Former Worker address data.')  
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Former Worker does not have a valid country code in Former Worker address data.')		
		
#===============================================
#formerworkers ISO countrycode is invalid in (formerworkerphone)
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkerphone as pd 
															LEFT JOIN name_requirements as nr 
															on (pd.countrycode = nr.countrycode)
															WHERE nr.countrycode IS NULL;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker does not have a valid country code in Former Worker phone data.')  
        except:
            errors[row['former_worker_id']] = []

            errors[row['former_worker_id']].append('Former Worker does not have a valid country code in Former Worker phone data.')			 
		
#===============================================
#formerworkers Postal Code does not match the region id. USA only (formerworkeraddress)
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""SELECT pd.former_worker_id FROM formerworkeraddress as pd
														JOIN zipcode_requirements as zr 
														ON (pd.postalcode = zr.zipcode)
														WHERE EXISTS
														(SELECT id FROM Enable_Postal_Code_Validation WHERE id = '1')
														AND pd.region != zr.countryregioncode"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Postal Code does not match the region id.')  
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('Postal Code does not match the region id.')		
       
#===============================================
#formerworker does not have a valid address region for country
    try:
        formerworker = pd.read_sql_query(sqlalchemy.text("""select  distinct pa.former_worker_id from formerworkeraddress pa
                                                        join region_requirements rr on pa.countrycode = rr.countrycode
                                                        left join region_requirements rr2 on pa.region = rr2.countryregioncode
                                                        where pa.region is not null
                                                        and rr2.countrycode is null;"""),engine)
    except Exception as e:
        print(e)

    for index,row in formerworker.iterrows():
        try:
            errors[row['former_worker_id']].append('Former Worker does not have a valid address region for country.')  
        except:
            errors[row['former_worker_id']] = []
            errors[row['former_worker_id']].append('formerworker does not have a valid address region for country')

#===============================================
#### END COUNTRYCODE, formerworker ID, AND OTHER VALIDATIONS ####
#===============================================
#===============================================
#### CLOSE OUT TIMER AND RETURN ERRORS ###
#===============================================
#    print(errors)
    print('--- %s seconds ---' % (time.time() - start_time))

    return errors

#load()
#print(validate())
