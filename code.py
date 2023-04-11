# Importing the required libraries
import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
import xml.etree.ElementTree as Xet

# set up database connection/ mysql/php/mariadb
try:
    connSqlServer = mysql.connector.connect(
        host='localhost',
        port=3308,
        user='root',
        password='',
        database='a'
    )
    
    if connSqlServer.is_connected():
        cursor = connSqlServer.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("** Database connected ** !: ", record)

       
        cursor.execute("CREATE TABLE IF NOT EXISTS award (AwardTitle VARCHAR(255),AGENCY VARCHAR(255),AwardEffectiveDate VARCHAR(255),AwardExpirationDate VARCHAR(255),AwardTotalIntnAmount VARCHAR(255),AwardAmount BIGINT, AbstractNarration TEXT , MinAmdLetterDate varchar(255), MaxAmdLetterDate varchar(255),CFDA_NUM VARCHAR(255), NSF_PAR_USE_FLAG BIGINT, FUND_AGCY_CODE BIGINT, AWDG_AGCY_CODE BIGINT, AwardID BIGINT PRIMARY KEY)")
        #FUND_OBLG VARCHAR(255),ARRAAmount varchar(255)
        print("Award Tables created or already exist")
        
         
        cursor.execute("CREATE TABLE IF NOT EXISTS award_instrument (Value VARCHAR(255), AwardID BIGINT)")
        print("Award Instrument Tables created or already exist")   
        
        cursor.execute("CREATE TABLE IF NOT EXISTS award_organization (Code BIGINT, AwardID BIGINT)")
        print("Organization Tables created or already exist")
        
        cursor.execute("CREATE TABLE IF NOT EXISTS program_officer (SignBlockName VARCHAR(255),PO_EMAI VARCHAR(255),PO_PHON VARCHAR(255), AwardID BIGINT)")
        print("Program_officer Tables created or already exist")
        
        cursor.execute("CREATE TABLE IF NOT EXISTS investigator (FirstName VARCHAR(255), LastName VARCHAR(255), PI_MID_INIT VARCHAR(255), PI_SUFX_NAME VARCHAR(255),PI_FULL_NAME VARCHAR(255),EmailAddress VARCHAR(255),NSF_ID VARCHAR(255), StartDate VARCHAR(255),EndDate VARCHAR(255), RoleCode VARCHAR(255),AwardID BIGINT)")
        print("Investigator Tables created or already exist")
        
        cursor.execute("CREATE TABLE IF NOT EXISTS institution (Name VARCHAR(255),CityName VARCHAR(255),ZipCode VARCHAR(255),PhoneNumber BIGINT,StreetAddress VARCHAR(255),StreetAddress2 VARCHAR(255), CountryName VARCHAR(255),StateName VARCHAR(255),StateCode VARCHAR(255), CONGRESSDISTRICT BIGINT,CONGRESS_DISTRICT_ORG VARCHAR(255),ORG_UEI_NUM VARCHAR(255), ORG_LGL_BUS_NAME VARCHAR(255),ORG_PRNT_UEI_NUM VARCHAR(255),AwardID BIGINT)")
        print("Institution Tables created or already exist")
        
        cursor.execute("CREATE TABLE IF NOT EXISTS performance_institution (Name VARCHAR(255),CityName VARCHAR(255),StateCode VARCHAR(255),ZipCode VARCHAR(255),StreetAddress VARCHAR(255),CountryCode VARCHAR(255),CountryName VARCHAR(255),StateName VARCHAR(255),CountryFlag VARCHAR(255),CONGRESSDISTRICT VARCHAR(255),CONGRESS_DISTRICT_PERF VARCHAR(255), AwardID BIGINT)")
        print("Performance_institution Tables created or already exist")
        
        cursor.execute("CREATE TABLE IF NOT EXISTS program_element (Code VARCHAR(255),Text VARCHAR(255), AwardID BIGINT)")
        print("Program_element Tables created or already exist")
        
        cursor.execute("CREATE TABLE IF NOT EXISTS program_reference (Code VARCHAR(255),Text VARCHAR(255), AwardID BIGINT)")
        print("Program_reference Tables created or already exist")
               
        cursor.execute("CREATE TABLE IF NOT EXISTS appropriation (Code VARCHAR(255), Name VARCHAR(255), APP_SYMB_ID BIGINT, AwardID BIGINT)")
        print("Appropriation Tables created or already exist")
        
        cursor.execute("CREATE TABLE IF NOT EXISTS fund (Code VARCHAR(255), Name VARCHAR(225), FUND_SYMB_ID VARCHAR(255), AwardID BIGINT)")
        print("Fund Tables created or already exist")
        
        cursor.execute("CREATE TABLE IF NOT EXISTS division (Abbreviation VARCHAR(255), LongName VARCHAR(255), OrganizationCode BIGINT, AwardID BIGINT)")
        print("Division Tables created or already exist")
        
        cursor.execute("CREATE TABLE IF NOT EXISTS directorate (Abbreviation VARCHAR(255), LongName VARCHAR(255), OrganizationCode BIGINT, AwardID BIGINT)")
        print("Directorate Tables created or already exist")
           
        cursor.execute('DROP TABLE IF EXISTS award_institution;')
        cursor.execute("CREATE TABLE award_institution (AwardID BIGINT,Name VARCHAR(255),CityName VARCHAR(255), StateName VARCHAR(255), ORG_UEI_NUM VARCHAR(255), ORG_LGL_BUS_NAME VARCHAR(255), FOREIGN KEY(AwardID) REFERENCES award(AwardID))")

        cursor.execute("CREATE TABLE IF NOT EXISTS award_investigator (AwardID BIGINT, FirstName VARCHAR(225),LastName VARCHAR(225), NSF_ID VARCHAR(225), RoleCode VARCHAR(225), FOREIGN KEY(AwardID) REFERENCES award(AwardID))")
        print("Award_investigator Tables created or already exist")
        
        cursor.execute("CREATE TABLE IF NOT EXISTS award_performance_institution (AwardID BIGINT, Name VARCHAR(225),CityName VARCHAR(225), StateName VARCHAR(225), FOREIGN KEY(AwardID) REFERENCES award(AwardID))")
        print("Award_performance_institution Tables created or already exist")
        
        
        cursor.execute("CREATE TABLE IF NOT EXISTS award_program_reference (AwardID BIGINT, Code VARCHAR(255),Text VARCHAR(255), FOREIGN KEY(AwardID) REFERENCES award(AwardID))")
        print("Award_program_reference Tables created or already exist")
        
        cursor.execute("CREATE TABLE IF NOT EXISTS award_program_element (AwardID BIGINT, Code VARCHAR(224),Text VARCHAR(225), FOREIGN KEY(AwardID) REFERENCES award(AwardID))")
        print("Award_program_element Tables created or already exist")
        
       
       
        # getting list of xml files in directory
        # path = where xml files are stored
        path = '/Users/salukayastha/Desktop/Project/nsf_xml_No_duplicatedata/xml/'
        xml_files = [f for f in os.listdir(path) if f.endswith('.xml')]

        if xml_files:
            # iterate through xml files and insert data into database
            for xml_file in xml_files:
                tree = Xet.parse(path + xml_file)
                root = tree.getroot()

#-------------------------------------
                for award in root.findall('Award'):
                    award_id = award.find('AwardID').text
                    award_title = award.find('AwardTitle').text
                    award_agency = award.find('AGENCY').text
                    award_effectivedate = award.find('AwardEffectiveDate').text
                    award_expirationdate = award.find('AwardExpirationDate').text
                    award_total_amount = award.find('AwardTotalIntnAmount'). text
                    award_amount = award.find('AwardAmount').text
                    award_abstract_narration = award.find('AbstractNarration').text
                    award_min_amd_ldate = award.find('MinAmdLetterDate').text
                    award_max_amd_ldate = award.find('MaxAmdLetterDate').text
                    # award_arraamount = award.find('ARRAAmount').text
                    #tran_type = award.find('TRAN_TYPE').text 
                    cfda_num = award.find('CFDA_NUM').text
                    nsf_par_use_flag = award.find('NSF_PAR_USE_FLAG').text
                    fund_agcy_code = award.find('FUND_AGCY_CODE').text
                    awdg_agcy_code = award.find('AWDG_AGCY_CODE').text
                    #fund_oblg = award.find('FUND_OBLG').text
                    #trying to avoid duplicate value

# Check if the award instrument already exists and works for duplicate data                    
                    cursor.execute('SELECT * FROM award WHERE AwardID = %s AND AwardID = %s', (award_id,award_id)) #AwardTitle = %s   award_title
                    result = cursor.fetchone()
                    if result:
                        print(f"AwardID {award_id} already exists in award table. Skipping update on table..")
                    else:
                        cursor.execute('INSERT INTO award (AwardTitle,AGENCY,AwardEffectiveDate,AwardExpirationDate,AwardTotalIntnAmount,AwardAmount,AbstractNarration,MinAmdLetterDate,MaxAmdLetterDate,CFDA_NUM,NSF_PAR_USE_FLAG,FUND_AGCY_CODE,AWDG_AGCY_CODE,AwardID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )', (award_title, award_agency, award_effectivedate, award_expirationdate, award_total_amount,award_amount, award_abstract_narration, award_min_amd_ldate, award_max_amd_ldate, cfda_num, nsf_par_use_flag, fund_agcy_code, awdg_agcy_code, award_id)) #fund_oblg
                          
#-------------------------------------
                for award in root.findall('Award'):
                    award_instrument = award.findall('AwardInstrument')
                    for award_instrument in award_instrument:
                        value = award_instrument.find('Value').text
                        award_id = award.find('AwardID').text
        
# Check if the award instrument already exists and works for duplicate data
                        cursor.execute('SELECT * FROM award_instrument WHERE AwardID = %s AND AwardID = %s', (award_id,award_id))
                        result = cursor.fetchone()
                        if result:
                            print(f"AwardID {award_id} already exists in award_instrument table. Skipping update on table..")
                        else:
                            cursor.execute('INSERT INTO award_instrument (Value, AwardID) VALUES (%s, %s)', (value, award_id))
                    
#-------------------------------------

                for award in root.findall('Award'):
                    award_organization = award.findall('Organization')
                    for award_organization in award_organization:
                        code = award_organization.find('Code').text
                        award_id = award.find('AwardID').text
        
# Check if the award organization already exists and works for duplicate data
                        cursor.execute('SELECT * FROM award_organization WHERE AwardID = %s AND AwardID = %s', (award_id, award_id))
                        result = cursor.fetchone()
                        if result:
                            print(f"AwardID {award_id} already exists in award_organization table. Skipping update on table...")                    
                        else:
                            cursor.execute('INSERT INTO award_organization (Code, AwardID) VALUES (%s, %s)', (code, award_id))

#-------------------------------------
                for award in root.findall('Award'):
                    program_officer = award.findall('ProgramOfficer')
                    for program_officer in program_officer:
                        sign_block_name = program_officer.find('SignBlockName').text
                        po_email = program_officer.find('PO_EMAI').text
                        po_phone = program_officer.find('PO_PHON').text
                        award_id = award.find('AwardID').text
        
# Check if the award program_officer already exists and works for duplicate data
                        cursor.execute('SELECT * FROM program_officer WHERE AwardID = %s AND AwardID = %s', (award_id, award_id))
                        result = cursor.fetchone()
                        if result:
                            print(f"AwardID {award_id} already exists in program_officer table. Skipping update on table...")
                        else:
                            cursor.execute('INSERT INTO program_officer (SignBlockName, PO_EMAI, PO_PHON, AwardID) VALUES (%s, %s,%s, %s)', (sign_block_name,po_email,po_phone, award_id))

#-------------------------------------

                for award in root.findall('Award'):
                    investigator = award.findall('Investigator')
                    for investigator in investigator:
                        fname = investigator.find('FirstName').text
                        lname = investigator.find('LastName').text
                        pi_mid_init = investigator.find('PI_MID_INIT').text
                        pi_sufx = investigator.find('PI_SUFX_NAME').text
                        pi_full_name = investigator.find('PI_FULL_NAME').text
                        email = investigator.find('EmailAddress').text
                        nsf_id = investigator.find('NSF_ID').text
                        start_date = investigator.find('StartDate').text
                        end_date = investigator.find('EndDate').text
                        role_code = investigator.find('RoleCode').text
                        award_id = award.find('AwardID').text
       
# Check if the award investigator already exists and works for duplicate data
                        cursor.execute('SELECT * FROM investigator WHERE AwardID = %s AND AwardID = %s ', (award_id, award_id))
                        result = cursor.fetchone()
                        if result:
                            print(f"AwardID {award_id} already exists in investigator table. Skipping update on table...")                      
                        else:
                            cursor.execute('INSERT INTO investigator (FirstName,LastName,PI_MID_INIT,PI_SUFX_NAME,PI_FULL_NAME,EmailAddress,NSF_ID,StartDate,EndDate,RoleCode,AwardID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (fname,lname,pi_mid_init,pi_sufx,pi_full_name,email,nsf_id,start_date,end_date,role_code, award_id))

# Check if the  award_investigator already exists and works for duplicate data
                        cursor.execute('SELECT award_investigator.AwardID FROM award_investigator LEFT JOIN investigator ON award_investigator.AwardID = investigator.AwardID WHERE award_investigator.AwardID = %s AND award_investigator.AwardID = %s', (award_id,award_id))
                        result = cursor.fetchone()

                        if result:
                            print(f"AwardID {award_id} already exists in award_investigator table. Skipping update on table...")
                        else:
                            cursor.execute('INSERT INTO award_investigator (AwardID, FirstName, LastName, NSF_ID, RoleCode) VALUES (%s, %s, %s, %s, %s)', (award_id,fname,lname,nsf_id,role_code ))
        
                    
#-------------------------------------
                for award in root.findall('Award'):
                    institution = award.findall('Institution')
                    for institution in institution:
                        name = institution.find('Name').text
                        city = institution.find('CityName').text
                        zip_code = institution.find('ZipCode').text
                        phone_num = institution.find('PhoneNumber').text
                        add = institution.find('StreetAddress').text
                        add2 = institution.find('StreetAddress2').text
                        country_name = institution.find('CountryName').text
                        state_name = institution.find('StateName').text
                        state_code = institution.find('StateCode').text
                        congressdist = institution.find('CONGRESSDISTRICT').text
                        congress_dist_org = institution.find('CONGRESS_DISTRICT_ORG').text
                        org_uei = institution.find('ORG_UEI_NUM').text
                        org_lgl = institution.find('ORG_LGL_BUS_NAME').text
                        org_prnt = institution.find('ORG_PRNT_UEI_NUM').text
                        award_id = award.find('AwardID').text
       
# Check if the  institution already exists and works for duplicate data
                   
                        cursor.execute('SELECT * FROM institution WHERE AwardID = %s AND AwardID = %s', (award_id, award_id))
                        result = cursor.fetchone()
                        if result:
                            print(f"AwardID {award_id} already exists in institution table. Skipping update on table...")
                        else:
                            cursor.execute('INSERT INTO institution (Name,CityName,ZipCode,PhoneNumber,StreetAddress,StreetAddress2,CountryName,StateName,StateCode,CONGRESSDISTRICT,CONGRESS_DISTRICT_ORG,ORG_UEI_NUM,ORG_LGL_BUS_NAME,ORG_PRNT_UEI_NUM,AwardID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (name,city,zip_code,phone_num,add,add2,country_name,state_name,state_code,congressdist,congress_dist_org,org_uei,org_lgl,org_prnt, award_id))

# Check if the award award_institution already exists and works for duplicate data
                        cursor.execute('SELECT award_institution.AwardID FROM award_institution LEFT JOIN institution ON award_institution.AwardID = institution.AwardID WHERE award_institution.AwardID = %s AND award_institution.AwardID = %s', (award_id,award_id))
                        result = cursor.fetchone()

                        if result:
                            print(f"AwardID {award_id} already exists in award_institution table. Skipping update on table...")
                        else:
                            cursor.execute('INSERT INTO award_institution (AwardID, Name, CityName, StateName, ORG_UEI_NUM, ORG_LGL_BUS_NAME) VALUES (%s, %s, %s, %s, %s, %s)', (award_id, name, city, state_name, org_uei, org_lgl ))
                      
                                                               
#-------------------------------------
                for award in root.findall('Award'):
                    performance_institution = award.findall('Performance_Institution')
                    for performance_institution in performance_institution:
                        name = performance_institution.find('Name').text
                        city_name = performance_institution.find('CityName').text
                        state_code = performance_institution.find('StateCode').text
                        zip_code = performance_institution.find('ZipCode').text
                        stress_address = performance_institution.find('StreetAddress').text
                        country_code = performance_institution.find('CountryCode').text
                        country_name = performance_institution.find('CountryName').text
                        state_name = performance_institution.find('StateName').text
                        country_flag = performance_institution.find('CountryFlag').text
                        congress_district = performance_institution.find('CONGRESSDISTRICT').text
                        congress_district_perf = performance_institution.find('CONGRESS_DISTRICT_PERF').text
                        award_id = award.find('AwardID').text
        
# Check if the award performance_institution already exists and works for duplicate data
                        cursor.execute('SELECT * FROM performance_institution WHERE AwardID = %s AND AwardID = %s', (award_id, award_id))
                        result = cursor.fetchone()
                        if result:
                            print(f"AwardID {award_id} already exists in performance_institution table. Skipping update on table...")
                        else:
                            cursor.execute('INSERT INTO performance_institution (Name,CityName,StateCode,ZipCode,StreetAddress,CountryCode,CountryName,StateName,CountryFlag,CONGRESSDISTRICT,CONGRESS_DISTRICT_PERF,AwardID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (name,city_name,state_code,zip_code,stress_address,country_code,country_name,state_name,country_flag,congress_district,congress_district_perf, award_id))

# Check if the award award_performance_institution already exists and works for duplicate data
                    
                        cursor.execute('SELECT award_performance_institution.AwardID FROM award_performance_institution LEFT JOIN performance_institution ON award_performance_institution.AwardID = performance_institution.AwardID WHERE award_performance_institution.AwardID = %s AND award_performance_institution.AwardID = %s', (award_id,award_id))
                        result = cursor.fetchone()
                        if result:
                            print(f"AwardID {award_id} already exists in award_performance_institution table. Skipping update on table...")
                        else:
                            cursor.execute('INSERT INTO award_performance_institution (AwardID, Name, CityName, StateName) VALUES (%s, %s, %s, %s)', (award_id,name,city,state_name))
        
                       
 #----------------------------------------------- 
                for award in root.findall('Award'):
                    program_element = award.findall('ProgramElement')
                    for program_element in program_element:
                        code = program_element.find('Code').text
                        text = program_element.find('Text').text
                        award_id = award.find('AwardID').text
        
# Check if the award program_element already exists and works for duplicate data
                        cursor.execute('SELECT * FROM program_element WHERE AwardID = %s AND AwardID = %s', (award_id, award_id))
                        result = cursor.fetchone()
                        if result:
                            print(f"AwardID {award_id} already exists in program_element table. Skipping update on table...")
                        else:
                            cursor.execute('INSERT INTO program_element (Code, Text, AwardID) VALUES (%s, %s, %s)', (code, text, award_id))

# # Check if the award award_program_element already exists and works for duplicate data
#                         cursor.execute('SELECT award_program_element.AwardID FROM award_program_element LEFT JOIN program_element ON award_program_element.AwardID = program_element.AwardID WHERE award_program_element.AwardID = %s AND award_program_element.AwardID = %s', (award_id,award_id))
#                         result = cursor.fetchone()
#                         if result:
#                             print(f"AwardID {award_id} already exists in award_program_element table. Skipping update on table...")
#                         else:
#                             cursor.execute('INSERT INTO award_program_element (AwardID, Code, Text) VALUES (%s, %s, %s)', (award_id,code,text))
                             
 #-----------------------------------------------                  
                for award in root.findall('Award'):
                    program_reference = award.findall('ProgramReference')
                    for program_reference in program_reference:
                        code = program_reference.find('Code').text
                        text = program_reference.find('Text').text
                        award_id = award.find('AwardID').text
        
# Check if the award program_reference already exists and works for duplicate data
                        cursor.execute('SELECT * FROM program_reference WHERE AwardID = %s AND AwardID = %s', (award_id, award_id))
                        result = cursor.fetchone()
                        if result:
                            print(f"AwardID {award_id} already exists in program_reference table. Skipping update on table...")
                        else:
                            cursor.execute('INSERT INTO program_reference (Code,Text, AwardID) VALUES (%s, %s, %s)', (code,text,award_id))

# Check if the award award_program_reference already exists and works for duplicate data
                    
                    cursor.execute('SELECT award_program_reference.AwardID FROM award_program_reference LEFT JOIN program_reference ON award_program_reference.AwardID = program_reference.AwardID WHERE award_program_reference.AwardID = %s AND award_program_reference.AwardID = %s', (award_id,award_id))
                    result = cursor.fetchone()
                    if result:
                        print(f"AwardID {award_id} already exists in award_program_reference table. Skipping update on table...")
                    else:
                     cursor.execute('INSERT INTO award_program_reference (AwardID, Code,Text) VALUES (%s, %s, %s)', (award_id,code,text))

#-----------------------------------------------
                for award in root.findall('Award'):
                    appropriation = award.findall('Appropriation')
                    for appropriation in appropriation:
                        code = appropriation.find('Code').text
                        name = appropriation.find('Name').text
                        app_symb = appropriation.find('APP_SYMB_ID').text
                        award_id = award.find('AwardID').text
        
# Check if the award - appropriation already exists and works for duplicate data
                        cursor.execute('SELECT * FROM appropriation WHERE AwardID = %s AND AwardID = %s', (award_id, award_id))
                        result = cursor.fetchone()
                        if result:
                            print(f"AwardID {award_id} already exists in appropriation table. Skipping update on table...")
                        else:
                            cursor.execute('INSERT INTO appropriation (Code,Name,APP_SYMB_ID, AwardID) VALUES (%s, %s, %s, %s)', (code,name,app_symb,award_id))

#-----------------------------------------------
                for award in root.findall('Award'):
                    fund = award.findall('Fund')
                    for fund in fund:
                        code = fund.find('Code').text
                        name = fund.find('Name').text
                        fund_symb = fund.find('FUND_SYMB_ID').text
                        award_id = award.find('AwardID').text
        
# Check if the award - fund already exists and works for duplicate data
                        cursor.execute('SELECT * FROM fund WHERE AwardID = %s AND AwardID = %s', (award_id, award_id))
                        result = cursor.fetchone()
                        if result:
                            print(f"AwardID {award_id} already exists in fund table. Skipping update on table...")
                        else:
                            cursor.execute('INSERT INTO fund (Code,Name,FUND_SYMB_ID, AwardID) VALUES (%s, %s, %s, %s)', (code,name,fund_symb,award_id))
#----------------------------------------------

                for award in root.findall('Award'):
                    award_organizations = award.findall('Organization')
                    for award_organization in award_organizations:
                        division = award_organization.findall('Division')   
                        for division in division:
                            abbreviation = division.find('Abbreviation').text 
                            long_name = division.find('LongName').text
                            code = award_organization.find('Code').text
                            award_id = award.find('AwardID').text
        
# Check if the award - organization - division already exists and works for duplicate data
                            cursor.execute('SELECT * FROM division WHERE AwardID = %s AND AwardID = %s', (award_id, award_id))
                            result = cursor.fetchone()
                            if result:
                                print(f"AwardID {award_id} already exists in division table. Skipping update on table...")
                            else:
                                cursor.execute('INSERT INTO division (Abbreviation, LongName, OrganizationCode, AwardID) VALUES (%s, %s, %s, %s)', (abbreviation, long_name, code, award_id))

#-------------------------------------------------
                for award in root.findall('Award'):
                    award_organizations = award.findall('Organization')
                    for award_organization in award_organizations:
                        directorates = award_organization.findall('Directorate')   
                        for directorate in directorates:
                            abbreviation = directorate.find('Abbreviation').text
                            long_name = directorate.find('LongName').text
                            code = award_organization.find('Code').text
                            award_id = award.find('AwardID').text
        
# Check if the award - organization - directorate already exists and works for duplicate data
                            cursor.execute('SELECT * FROM directorate WHERE AwardID = %s AND AwardID = %s', (award_id, award_id))
                            result = cursor.fetchone()
                            if result:
                                print(f"AwardID {award_id} already exists in directorate table. Skipping update on table...")
                            else:
                                cursor.execute('INSERT INTO directorate (Abbreviation, LongName, OrganizationCode, AwardID) VALUES (%s, %s, %s, %s)', (abbreviation, long_name, code, award_id))

    
                                
#------------- this is server transaction commitment------------------
            connSqlServer.commit()
            print("Data inserted successfully")
        else:
            print("No xml files found in the directory")

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connSqlServer.is_connected():
        cursor.close()
        connSqlServer.close()
        print("MySQL connection is closed")
