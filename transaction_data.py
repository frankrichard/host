
import pandas as pd

import csv

import time

import traceback

import hashlib

import json

import difflib

from datetime import datetime

from kafka import KafkaProducer

import sys

start_time = time.time()

sys.path.append(r'C:\Users\CBT\AppData\Local\Programs\Python\Python311\Lib\site-packages')

config = pd.read_excel('config.xlsx',engine = 'openpyxl')

config = dict(list(zip(config['key'],config['value'])))



import requests

#Reading files

import os

from unidecode import unidecode

import re


def hash(row,column,hash_value):

    columnName = 'hash_'

    # hashColumn = pd.Series()

    # for i in range((len(sourcedf[column[0]]))):

    concatstr = ''

    for j in column:

      concatstr = concatstr + row[j]

    row[hash_value] = hashlib.sha512( concatstr.encode("utf-8") ).hexdigest()

    return row


email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

def identify_misspelled_names(names):
    # Identify misspelled names (excluding identical names)

    misspelled_names = []

    misspelled_names1 = []

    for i, name1 in enumerate(names):

        for j, name2 in enumerate(names):


            if i != j:

                # Calculate similarity between names

                similarity = difflib.SequenceMatcher(None, name1, name2).ratio()

                # Set a threshold for similarity (you may need to adjust this)
                similarity_threshold = 0.75



                # If similarity is below the threshold, consider it misspelled
                if similarity > similarity_threshold:

#                    misspelled_names.append(name1)

                    if ((name2 not in misspelled_names) and (name2 not in misspelled_names1)):

                           misspelled_names1.append(name2)
            

                    if ((name1 not in misspelled_names) and (name1 not in misspelled_names1)):

                           misspelled_names.append(name1)

#                    misspelled_name1.append()

    return misspelled_names,misspelled_names1




def mis_spelled_apply_function(row):

    print(row['FIRSTNAME'])
    
    unique,duplicates = identify_misspelled_names(row['spell_check'])
    
    if ((row['FIRSTNAME'] in unique) or (len(row['spell_check'])<2)):
        
        row['mis_spell'] = '1'
        
    else:
        
        row['mis_spell'] = '0'

    return row

def minor_calculation(row):
    
    # dob_date = datetime.strptime(row['DATEOFBIRTH'], "%Y-%m-%d")
    
    if row['DATEOFBIRTH']!='':
    
        current_date = datetime.now()
    
        age = current_date.year - row['DATEOFBIRTH'].year - ((current_date.month, current_date.day) < (row['DATEOFBIRTH'].month, row['DATEOFBIRTH'].day))
        
        if age<18:
            
            row['IS_MINOR'] = 'Yes'
            
        else:
            
            row['IS_MINOR'] = 'No'

    else:
        
        row['IS_MINOR'] = ''

    return row
    
    

def special_character_check_firstname(row):

    first_name = row['FIRSTNAME']

    last_name = row['LASTNAME']

    while(True):

        special_char = re.compile(r'[^a-zA-Z0-9]')

        if special_char.search(row[config['FirstName']].replace(' ','').replace('-','')) != None:


            if len(row[config['LastName']].split(' '))>1:

                temp_last_name = row[config['LastName']].split(' ')

                row[config['FirstName']] = ' '.join(temp_last_name[0:len(temp_last_name)-1])

                row[config['LastName']] = temp_last_name[len(temp_last_name)-1]

            else:

                row['valid'] = 'invalid'


                if row['reason']=='':

                    row['reason'] = 'special characters in First name'

                else:

                    row['reason'] += ',Special characters in first name'


        if ((row['FIRSTNAME']==first_name) & (row['LASTNAME']==last_name)):

            

            break

        else:

            first_name = row['FIRSTNAME']

            last_name = row['LASTNAME']

            pass


        row = single_character_check_firstname(row)

        row = single_character_check_lastname(row)

        row = number_check_firstname(row)

        row = number_check_lastname(row)

    return row

def special_character_check_lastname(row):

    first_name = row['FIRSTNAME']

    last_name = row['LASTNAME']

    while(True):


        special_char = re.compile(r'[^a-zA-Z0-9]')

        if special_char.search(row[config['FirstName']].replace(' ','').replace('-','')) != None:

            if len(row[config['FirstName']].split(' '))>1:

                temp_last_name = row[config['FirstName']].split(' ')

                row[config['FirstName']] = ' '.join(temp_last_name[0:len(temp_last_name)-1])

                row[config['LastName']] = temp_last_name[len(temp_last_name)-1]

            else:

                row['valid'] = 'invalid'


                if row['reason']=='':

                    row['reason'] = 'Special characters in last name'


                else:

                    row['reason'] += ',Special characters in last name'


        if ((row['FIRSTNAME']==first_name) & (row['LASTNAME']==last_name)):

            break

        else:


            first_name = row['FIRSTNAME']

            last_name = row['LASTNAME']

            pass


        row = single_character_check_firstname(row)

        row = single_character_check_lastname(row)


        row = number_check_firstname(row)

        row = number_check_lastname(row)

        row = special_character_check_firstname(row)

    return row


def GYU(row):


    if 'GYU' in row[config['FirstName']]:

        row[config['FirstName']] = row[config['FirstName']].replace('GYU ','')

        splitted_words = row[config['FirstName']].split('#')

        if len(splitted_words)>1:

            splitted_words.pop()

        splitted_words = splitted_words[0].split(' ')


        temp_last_name = splitted_words


        # temp_last_name = row[config['FirstName']].split(' ')

        row[config['FirstName']] = ' '.join(temp_last_name[0:len(temp_last_name)-1])

        row[config['LastName']] = temp_last_name[len(temp_last_name)-1]

            


    elif 'GYU ' in row[config['LastName']]:

        row[config['LastName']] = row[config['LastName']].replace('GYU ','')

        splitted_words = row[config['LastName']].split('#')

        if len(splitted_words)>1:

            splitted_words.pop()


        splitted_words = splitted_words[0].split(' ')

        temp_last_name = splitted_words

        # temp_last_name = row[config['FirstName']].split(' ')

        row[config['FirstName']] = ' '.join(temp_last_name[0:len(temp_last_name)-1])

        row[config['LastName']] = temp_last_name[len(temp_last_name)-1]




    return row



def number_check_firstname(row):

    first_name = row['FIRSTNAME']

    last_name = row['LASTNAME']

    while(True):


        if len(re.findall(r'[0-9]+', row[config['FirstName']]))>0:


            if len(row[config['LastName']].split(' '))>1:


                temp_last_name = row[config['LastName']].split(' ')

                row[config['FirstName']] = ' '.join(temp_last_name[0:len(temp_last_name)-1])


                row[config['LastName']] = temp_last_name[len(temp_last_name)-1]

            else:


                row['valid'] = 'invalid'

                if row['reason']=='':

                    row['reason'] = 'Numeric characters in First name'



        if ((row['FIRSTNAME']==first_name) & (row['LASTNAME']==last_name)):

            break

        else:

            first_name = row['FIRSTNAME']

            last_name = row['LASTNAME']

            pass


        row = single_character_check_firstname(row)

        row = single_character_check_lastname(row)



    return row




def number_check_lastname(row):

    first_name = row['FIRSTNAME']

    last_name = row['LASTNAME']


    while(True):

        if len(re.findall(r'[0-9]+', row[config['LastName']]))>0:

            if len(row[config['FirstName']].split(' '))>1:

                temp_last_name = row[config['FirstName']].split(' ')

                row[config['FirstName']] = ' '.join(temp_last_name[0:len(temp_last_name)-1])


                row[config['LastName']] = temp_last_name[len(temp_last_name)-1]

            else:

                row['valid'] = 'invalid'


                if row['reason']=='':

                    row['reason'] = 'Numeric characters in last name'


                else:

                    row['reason'] += ',Numeric characters in last name'


        if ((row['FIRSTNAME']==first_name) & (row['LASTNAME']==last_name)):

            break

        else:


            first_name = row['FIRSTNAME']

            last_name = row['LASTNAME']

            pass


        row = single_character_check_firstname(row)

        row = single_character_check_lastname(row)


        row = number_check_firstname(row)

    return row



def single_character_check_firstname(row):

    if len(row[config['FirstName']])<2:

        if len(row[config['LastName']].split(' '))>1:


            temp_last_name = row[config['LastName']].split(' ')

            row[config['FirstName']] = ' '.join(temp_last_name[0:len(temp_last_name)-1])

            row[config['LastName']] = temp_last_name[len(temp_last_name)-1]



        else:


            row['valid'] = 'invalid'

            if row[config['FirstName']]=='':

                row['reason'] = 'Null Value in first name'

            else:

                row['reason'] = 'Single character in first name'

    return row


def single_character_check_lastname(row):


    first_name = row['FIRSTNAME']

    last_name = row['LASTNAME']

    while(True):

        if len(row[config['LastName']])<2:

            if len(row[config['FirstName']].split(' '))>1:

                temp_last_name = row[config['FirstName']].split(' ')

                row[config['FirstName']] = ' '.join(temp_last_name[0:len(temp_last_name)-1])

                row[config['LastName']] = temp_last_name[len(temp_last_name)-1]


            else:

                row['valid'] = 'invalid'

                if row[config['LastName']]=='':


                    if row['reason']=='':

                        row['reason'] = 'Null value in last name'


                    else:

                        row['reason'] += ',Null value in last name'

                else:

                    if row['reason']=='':

                        row['reason'] = 'Single character in last name'

                    else:


                        row['reason'] += ',Single character in last name'



            row = single_character_check_firstname(row)

        if ((row['FIRSTNAME']==first_name) & (row['LASTNAME']==last_name)):

            break


        else:

            first_name = row['FIRSTNAME']

            last_name = row['LASTNAME']

            pass


    return row


def change_accent(row):

    row[config['FirstName']]= unidecode(row[config['FirstName']])

    row[config['LastName']]= unidecode(row[config['LastName']])

    row[config['CustomerAddress']]= unidecode(row[config['CustomerAddress']])

    return row


def list_all_files_in_drive(drive):

    all_files = []

    for root, dirs, files in os.walk(drive):

        for file in files:

            file_path = os.path.join(root, file)


            all_files.append(file_path)

    return all_files



drive_to_list = config['source_path']

files_in_drive = list_all_files_in_drive(drive_to_list)

files_location = []



for file in files_in_drive:

    file = file.replace('\\','/')

    list1 = file.split('/')

    list1.pop()


    list1 = '/'.join(list1)

    files_location.append(list1)



files_location = list(set(files_location))

files_location = files_location[0:1]

count = 0



files_location = list(set(files_location))

files_location = files_location[0:1]

count = 0

total_dataframe = pd.DataFrame()

mapping_config = pd.read_excel('business_configuration.xlsx',engine = 'openpyxl')

mapping_config.dropna(subset = ['File Name'],inplace = True)

mapping_config.dropna(subset = ['First Name'],inplace = True)

mapping_config.dropna(subset = ['Last Name'],inplace = True)



mapping_config.reset_index(inplace = True,drop = True)

mapping_config.fillna('',inplace = True)


mapping = pd.read_excel('column_mapping.xlsx',engine = 'openpyxl')

mapping.fillna('',inplace = True)

mapping = mapping.to_dict(orient = 'records')


files_location = files_location[0:1]
print(files_location)

for i in files_location:

#    print(i)

#    if (('2023-08-11' in i) or ('2023-08-20' in i)):
#         continue

    csv_files = os.listdir(i)

#    print(csv_files)

#    print(mapping_config['File Name'])


    print('hello')

#    print((('.csv' in transaction_file) and (transaction_file in mapping_config['File Name'])))

    for transaction_file in csv_files:

#        print(((transaction_file in list(mapping_config['File Name']))))
        try:

            if (('.csv' in transaction_file) and (transaction_file in list(mapping_config['File Name']))):

                print(transaction_file)

                print(i)

#                print((('.csv' in transaction_file) and (transaction_file in mapping_config['File Name'])))

                for row in range(len(mapping)):

                    if mapping[row]['Actual CSV Files']==transaction_file:

                        mapping1 = {

                            value: key for key, value in mapping[row].items()  if value!=''

                        }

#                print(mapping1)



                file1 = pd.read_csv(i+"//"+transaction_file,encoding='ISO-8859-1', sep="|")


                current_config = mapping_config[mapping_config['File Name']==transaction_file]

                current_config.reset_index(inplace = True,drop = True)

                columns = []

                for j in file1.columns:

                    columns.append(j.upper())


                file1.columns = columns

                file1.rename(columns = {current_config.loc[0,'First Name'].upper():config['FirstName']},inplace = True)

                file1.rename(columns = {current_config.loc[0,'Last Name'].upper():config['LastName']},inplace = True)

                file1.rename(columns = {current_config.loc[0,'Address'].upper():config['ADDRESS1']},inplace = True)

                file1.rename(columns = {current_config.loc[0,'DOB'].upper():config['CustomerDOB']},inplace = True)

                print(len(file1))


                file1.rename(mapping1,inplace = True)



                # renaming_columns = eval(current_config.loc[0,'headers'])

                # file1.rename(columns = renaming_columns,inplace = True)

#                print(missing_columns)

                missing_columns = set([config['FirstName'],config['LastName'],config['CustomerAddress'],config['CustomerDOB']]) - set(file1.columns) 

#                print(missing_columns)

                if len(missing_columns)>1:

                    print('missing')


                    print('columns missing in file '+current_config.loc[0,'File Name']+" : "+','.join(missing_columns))

                    continue



                df = file1.copy()


                df[config['ADDRESS2']] = ''

                df[config['ADDRESS3']] = ''

                df[config['ADDRESS4']] = ''

                df[config['CustomerAddress']] = df[config['ADDRESS1']]

                df['floki_changes'] = ''
#


                print('Input file Length')

                print(len(df))




                #rule1 upper case and accent change


                df[config['FirstName']] = df[config['FirstName']].fillna('')

                df[config['LastName']] = df[config['LastName']].fillna('')

                df[config['CustomerAddress']] = df[config['CustomerAddress']].fillna('')





                df[config['FirstName']] = df[config['FirstName']].str.upper()

                df['CustomerLasttName'] = df[config['LastName']].str.upper()

                df[config['CustomerAddress']] = df[config['CustomerAddress']].str.upper()



                last_name_words = config['lastname_words']

                for words in last_name_words.split(','):

                    df[config['FirstName']] = df[config['FirstName']].str.replace(words.upper(),words.upper().replace(' ','-')) 
        
                    df[config['LastName']] = df[config['LastName']].str.replace(words.upper(),words.upper().replace(' ','-')) 



                df = df.apply(lambda row:GYU(row),axis = 1)


                print(len(df))

                #change_accent

                for column in list(df.columns):

                    df[column] = df[column].astype(str)


                    df[column] = df[column].apply(lambda x: unidecode(str(x)) if pd.notnull(x) else x)

                    df[column] = df[column].str.replace(',',';')




                df = df.apply(lambda row:change_accent(row),axis = 1)

                print(len(df))


                for column in ['EMAIL','LANDLINE_NO']:

                    print(column)

                    print(df.columns)

                    print(column not in list(df.columns))

                    if column not in list(df.columns):



                        df[column] = ''

#                        print(df[column])

                    df[column+'_error'] = df[column].copy()



                df.loc[~(df['EMAIL'].str.contains(email_regex)),'floki_changes']= 'invalid Email so replaced as null;'

                df['EMAIL']= df[df['EMAIL'].str.contains(email_regex, na=False)]['EMAIL']

                print(len(df))

                #landline number validation

                df['LANDLINE_CHECK'] = df['LANDLINE_NO']


                df['LANDLINE_NO'] = pd.to_numeric(df['LANDLINE_NO'],errors = 'coerce')


                df.loc[df['LANDLINE_NO'].isna(),'floki_changes'] += 'invalid landline no;'


                #date format

                df.rename(columns = {'EXPIRYDATE_x':'EXPIRYDATE','LOAD_DT_x':'LOAD_DT'},inplace = True)


                for column in ['EXPIRYDATE','LOAD_DT','ISSUEDATE','DATEOFBIRTH']:

                    if column not in list(df.columns):

                        df[column] = ''

                    df[column+'_error'] = df[column].copy()



                df['ISSUEDATE'] = pd.to_datetime(df['ISSUEDATE'],format = '%m/%d/%Y %I:%M:%S %p',errors = 'coerce')


                df['ISSUEDATE'] = df['ISSUEDATE'].dt.strftime('%m/%d/%Y')

                df['ISSUEDATE'].fillna('',inplace = True)

                df['EXPIRYDATE'] = pd.to_datetime(df['EXPIRYDATE'],format = '%m/%d/%Y %I:%M:%S %p',errors = 'coerce')

                df['EXPIRYDATE'] = df['EXPIRYDATE'].dt.strftime('%m/%d/%Y')

                df['EXPIRYDATE'].fillna('',inplace = True)


                df['DATEOFBIRTH'] = pd.to_datetime(df['DATEOFBIRTH'],format = '%m/%d/%Y %I:%M:%S %p',errors = 'coerce')

                df['DATEOFBIRTH'] = df['DATEOFBIRTH'].dt.strftime('%m/%d/%Y')

                df['DATEOFBIRTH'].fillna('',inplace = True)

                df['LOAD_DT'] = pd.to_datetime(df['LOAD_DT'],format = '%m/%d/%Y %I:%M:%S %p',errors = 'coerce')

                df['LOAD_DT'] = df['LOAD_DT'].dt.strftime('%m/%d/%Y')

                df['LOAD_DT'].fillna('',inplace = True)


                df.loc[df['ISSUEDATE']=='','floki_changes']+='issuedate not in format or empty;'

                df.loc[df['EXPIRYDATE']=='','floki_changes']+='expiry date not in format or empty;'

                df.loc[df['DATEOFBIRTH']=='','floki_changes']+='DOB not in format or empty;'

                df.loc[df['LOAD_DT']=='','floki_changes']+='LOAD_DT not in format or empty;'



                suffixes = [ ' I', ' II', ' III', ' IV', ' V', ' JR', ' SR']

                df['SUFFIX'] = ''

                for suffix in suffixes:

                    df.loc[df[config['FirstName']].str.contains(suffix),'SUFFIX'] = suffix

                    df[config['FirstName']] = df[config['FirstName']].str.replace(suffix,'')

                    df.loc[df[config['LastName']].str.contains(suffix),'SUFFIX'] = suffix

                    df[config['LastName']] = df[config['LastName']].str.replace(suffix,'')

                # corporate customers

                print('corporate customers')

                corp_name_pattern = ['ACADEMY',"TECHNOLOGY" ,"TECHNO", "STALL","SERVICES","BRANCH","OUTLET","EXPRESS","CENTER","BUSINESS","CORPORATION","COMPANY"," INC","INC ","COURIER","COOPERATIVE","BANK","SECURITY","DISTRIBUTOR","DISTILLERS","PHARMACY","MOTORS","SCHOOL","TRADEING","ACCOUNTS","ASSOCIATION","UNIV","COLLEGES","MERCHANT/MERCHANDIZING","STORE","PHILS","INSTITUTE","LIMITED","ENTERPRISES","VENTURES",'SHOP', 'BOUTIQUE','CLINIC', 'HOSPITAL','FINANCIAL', 'PETROL', 'GASSTATION',"FUEL","DRUG","TRAVEL","TOURS",'TOURISM', 'RESTAURANT','LTD', 'FINANCE', 'REGION', 'MARKETING','DOLE NCR', 'FOOD', 'BAKERY',"CONSTRUCTION","BUILDERS","SUPPLY MATERIALS",'JEWELRY', 'JEWELERS', 'EDUCATIONAL',"AUTO",'MOTORCYCLE', 'PARTS' ,'INSURANCE',"HEALTH","WELLNESS","REALESTATE","PROPERTIES"]

                if 'BRANCHCODE' not in list(df.columns):

                      df['BRANCHCODE'] = ''

                df['CORPORATE'] = False

                for corporate_key in corp_name_pattern:

                    df_temp = df[df['CORPORATE']==False]

                    df = df[df['CORPORATE']==True]


                    df_temp['CORPORATE'] = df_temp[config['FirstName']].str.contains(corporate_key)

                    df = pd.concat([df,df_temp],axis = 0)

                    df_temp = df[df['CORPORATE']==False]

                    df = df[df['CORPORATE']==True]

                    df_temp['CORPORATE'] = df_temp[config['LastName']].str.contains(corporate_key)

                    df = pd.concat([df,df_temp],axis = 0)


                df['valid'] = 'valid'

                df['reason'] = ''



                # age calculation

                df['IS_MINOR'] = ''

                df = df.apply(lambda row:minor_calculation(row),axis = 1)
                
                


                #using branch codes
                
                
                

                df['BRANCHCODE'].fillna('ZZZ',inplace  =True)
                
                df[df['BRANCHCODE']=='','BRANCHCODE'] = 'ZZZ'

                branch_codes = pd.read_excel('branch_code.xlsx',engine = 'openpyxl')

                branch_codes1 = dict(list(zip(branch_codes['PEPP Code'],branch_codes['Partner Name'])))


                df['some'] = ''
                
                

                df.loc[((df['CORPORATE']=='False') & (df['BRANCHCODE'].isin(branch_codes['PEPP Code']))),'CORPORATE'] = True
                
                df.loc[((df['CORPORATE']=='False') & (df['BRANCHCODE'].isin(branch_codes['PEPP Code']))),'some'] = df.loc[((df['CORPORATE']=='False') & (df['BRANCHCODE'].isin(branch_codes['PEPP Code']))),'BRANCHCODE']

                df['some'] = df['some'].replace(branch_codes1)

                df.loc[((df['CORPORATE']=='False') & (df['BRANCHCODE'].isin(branch_codes['PEPP Code']))),config['LastName']] = df.loc[((df['CORPORATE']=='False') & (df['BRANCHCODE'].isin(branch_codes['PEPP Code']))),'some']

                df.drop(columns = ['some'],inplace = True)
                

                corporate_customers = df[df['CORPORATE'] == True]

                df = df[df['CORPORATE']==False]
                

                print('single character check')
                print(len(df))

                df = df.apply(lambda row:single_character_check_firstname(row),axis = 1)

                df = df.apply(lambda row:single_character_check_lastname(row),axis = 1)

                print()

                print('numeric character check')

                print(len(df))

                df = df.apply(lambda row:number_check_firstname(row),axis = 1)

                df = df.apply(lambda row:number_check_lastname(row),axis = 1)

                print(len(df))
                print('contact details check')



                if 'CONTACT_DETAILS' not in list(df.columns):

                      df['CONTACT_DETAILS'] = ''

                df['length'] = df['CONTACT_DETAILS'].str.len()


                df_valid = ((df['CONTACT_DETAILS'].str.len()==10) & (df['CONTACT_DETAILS'].str.startswith('9')) | ((df['CONTACT_DETAILS'].str.len()==11) & (df['CONTACT_DETAILS'].str.startswith('0'))) | ((df['CONTACT_DETAILS'].str.len()==12) & (df['CONTACT_DETAILS'].str.startswith('6'))))

#                df_invalid_phone = ~((df['CONTACT_DETAILS'].str.len()==10 & df['CONTACT_DETAILS'].str.startswith('9'$

                df_invalid_phone = ~((df['CONTACT_DETAILS'].str.len()==10) & (df['CONTACT_DETAILS'].str.startswith('9')) | ((df['CONTACT_DETAILS'].str.len()==11) & (df['CONTACT_DETAILS'].str.startswith('0'))) | ((df['CONTACT_DETAILS'].str.len()==12) & (df['CONTACT_DETAILS'].str.startswith('6'))))


                print(len(df))
                # df.loc[df_valid,'valid'] = 'valid'

                df.loc[df_invalid_phone,'valid'] = 'invalid'

                df.loc[df_invalid_phone,'reason'] = 'phone number is invalid'

                print(len(df))

                print()



                print('special character check')



                df = df.apply(lambda row:special_character_check_firstname(row),axis = 1)

                df = df.apply(lambda row:special_character_check_lastname(row),axis = 1)


                print(len(df))

                if 'CustomerBirthDate' in df.columns:

                    df['CustomerBirthDate'] = df['CustomerBirthDate'].fillna('')


                df1 = pd.DataFrame()

                df2 = pd.DataFrame()



                print('Hash code generation started')

                missing_columns = set(config['HASH_1_columns'].split(',')) - set(df.columns)

                for xy in missing_columns:

                    df[xy] = ''

                missing_columns = set(config['HASH_2_columns'].split(',')) - set(df.columns)


                for xy in missing_columns:

                    df[xy] = ''

                for xy in config['HASH_1_columns'].split(','):

                    df[xy].fillna('',inplace = True)

                for xy in config['HASH_2_columns'].split(','):

                    df[xy].fillna('',inplace = True)


                print(len(df))

                df = df.apply(lambda row:hash(row,config['HASH_1_columns'].split(','),'HASH_1'),axis = 1)

                df = df.apply(lambda row:hash(row,config['HASH_2_columns'].split(','),'HASH_2'),axis = 1)


                print(len(df))


                final_df = df.copy()


                print(len(final_df))


                count = count+1


                final_df_duplicates1 = final_df[final_df.duplicated(['HASH_1'])]

                final_df = final_df[~(final_df.duplicated(['HASH_1']))]

                final_df_duplicates2 = final_df[final_df.duplicated(['HASH_2'])]

                final_df = final_df[~(final_df.duplicated(['HASH_2']))]


                final_df_duplicates = pd.concat([final_df_duplicates1,final_df_duplicates2],axis = 0)

                final_df_duplicates['valid'] = 'invalid'

                # final_df_duplicates['reason'] +=','

                final_df_duplicates.loc[final_df_duplicates['reason']=='','reason'] = 'duplicates in raw data'

                final_df_duplicates.loc[final_df_duplicates['reason']!='','reason'] += 'duplicates in raw data'

                print('mis spelling logic')

                print(len(final_df))

                print(time.time() - start_time)

                group = final_df.groupby(['LASTNAME','CUSTOMERADDRESS','DATEOFBIRTH'])['FIRSTNAME'].agg(list)
                
                group = group.reset_index()
                
                group.rename(columns = {'FIRSTNAME':'spell_check'},inplace = True)
                
                final_df = pd.merge(final_df,group,on = ['LASTNAME','CUSTOMERADDRESS','DATEOFBIRTH'],how = 'left')
                
                final_df['mis_spell'] = ''
                
                final_df = final_df.apply(lambda row:mis_spelled_apply_function(row),axis = 1)
                
                mis_spelled_duplicates_final = final_df[final_df['mis_spell']==0]
                
                final_df = final_df[final_df['mis_spell']==1]
                
                mis_spelled_duplicates_final['valid'] = 'invalid' 
                
                mis_spelled_duplicates_final['reason'] = 'duplicates by mis-spelling logic'


                print(len(final_df))


                print(time.time() - start_time)

                print(len(mis_spelled_duplicates_final))

#                print(hello)

                try:

                     final_df['FIRSTNAME'] = final_df['FIRSTNAME'].str.upper()

                     mis_spelled_duplicates_final['FIRSTNAME'] = mis_spelled_duplicates_final['FIRSTNAME'].str.upper()



                except:

                     pass


                # hashcode chcek API


                print('hashcode API check')



                duplicate_hash_df = pd.DataFrame()

                duplicate_hash_list = []

                count = 0

                for xy in range(0,(len(final_df)//500)):

                    hash_codes = final_df['HASH_1'][(xy*500):((xy*500)+500)]


                    body = {"hashCodes":list(hash_codes)}

                    response = requests.post(url = 'http://mr403s0332d.palawangroup.com:4200/getCustomerDataby3Hashcode',headers = {'X-AUTH-TOKEN':'eyJ1c2VybmFtZSI6InN5c3RlbSIsInRva2VuIjoiODRjOWZmNmQtZTllMy00MWUwLWI0MDctZmY5ZGQ5YjFmYWU4In0=','Content-Type':'application/json'},json = body,params = {'BusinessId':'9','isCustomer':True})

                    duplicate_hashcodes = response.json()

                    duplicate_hash_list.extend(duplicate_hashcodes)

                    count+=1

                    print(count)





                hash_codes = final_df[(len(final_df)//500)*500:]

                body = {"hashCodes":list(hash_codes)}

                response = requests.post(url = 'http://mr403s0332d.palawangroup.com:4200/getCustomerDataby3Hashcode',headers = {'X-AUTH-TOKEN':'eyJ1c2VybmFtZSI6InN5c3RlbSIsInRva2VuIjoiODRjOWZmNmQtZTllMy00MWUwLWI0MDctZmY5ZGQ5YjFmYWU4In0=','Content-Type':'application/json'},json = body,params = {'BusinessId':'9','isCustomer':True})

                duplicate_hashcodes = response.json()


                duplicate_hash_list.extend(duplicate_hashcodes)

                duplicate_hash = final_df[final_df['HASH_1'].isin(duplicate_hash_list)]

                final_df = final_df[~(final_df['HASH_1'].isin(duplicate_hash_list))]

                duplicate_hash_df = pd.concat([duplicate_hash_df,duplicate_hash],axis = 0)


                duplicate_hash_list = []


                for xy in range(0,(len(final_df)//500)):

                    hash_codes = final_df['HASH_2'][(xy*500):((xy*500)+500)]

                    body = {"hashCodes":list(hash_codes)}

                    response = requests.post(url = 'http://mr403s0332d.palawangroup.com:4200/getCustomerDataby3Hashcode',headers = {'X-AUTH-TOKEN':'eyJ1c2VybmFtZSI6InN5c3RlbSIsInRva2VuIjoiODRjOWZmNmQtZTllMy00MWUwLWI0MDctZmY5ZGQ5YjFmYWU4In0=','Content-Type':'application/json'},json = body,params = {'BusinessId':'9','isCustomer':True})


                    duplicate_hashcodes = response.json()

                    print()

                    print(response.status_code)

                    # duplicate_hash = final_df[final_df['HASH_2'].isin(duplicate_hashcodes)]

                    duplicate_hash_list.extend(duplicate_hashcodes)



                hash_codes = final_df[(len(final_df)//500)*500:]

                body = {"hashCodes":list(hash_codes)}


                response = requests.post(url = 'http://mr403s0332d.palawangroup.com:4200/getCustomerDataby3Hashcode',headers = {'X-AUTH-TOKEN':'eyJ1c2VybmFtZSI6InN5c3RlbSIsInRva2VuIjoiODRjOWZmNmQtZTllMy00MWUwLWI0MDctZmY5ZGQ5YjFmYWU4In0=','Content-Type':'application/json'},json = body,params = {'BusinessId':'9','isCustomer':True})

                duplicate_hashcodes = response.json()

                duplicate_hash_list.extend(duplicate_hashcodes)

                duplicate_hash = final_df[final_df['HASH_2'].isin(duplicate_hash_list)]

                duplicate_hash_df = pd.concat([duplicate_hash_df,duplicate_hash],axis = 0)


                final_df = final_df[~(final_df['HASH_2'].isin(duplicate_hash_list))]

                duplicates = duplicate_hash_df.copy()

                duplicates['reason'] = 'duplicates identified from system'


                invalid_records = final_df[final_df['valid']=='invalid']

                final_df = final_df[final_df['valid']=='valid']


                if os.path.exists(i.replace(config['replace_string'],config['replace_with'])+"//valid"):

                    pass

                else:

                    os.makedirs(i.replace(config['replace_string'],config['replace_with'])+"//valid")

                if os.path.exists(i.replace(config['replace_string'],config['replace_with'])+"//invalid"):

                    pass

                else:
                    
                    
                    os.makedirs(i.replace(config['replace_string'],config['replace_with'])+"//invalid")


                if ('Remarks_y' in final_df.columns):

                    pass

                else:

                    final_df['Remarks_y'] = ''

                    
                final_df.fillna('',inplace = True)

                final_df.replace('none','')

                # final_df['CONTACT_DETAILS'] = final_df[]

                final_df.rename(columns = {'EXPIRYDATE_x':'EXPIRYDATE','LOAD_DT_x':'LOAD_DT'},inplace = True)

                final_df['PRIMARYIDTYPE'] = ''

                final_df['PRIMARYID'] = ''
                    


                final_df.fillna('',inplace = True)


                final_df = final_df.replace('NONE','')

                final_df['ID'] = ''

                final_df['BIZ_ID'] = ''

                final_df['GEN_ID'] = ''

                final_df.to_csv(i.replace(config['replace_string'],config['replace_with'])+"//valid//"+transaction_file.replace('.csv','')+"_CDMS_output.csv",index  = False)

                final_df = pd.read_csv(i.replace(config['replace_string'],config['replace_with'])+"//valid//"+transaction_file.replace('.csv','')+"_CDMS_output.csv")




                final_df.fillna('',inplace = True)

                errored_df = final_df[(((final_df['EMAIL']=='') & (final_df['EMAIL_error']!='')) | ((final_df['LANDLINE_NO']=='') & (final_df['LANDLINE_NO_error']!='')) | ((final_df['EXPIRYDATE']=='') & (final_df['EXPIRYDATE_error']!='')) | ((final_df['LOAD_DT']=='') & (final_df['LOAD_DT_error']!='')) |((final_df['DATEOFBIRTH']=='') & (final_df['DATEOFBIRTH_error']!='')))]
                
                errored_df.to_csv(i.replace(config['replace_string'],config['replace_with'])+"//invalid//errored_out.csv",index  = False)

                # final_df = final_df[headers_final]



                final_df['Remarks_y'] = ''




                final_df.to_csv(i.replace(config['replace_string'],config['replace_with'])+"//valid//"+transaction_file.replace('.csv','')+"_CDMS_output.csv",index  = False)




                # duplicate_hash.to_csv('invalid//duplicates_'+business.loc[i,'File Name'],index  = False)

                final_df_duplicates['reason'] = 'duplicates in input'


                invalid_records = pd.concat([invalid_records,final_df_duplicates],axis = 0)



                invalid_records = pd.concat([invalid_records,duplicates],axis = 0)




                invalid_records = pd.concat([invalid_records,mis_spelled_duplicates_final],axis = 0)

                print(len(invalid_records))

                invalid_records.to_csv(i.replace(config['replace_string'],config['replace_with'])+"//invalid//"+transaction_file.replace('.csv','')+"_CDMS_output.csv",index  = False)

                corporate_customers.to_csv(i.replace(config['replace_string'],config['replace_with'])+"//invalid//"+transaction_file.replace('.csv','')+"_corporate_customers.csv",index  = False)

                print(len(final_df))


                print(len(corporate_customers))


                print('end')


                body = {

                    "fileName":transaction_file.replace('.csv','')+"_CDMS_output.csv",

                    "filePath":i.replace(config['replace_string'],config['replace_with'])+"//valid//",

                    "subListID":85,

                    "userID":149,

                    "businessHierarchyId":23


                }



                response = requests.post(url = 'http://mr403s0332d.palawangroup.com:4200/fileUploadExternalApi',headers = {'X-AUTH-TOKEN':'eyJ1c2VybmFtZSI6InN5c3RlbSIsInRva2VuIjoiODRjOWZmNmQtZTllMy00MWUwLWI0MDctZmY5ZGQ5YjFmYWU4In0=','Content-Type':'application/json'},json = body)

                print(response.status_code)

                upload_id = response.json()['content']['uploadId']

                print(upload_id)


                try:

                    producer = KafkaProducer(bootstrap_servers='MR402S0352D.palawangroup.com:9092')

                    topic = 'ftpKafkaConsumer'

                    my_dict = {'fileUploadId': upload_id, 'filePath': i.replace(config['replace_string'],config['replace_with'])+"//valid//", 'fileName': 'CDMS_value.csv',"subListID":85,"businessHierarchyId":23}
                    # my_dict = {'fileUploadId': 314, 'filePath': '/STFS0029M/1491702726149369/', 'fileName': 'sample$

                    my_dict = json.dumps(my_dict)

                    producer.send(topic, value=my_dict.encode('utf-8'))

                    print("Message sent successfully")


                except Exception as e:

                    print(f"Error: {e}")

                finally:

                    producer.close()

                with open('response.txt','r') as w:

                    text = w.read()

                with open('response.txt','w') as w:

                    w.write(text+str(count)+"."+i+"\n")
                    
                print(time.time() - start_time)

#
#                break



        except Exception as e:
        #else:
            print(str(e))

            print(traceback.print_exc())


            count = count+1


            with open('response.txt','r') as w:


                text = w.read()

            with open('response.txt','w') as w:

                w.write(text+"Error at file:"+str(count)+"."+i+"\n")





                

