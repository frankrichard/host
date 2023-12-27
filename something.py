
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 19:13:28 2023

@author: CBT
"""

import pandas as pd

import traceback

import hashlib

import json

import difflib

from kafka import KafkaProducer

import sys

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

    for i, name1 in enumerate(names):

        for j, name2 in enumerate(names):

            # Skip comparing a name to itself

            if i != j:

                # Calculate similarity between names

                similarity = difflib.SequenceMatcher(None, name1, name2).ratio()

                # Set a threshold for similarity (you may need to adjust this)
                similarity_threshold = 0.8

                # If similarity is below the threshold, consider it misspelled
                if similarity > similarity_threshold and name1.lower() not in misspelled_names:

                    misspelled_names.append(name1.lower())

    return [i.upper() for i in misspelled_names]

def special_character_check_firstname(row):

    special_char=re.compile("//;,=?\[]\{}\(\)_`+&*'~#􀀀")

    if special_char.search(row[config['FirstName']]) != None:
        
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
            
            
    return row

def special_character_check_lastname(row):

    special_char=re.compile("\/;,=?\[]\{}\(\)_`+&*'~#􀀀")
    
    if special_char.search(row[config['LastName']]) != None:
        
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
    
    if len(re.findall(r'[0-9]+', row[config['FirstName']]))>0:
        
        
        if len(row[config['LastName']].split(' '))>1:
            
            
            temp_last_name = row[config['LastName']].split(' ')
            
            row[config['FirstName']] = ' '.join(temp_last_name[0:len(temp_last_name)-1])
            
            row[config['LastName']] = temp_last_name[len(temp_last_name)-1]
        
        else:
            
            
            row['valid'] = 'invalid'

            if row['reason']=='':
            
                row['reason'] = 'Numeric characters in First name'
                
            else:
                
                row['reason'] += ',Numeric characters in first name'
            
            
    return row




def number_check_lastname(row):
    
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

total_dataframe = pd.DataFrame()



for i in files_location:
    
    
    try:
    
    
        
        if (config['CDMS_file1']) in os.listdir(i):
            
            
            print(i)
            
            
                
            file1 = pd.read_csv(i+"//"+config['CDMS_file1'],encoding='ISO-8859-1', sep="|")
            
            file2 = pd.read_csv(i+'//'+config['CDMS_file2'],encoding='ISO-8859-1', sep="|")
            
            file3 = pd.read_csv(i+"//"+config['CDMS_file3'],encoding='ISO-8859-1', sep="|")
            
            file4 = pd.read_csv(i+'//'+config['CDMS_file4'],encoding='ISO-8859-1', sep="|")
        
            headers = pd.read_csv('headers_matching.csv')
            
            headers = dict(list(zip(headers['key'],headers['value'])))
            
            file1.rename(columns = headers,inplace = True)
            
            file2.rename(columns = headers,inplace = True)
            
            file3.rename(columns = headers,inplace = True)
            
            file4.rename(columns = headers,inplace = True)
                
            
            
            #Merging all the files
            
            CDMS_merged = pd.merge(file1,file2,how = 'left',on = [config['customer_id']])
            
            CDMS_merged = pd.merge(CDMS_merged,file3,how = 'left',on = [config['customer_id']])
            
            CDMS_merged = pd.merge(CDMS_merged,file4,how = 'left',on = [config['customer_id']])
            
            # print('missed columns:',set(CDMS_merged) - set(headers.values()))
            
            #merging_address
            
            CDMS_merged[config['CustomerAddress']] = CDMS_merged[config['ADDRESS1']] + CDMS_merged[config['ADDRESS2']] + CDMS_merged[config['ADDRESS3']] + CDMS_merged[config['ADDRESS4']]
    
            #Columns rename
            
            # renaming_columns = dict(list(zip(config['columns_present'].split(','),config['columns_to_be_changed'].split(','))))
            
            # CDMS_merged.rename(columns = renaming_columns,inplace = True)
            
            print(i.replace(config['replace_string'],config['replace_with']+"//CDMS_output.csv"))
            
            #Creating Hash Code function
            
            
            CDMS_merged_HASH_1 = pd.DataFrame()
            
            CDMS_merged_HASH_2 = pd.DataFrame()
            
            # hash(CDMS_merged,CDMS_merged_HASH_1,config['HASH_1_columns'].split(','))
            
            # CDMS_output = pd.concat([CDMS_merged,CDMS_merged_HASH_1],axis = 1)
            
            # CDMS_output.rename(columns = {'hash_'+''.join(config['HASH_1_columns'].split(',')):'HASH_1'},inplace = True)
            
            # hash(CDMS_merged,CDMS_merged_HASH_2,config['HASH_2_columns'].split(','))
            
            # CDMS_output = pd.concat([CDMS_output,CDMS_merged_HASH_2],axis = 1)
            
            # CDMS_output.rename(columns = {'hash_'+''.join(config['HASH_2_columns'].split(',')):'HASH_2'},inplace = True)
            
            # CDMS_output = CDMS_output[config['output_columns'].split(',')]
            
            CDMS_output = CDMS_merged.copy()
            
            print(i.replace(config['replace_string'],config['replace_with'])+"//CDMS_output.csv")
            
            if os.path.exists(i.replace(config['replace_string'],config['replace_with'])):
                
                pass
            
            else:
                
                os.makedirs(i.replace(config['replace_string'],config['replace_with']))
    
    
    
    
            
            # CDMS_output.to_csv(i.replace(config['replace_string'],config['replace_with'])+"//CDMS_output.csv",index = False)
            
            
    
            
            # print(business.loc[i,'File Name'])
            
            df = CDMS_output.copy()
            
            print('Input file Length')
            
            print(len(df))
            
            
            
            #rule1 upper case and accent change
            
            
            
            df[config['FirstName']] = df[config['FirstName']].fillna('')   
            
            df[config['LastName']] = df[config['LastName']].fillna('')   
            
            df[config['CustomerAddress']] = df[config['CustomerAddress']].fillna('')   
    
    
            
    
    
    
            

             
            
            df[config['FirstName']] = df[config['FirstName']].str.upper()   
            
            df['CustomerLasttName'] = df[config['LastName']].str.upper()   
            
            df[config['CustomerAddress']] = df[config['CustomerAddress']].str.upper()  
    
    
            #dela cruz
            
            last_name_words = config['lastname_words']
            
            for words in last_name_words.split(','):
                
                df[config['FirstName']] = df[config['FirstName']].str.replace(words.upper(),words.upper().replace(' ','-')) 
    
                df[config['LastName']] = df[config['LastName']].str.replace(words.upper(),words.upper().replace(' ','-')) 
    
            
            
            #GYU code
                
            df = df.apply(lambda row:GYU(row),axis = 1)
            
                    
    
    
            #change_accent
            
            for column in df.columns:
                
                df[column] = df[column].str.unidecode()

            
            
            df = df.apply(lambda row:change_accent(row),axis = 1)
            
            #email validation
            
            df['EMAIL']= df[df['EMAIL'].str.contains(email_regex, na=False)]['EMAIL']

                        


            #adding suffix
            
            suffixes = [ ' I', ' II', ' III', ' IV', ' V', ' JR', ' SR']    
            
            df['SUFFIX'] = ''
            
            for suffix in suffixes:
                
                df.loc[df[config['FirstName']].str.contains(suffix),'SUFFIX'] = suffix 
            
                df[config['FirstName']].str.replace(suffix,'')
            
                df.loc[df[config['LastName']].str.contains(suffix),'SUFFIX'] = suffix 
            
                df[config['LastName']].str.replace(suffix,'')
            
            # corporate customers
            
            print('corporate customers')
            
            corp_name_pattern = ['ACADEMY',"TECHNOLOGY" ,"TECHNO", "STALL","SERVICES","BRANCH","OUTLET","EXPRESS","CENTER","BUSINESS","CORPORATION","COMPANY"," INC","INC ","COURIER","COOPERATIVE","BANK","SECURITY","DISTRIBUTOR","DISTILLERS","PHARMACY","MOTORS","SCHOOL","TRADEING","ACCOUNTS","ASSOCIATION","UNIV","COLLEGES","MERCHANT/MERCHANDIZING","STORE","PHILS","INSTITUTE","LIMITED","ENTERPRISES","VENTURES",'SHOP', 'BOUTIQUE','CLINIC', 'HOSPITAL','FINANCIAL', 'PETROL', 'GASSTATION',"FUEL","DRUG","TRAVEL","TOURS",'TOURISM', 'RESTAURANT','LTD', 'FINANCE', 'REGION', 'MARKETING','DOLE NCR', 'FOOD', 'BAKERY',"CONSTRUCTION","BUILDERS","SUPPLY MATERIALS",'JEWELRY', 'JEWELERS', 'EDUCATIONAL',"AUTO",'MOTORCYCLE', 'PARTS' ,'INSURANCE',"HEALTH","WELLNESS","REALESTATE","PROPERTIES"]
            
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
            
            #using branch codes
            
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
            
            #single character
            
            
            
            
            # df['count'] = df[config['FirstName']].str.len()
            
            # df_temp = df[df['count']<2]
            
            # df = df[df['count']>=2]
            
            # temp_split = df_temp[config['LastName']].str.split(' ')
            
            # df_temp = pd.concat([df_temp,temp_split],axis = 1)
            
            valid_count = 0
            
            invalid_count = 0
            
            
            while(True):
            
            
            
                print()
                
                print('single character check')
                
                
                df = df.apply(lambda row:single_character_check_firstname(row),axis = 1)
                
                df = df.apply(lambda row:single_character_check_lastname(row),axis = 1)
                
                print()
                
                print('numeric character check')
                
                df = df.apply(lambda row:number_check_firstname(row),axis = 1)
                
                df = df.apply(lambda row:number_check_lastname(row),axis = 1)
                
        
                print('contact details check')
                
                df['length'] = df['CONTACT_DETAILS'].str.len()
                
                # df.loc[(df['length']==10) & (df['CONTACT_DETAILS'])]
                
                df_valid = ((df['CONTACT_DETAILS'].str.len()==10) & (df['CONTACT_DETAILS'].str.startswith('9')) | ((df['CONTACT_DETAILS'].str.len()==11) & (df['CONTACT_DETAILS'].str.startswith('0'))) | ((df['CONTACT_DETAILS'].str.len()==12) & (df['CONTACT_DETAILS'].str.startswith('6'))))
                
                df_invalid_phone = ~((df['CONTACT_DETAILS'].str.len()==10) & (df['CONTACT_DETAILS'].str.startswith('9')) | ((df['CONTACT_DETAILS'].str.len()==11) & (df['CONTACT_DETAILS'].str.startswith('0'))) | ((df['CONTACT_DETAILS'].str.len()==12) & (df['CONTACT_DETAILS'].str.startswith('6'))))
                
                # df.loc[df_valid,'valid'] = 'valid'
                
                df.loc[df_invalid_phone,'valid'] = 'invalid'
        
                df.loc[df_invalid_phone,'reason'] = 'phone number is invalid'
                
                
        
                print()    
                
                
                
                print('special character check')
                
                
                
                df = df.apply(lambda row:special_character_check_firstname(row),axis = 1)
                
                df = df.apply(lambda row:special_character_check_lastname(row),axis = 1)
                
                if ((valid_count == df['valid'].value_counts()['valid']) and (invalid_count == df['valid'].value_counts()['invalid'])):
                    
                    break
    
                else:
                    
                    valid_count = df['valid'].value_counts()['valid']
                    
                    invalid_count = df['valid'].value_counts()['invalid']
                
            
            missing_columns = set(config['HASH_1_columns'].split(',')) - set(df.columns)
            
            for xy in missing_columns:
                
                df[xy] = ''
            
            missing_columns = set(config['HASH_2_columns'].split(',')) - set(df.columns)
            
            for xy in missing_columns:
                
                df[xy] = ''
            
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
    
    
            
            df = df.apply(lambda row:hash(row,config['HASH_1_columns'].split(','),'HASH_1'),axis = 1)
            
            df = df.apply(lambda row:hash(row,config['HASH_2_columns'].split(','),'HASH_2'),axis = 1)
            

            total_dataframe  = pd.concat([total_dataframe,df],axis = 0)
            
            print(len(total_dataframe))







                
            count = count+1
                
                
            with open('response.txt','r') as w:
                
                text = w.read()

            with open('response.txt','w') as w:
                
                w.write(text+str(count)+"."+i+"\n")
                                    
                
    except Exception as e:
    
        print(str(e))            
        
        print(traceback.print_exc())
                
            
        count = count+1
            
            
        with open('response.txt','r') as w:
            
            text = w.read()

        with open('response.txt','w') as w:
            
            w.write(text+"Error at file:"+str(count)+"."+i+"\n")








final_df = total_dataframe.copy()

final_df_duplicates1 = final_df[final_df.duplicated(['HASH_1'])]

final_df = final_df[~(final_df.duplicated(['HASH_1']))]

final_df_duplicates2 = final_df[final_df.duplicated(['HASH_2'])]

final_df = final_df[~(final_df.duplicated(['HASH_2']))]

final_df_duplicates = pd.concat([final_df_duplicates1,final_df_duplicates2],axis = 0)                    

final_df_duplicates['valid'] = 'invalid'
    
# final_df_duplicates['reason'] +=',' 

final_df_duplicates.loc[final_df_duplicates['reason']=='','reason'] = 'duplicates in raw data'

final_df_duplicates.loc[final_df_duplicates['reason']!='','reason'] += 'duplicates in raw data'


#mis spelling logic
print('mis spelling logic')

mis_spelled = final_df[final_df.duplicated(keep='first',subset = [config['LastName'],config['CustomerAddress'],config['CustomerDOB']]) | final_df.duplicated(keep='last',subset = [config['LastName'],config['CustomerAddress'],config['CustomerDOB']])]

mis_spelled_unique = mis_spelled[~(mis_spelled.duplicated([config['LastName'],config['CustomerAddress'],config['CustomerDOB']]))]

final_df = final_df[~(final_df.duplicated(keep='first',subset = [config['LastName'],config['CustomerAddress'],config['CustomerDOB']]) | final_df.duplicated(keep='last',subset = [config['LastName'],config['CustomerAddress'],config['CustomerDOB']]))]

mis_spelled_duplicates_final = pd.DataFrame()

mis_spelled_unique.reset_index(inplace = True,drop = True)

for xy in range(0,len(mis_spelled_unique)):
    
    lastname = mis_spelled_unique.loc[xy,config['LastName']]

    address = mis_spelled_unique.loc[xy,config['CustomerAddress']]

    dob = mis_spelled_unique.loc[xy,config['CustomerDOB']]

    temp_df = mis_spelled[((mis_spelled[config['LastName']]==lastname) & (mis_spelled[config['CustomerAddress']]==address) & (mis_spelled[config['CustomerDOB']]==dob))]

    identical = identify_misspelled_names(list(temp_df[config['FirstName']]))
    
    if len(identical)>0:
        
        set1 = set(list(temp_df[config['FirstName']]))
    
        final_set = list(set1 - set(identical))
        
        final_set.append(identical[0])

        final_df = pd.concat([final_df,mis_spelled[mis_spelled[config['FirstName']].isin(final_set)]],axis= 0)

        final_set = identical[1:]

        mis_spelled_duplicates = mis_spelled[mis_spelled[config['FirstName']].isin(final_set)]
        
        mis_spelled_duplicates_final = pd.concat([mis_spelled_duplicates_final,mis_spelled_duplicates],axis = 0)
        
    else:
        
        final_df = pd.concat([final_df,temp_df],axis = 0)

    
mis_spelled_duplicates_final['valid'] = 'invalid'

mis_spelled_duplicates_final['reason'] = 'duplicates by mis-spelling logic'

# final_df_duplicates = final_df[final_df.duplicated(['HASH_2'])]

# final_df = final_df[~(final_df.duplicated(['HASH_2']))]

# final_df_duplicates['valid'] = 'invalid'
    
# # final_df_duplicates['reason'] +=',' 

# final_df_duplicates.loc[final_df_duplicates['reason']=='','reason'] = 'duplicates in raw data'

# final_df_duplicates.loc[final_df_duplicates['reason']!='','reason'] += 'duplicates in raw data'


# cdms = pd.read_csv('CDMS_merged.csv')

























# hashcode chcek API





# duplicate_hash_df = pd.DataFrame()

# duplicate_hash_list = []

# for xy in range(0,(len(final_df)//500)):
    
#     hash_codes = final_df['HASH_1'][(xy*500):((xy*500)+500)]

#     body = {"hashCodes":hash_codes}

#     response = requests.post(url = 'http://mr403s0332d.palawangroup.com:4200/getCustomerDataby3Hashcode',headers = {'X-AUTH-TOKEN':'eyJ1c2VybmFtZSI6InN5c3RlbSIsInRva2VuIjoiODRjOWZmNmQtZTllMy00MWUwLWI0MDctZmY5ZGQ5YjFmYWU4In0=','Content-Type':'application/json'},json = body,params = {'BusinessId':'9','isCustomer':True})

#     duplicate_hashcodes = response.json()     
    
#     print()
        
#     print(response.status_code)

    
#     # duplicate_hash = final_df[final_df['HASH_1'].isin(duplicate_hashcodes)]
    
#     duplicate_hash_list.append(duplicate_hashcodes)
    
#     # duplicate_hash_df = pd.concat([duplicate_hash_df,duplicate_hash],axis = 0)
    
#     # valid_output = valid_output[~(valid_output['HASH_1'].isin(duplicate_hashcodes))]



# hash_codes = final_df[(len(final_df)//500)*500:]

# body = {"hashCodes":hash_codes}

# response = requests.post(url = 'http://mr403s0332d.palawangroup.com:4200/getCustomerDataby3Hashcode',headers = {'X-AUTH-TOKEN':'eyJ1c2VybmFtZSI6InN5c3RlbSIsInRva2VuIjoiODRjOWZmNmQtZTllMy00MWUwLWI0MDctZmY5ZGQ5YjFmYWU4In0=','Content-Type':'application/json'},json = body,params = {'BusinessId':'9','isCustomer':True})

# duplicate_hashcodes = response.json()        

# duplicate_hash_list.append(duplicate_hashcodes)

# duplicate_hash = final_df[final_df['HASH_1'].isin(duplicate_hashcodes)]

# final_df = final_df[~(final_df['HASH_1'].isin(duplicate_hashcodes))]

# duplicate_hash_df = pd.concat([duplicate_hash_df,duplicate_hash],axis = 0)

# # valid_output = final_df[~(valid_output['HASH_1'].isin(duplicate_hashcodes))]


# duplicate_hash_list = []


# for xy in range(0,(len(final_df)//500)):
    
#     hash_codes = final_df['HASH_2'][(xy*500):((xy*500)+500)]

#     body = {"hashCodes":hash_codes}

#     response = requests.post(url = 'http://mr403s0332d.palawangroup.com:4200/getCustomerDataby3Hashcode',headers = {'X-AUTH-TOKEN':'eyJ1c2VybmFtZSI6InN5c3RlbSIsInRva2VuIjoiODRjOWZmNmQtZTllMy00MWUwLWI0MDctZmY5ZGQ5YjFmYWU4In0=','Content-Type':'application/json'},json = body,params = {'BusinessId':'9','isCustomer':True})

#     duplicate_hashcodes = response.json()     

#     print()
        
#     print(response.status_code)
    
#     # duplicate_hash = final_df[final_df['HASH_2'].isin(duplicate_hashcodes)]

#     duplicate_hash_list.append(duplicate_hashcodes)

    
#     # duplicate_hash_df = pd.concat([duplicate_hash_df,duplicate_hash],axis = 0)
    
#     # valid_output = valid_output[~(valid_output['HASH_2'].isin(duplicate_hashcodes))]
    
    

# hash_codes = final_df[(len(final_df)//500)*500:]

# body = {"hashCodes":hash_codes}

# response = requests.post(url = 'http://mr403s0332d.palawangroup.com:4200/getCustomerDataby3Hashcode',headers = {'X-AUTH-TOKEN':'eyJ1c2VybmFtZSI6InN5c3RlbSIsInRva2VuIjoiODRjOWZmNmQtZTllMy00MWUwLWI0MDctZmY5ZGQ5YjFmYWU4In0=','Content-Type':'application/json'},json = body,params = {'BusinessId':'9','isCustomer':True})

# duplicate_hashcodes = response.json()  

# duplicate_hash_list.append(duplicate_hashcodes)      

# duplicate_hash = final_df[final_df['HASH_2'].isin(duplicate_hash_list)]

# duplicate_hash_df = pd.concat([duplicate_hash_df,duplicate_hash],axis = 0)

# # valid_output = valid_output[~(valid_output['HASH_2'].isin(duplicate_hashcodes))]

# final_df = final_df[~(final_df['HASH_2'].isin(duplicate_hash_list))]

# duplicates = duplicate_hash_df.copy()

# duplicates['reason'] = 'duplicates identified from system'











#completion of API check on hash codes












#     # response = requests.post(url = 'http://mr403s0332d.palawangroup.com:4200/fileUploadExternalApi',headers = {'X-AUTH-TOKEN':'eyJ1c2VybmFtZSI6InN5c3RlbSIsInRva2VuIjoiODRjOWZmNmQtZTllMy00MWUwLWI0MDctZmY5ZGQ5YjFmYWU4In0=','Content-Type':'application/json'},json = body)


# duplicates.to_csv('invalid//duplicates_'+business.loc[i,'File Name'],index  = False)

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


    
final_df.to_csv("/STFS0029M/migration_data/overall/valid//CDMS_output.csv",index  = False)

# duplicate_hash.to_csv('invalid//duplicates_'+business.loc[i,'File Name'],index  = False)

final_df_duplicates['reason'] = 'duplicates in input'


invalid_records = pd.concat([invalid_records,final_df_duplicates],axis = 0)









#hashcode API check

# invalid_records = pd.concat([invalid_records,duplicates],axis = 0)


#hashcode API check










invalid_records = pd.concat([invalid_records,mis_spelled_duplicates_final],axis = 0)

print(len(invalid_records))

invalid_records.to_csv("/STFS0029M/migration_data/overall/invalid//CDMS_output.csv",index  = False)

corporate_customers.to_csv("/STFS0029M/migration_data/overall/invalid//corporate_customers.csv",index  = False)


print(len(corporate_customers))

# print(CDMS_output.columns)

body = {

    "fileName":"CDMS_output.csv",

    "filePath":"/STFS0029M/migration_data/overall/valid/",

    "subListID":76,

    "userID":149,

    "businessHierarchyId":23

}



response = requests.post(url = 'http://mr403s0332d.palawangroup.com:4200/fileUploadExternalApi',headers = {'X-AUTH-TOKEN':'eyJ1c2VybmFtZSI6InN5c3RlbSIsInRva2VuIjoiODRjOWZmNmQtZTllMy00MWUwLWI0MDctZmY5ZGQ5YjFmYWU4In0=','Content-Type':'application/json'},json = body)

print(response.status_code)

upload_id = response.json()['content']['uploadId']


try:

    producer = KafkaProducer(bootstrap_servers='MR402S0352D.palawangroup.com:9092')

    topic = 'ftpKafkaConsumer'
 
    my_dict = {'fileUploadId': upload_id, 'filePath': "/STFS0029M/migration_data/overall/valid/", 'fileName': 'CDMS_value.csv'}
    # my_dict = {'fileUploadId': 314, 'filePath': '/STFS0029M/1491702726149369/', 'fileName': 'sampleDoc (98).csv'}

    my_dict = json.dumps(my_dict)

    producer.send(topic, value=my_dict.encode('utf-8'))

    print("Message sent successfully")
 
except Exception as e:

    print(f"Error: {e}")

finally:
    producer.close()
