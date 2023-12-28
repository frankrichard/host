# -*- coding: utf-8 -*-
"""
Created on Thu Dec 28 16:08:54 2023

@author: CBT
"""

import pandas as pd

import os

config = pd.read_excel('config.xlsx',engine = 'openpyxl')

config = dict(list(zip(config['key'],config['value'])))


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

print()

print(files_location)

df = pd.DataFrame(columns = ['Path',config['CDMS_file1'],config['CDMS_file2'],config['CDMS_file3'],config['CDMS_file4'],'overall_input'])

for i in range(0,len(files_location)):
    
    df.loc[i,'Path'] = files_location[i]

df = df.drop_duplicates(['Path'])

df.index = df['Path']



for i in files_location:
    
    
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

        df.loc[i,config['file1']] = len(file1)
        
        df.loc[i,config['file2']] = len(file2)

        df.loc[i,config['file3']] = len(file3)

        df.loc[i,config['file4']] = len(file4)

        df.loc[i,'overall_input'] = len(CDMS_merged)       

df.to_csv('input_count.csv')