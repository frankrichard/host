# -*- coding: utf-8 -*-
"""
Created on Tue Dec 26 11:58:44 2023

@author: CBT
"""

import pandas as pd

import os

def list_all_files_in_drive(drive):

    all_files = []

    for root, dirs, files in os.walk(drive):

        for file in files:

            file_path = os.path.join(root, file)

            all_files.append(file_path)

    return all_files

drive_to_list = 'C:\\Users\\CBT\\Desktop\\richard'

files_in_drive = list_all_files_in_drive(drive_to_list)

files_location = []

for file in files_in_drive:
    
    file = file.replace('\\','/')
    
    list1 = file.split('/')    
    
    list1.pop()
    
    list1 = '/'.join(list1)
                       
    files_location.append(list1)

files_location = list(set(files_location))

df = pd.DataFrame(columns = ['Path','valid','invalid','corporate'])

for i in range(0,len(files_location)):
    
    df.loc[i,'Path'] = files_location[i].replace('valid/CSDMS_output.csv','').replace('invalid/CDMS_output.csv','').replace('invalid/corporate_customers.csv','')
    
df.to_csv('index.csv',index = False)

df = df.drop_duplicates(['Path'])

df.index = df['Path']

for i in files_location:

    print(i.replace('valid/CSDMS_output.csv','').replace('invalid/CDMS_output.csv','').replace('invalid/corporate_customers.csv',''))
    
    some = i.replace('valid/CSDMS_output.csv','').replace('invalid/CDMS_output.csv','').replace('invalid/corporate_customers.csv','')
    
    print(i)
    
    if i.endswith('valid/CDMS_output.csv'):
        
        print('valid')
        
        valid = pd.read_csv(i)
        
        df.loc[some,'valid'] = len(valid)
    
    elif i.endswith('invalid/CDMS_output.csv'):
        
        print('invalid')
        
        invalid = pd.read_csv(i)
        
        df.loc[some,'invalid'] = len(invalid)
    
    elif i.endswith('invalid/corporate_customers.csv'):
        
        print('corporate')
        
        corporate = pd.read_csv(i)
        
        df.loc[some,'corporate'] = len(corporate)
    
    
df.to_csv('consolidation.csv')    
    

# files_location = files_location[0:1]











