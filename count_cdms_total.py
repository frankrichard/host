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


for i in files_location:
    
    
    if (config['CDMS_file1']) in os.listdir(i):
        
        
        print(i)
