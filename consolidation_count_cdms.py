# -*- coding: utf-8 -*-
"""
Created on Tue Dec 26 18:58:46 2023

@author: CBT
"""

import os

import pandas as pd
 
main_folder_path = ''
 
# Initialize a list to store information

data = []
 
for root, dirs, files in os.walk(main_folder_path):

    for file in files:

        if file.endswith('.csv'):

            file_path = os.path.join(root, file)
 
            try:

                # Try reading the CSV file

                df = pd.read_csv(file_path,sep = '|')
 
                # Get the relative path by removing the main folder path

                relative_path = os.path.relpath(file_path, main_folder_path)
 
                # Split the relative path into separate directories

                path_parts = relative_path.split(os.path.sep)
 
                # Extract the month and file name

                month = path_parts[0] if path_parts else None

                file_name = file.split('.')[0]  # remove file extension
 
                # Append information to the list

                data.append({'Month': month, 'File': file_name, 'Row Count': len(df)})
 
            except pd.errors.ParserError as e:

                # Handle the ParserError

                print(f"Error parsing {file_path}: {e}")
 
# Convert the list to a DataFrame

result_df = pd.DataFrame(data)
 
 
some = pd.pivot_table(result_df,columns = ['File'],index = ['Month'],values = ['Row Count'])
 
 
some.to_csv('frank.csv')
