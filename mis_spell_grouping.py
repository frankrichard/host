# -*- coding: utf-8 -*-
"""
Created on Sun Jan  7 20:38:17 2024

@author: CBT
"""

import pandas as pd

import numpy as np

import difflib



def identify_misspelled_names(names):
    # Identify misspelled names (excluding identical names)

    misspelled_names = []

    misspelled_names1 = []
    
    names1 = names.copy()

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

        names1.remove(name2)  

    return misspelled_names,misspelled_names1




def mis_spelled_apply_function(row):

    print(row['FIRSTNAME'])
    
    unique,duplicates = identify_misspelled_names(row['spell_check'])
    
    if ((row['FIRSTNAME'] in unique) or (len(row['spell_check'])<2)):
        
        row['mis_spell'] = '1'
        
    else:
        
        row['mis_spell'] = '0'

    return row
    


final_df = pd.DataFrame(np.random.choice(3,(10,4)))

final_df.rename(columns = {0:'FIRSTNAME',1:'LASTNAME',2:"CUSTOMERADDRESS",3:'DATEOFBIRTH'},inplace = True)

final_df['FIRSTNAME'] = ['frank','richard','richerd','frenk','sundharam','chandru','zenifer','madhu','anand','vijay']

final_df['LASTNAME'] = ['richard','john','john','richard','felix','sagayaraj','doss','kevin','vijay','gopi']

final_df['CUSTOMERADDRESS'] = 8

final_df['DATEOFBIRTH'] = 0

group = final_df.groupby(['LASTNAME','CUSTOMERADDRESS','DATEOFBIRTH'])['FIRSTNAME'].agg(list)

group = group.reset_index()

group.rename(columns = {'FIRSTNAME':'spell_check'},inplace = True)

final_df = pd.merge(final_df,group,on = ['LASTNAME','CUSTOMERADDRESS','DATEOFBIRTH'],how = 'left')

final_df['mis_spell'] = ''

final_df = final_df.apply(lambda row:mis_spelled_apply_function(row),axis = 1)

mis_spelled_duplicates_final = final_df[final_df['mis_spell']=='0']

final_df = final_df[final_df['mis_spell']=='1']

mis_spelled_duplicates_final['valid'] = 'invalid' 

mis_spelled_duplicates_final['reason'] = 'duplicates by mis-spelling logic'













