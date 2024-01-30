# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 15:51:20 2024

@author: CBT
"""




import pandas as pd

config = pd.read_excel('config.xlsx',engine = 'openpyxl')

config = dict(list(zip(config['key'],config['value'])))


import difflib

def mis_spelled_identify(row):
    
    if row['FIRSTNAME'] in row['unique']:
        
        row['mis_spell'] = '1'
        
    else:
        
        row['mis_spell'] = '0'


def mis_spelled_apply_function(row):

    
    unique,duplicates = identify_misspelled_names(row['spell_check'])

    row['unqiue'] = unique
    
    # if ((row['FIRSTNAME'] in unique) or (len(row['spell_check'])<2)):
        
    #     row['mis_spell'] = '1'
        
    # else:
        
    #     row['mis_spell'] = '0'

    return row



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






df = pd.read_csv('output/three_hash.csv')

final_df = df.copy()

group = final_df.groupby(['LASTNAME','CUSTOMERADDRESS','DATEOFBIRTH'])['FIRSTNAME'].agg(list)

group = group.reset_index()

group['unique'] = ''

group.rename(columns = {'FIRSTNAME':'spell_check'},inplace = True)

group = group.apply(lambda row:mis_spelled_apply_function(row),axis = 1)

final_df = pd.merge(final_df,group,on = ['LASTNAME','CUSTOMERADDRESS','DATEOFBIRTH'],how = 'left')

final_df['mis_spell'] = ''

#                final_df = final_df.apply(lambda row:mis_spelled_apply_function(row),axis = 1)

final_df = final_df.apply(lambda row:mis_spelled_identify(row),axis = 1)

print(len(final_df))

mis_spelled_duplicates_final = final_df[final_df['mis_spell']=='0']

final_df = final_df[final_df['mis_spell']=='1']

print('mis spelled',len(mis_spelled_duplicates_final))

print('correct',len(final_df))

#                print(final_df['valid'].value_counts())



mis_spelled_duplicates_final['valid'] = 'invalid'

mis_spelled_duplicates_final['reason'] = 'duplicates by mis-spelling logic'

mis_spelled_duplicates_final.to_csv('mis_spelled.csv',index = False)

final_df.to_csv('mis_spelled_unique.csv',index = False)


print('mis_spelling logic completed')






