# -*- coding: utf-8 -*-

import requests, zipfile, io
import numpy as np
import pandas as pd
from pandas import DataFrame as df
    
###STEP 1: DOWNLOAD ALL THE ZIP FILES ###
base_dls = "https://www.eia.gov/electricity/data/eia861/zip/f861"
zip_dict = {}
###naming of files is fucked up, dealing with it....
###populate zip_dict, a catalog of the zip files



for i in np.arange(90, 100):
    year = str(i)
    dls = base_dls + year + ".zip"
    print(dls)    
    zip_dict[year] = zipfile.ZipFile(io.BytesIO(requests.get(dls).content))
for i in np.arange(0, 10):
    year = '0' + str(i)
    dls = base_dls + year + ".zip"
    print(dls)    
    zip_dict[year] = zipfile.ZipFile(io.BytesIO(requests.get(dls).content)) 
for i in np.arange(10, 12):
    year = str(i)
    dls = base_dls + year + ".zip"
    print(dls)    
    zip_dict[year] = zipfile.ZipFile(io.BytesIO(requests.get(dls).content))
for i in np.arange(2012, 2016):
    year = str(i)
    dls = base_dls + year + ".zip"
    print(dls)    
    zip_dict[year] = zipfile.ZipFile(io.BytesIO(requests.get(dls).content))
    
###STEP 2: EXTRACT THE XLS/XLSX FILES FROM THE ZIP FILES ###
    
###file name conventions
    #90-98: F861TYP1.xls
    #99-00: FILE2.xls
    #01-05: YEAR/file2.xls
    #06: file2.xls
    #07-09: YEAR/file2_YEAR.xls
    #10-11: file2_YEAR.xls
    #12: retail_sales_2012.xls
    #13-15: Sales_Ult_Cust_YEAR.xls
##create dictionary of dataframe for the apporpriate docs
xls_dict = {}



for i in np.arange(90, 99):
    key = str(i)
    xls_dict['19' + key] = pd.read_excel(zip_dict[key].extract('F861TYP1.xls'))
###99 and '00 have a distinct file name    

xls_dict['1999'] = pd.read_excel(zip_dict['99'].extract('FILE2.xls')) 

xls_dict['2000'] = pd.read_excel(zip_dict['00'].extract('FILE2.xls'))   

for i in np.arange(1, 6):
    key = '0' + str(i)
    file_name = '20' + key + '/file2.xls'
    xls_dict['20' + key] = pd.read_excel(zip_dict[key].extract(file_name))

xls_dict['2006'] = pd.read_excel(zip_dict['06'].extract('file2.xls'))

xls_dict['2007'] = pd.read_excel(zip_dict['07'].extract('2007/file2_2007.xls'))

# '08 to '15 needs to be uploaded without headers ('header = none')
# Then use the script below to merge the cells
#then set them as headers
          #df = pd.DataFrame(df[1:], columns = [df[0]] )
for i in np.arange(8, 10):
    key = '0' + str(i) 
    file_name = '20' + key + '/file2_20' + key + '.xls'    
    xls_dict['20' + key] = pd.read_excel(zip_dict[key].extract(file_name), header = None)
    xls_dict['20' + key][:3] = xls_dict['20' + key][:3].fillna('')
    xls_dict['20' + key].columns = xls_dict['20' + key].iloc[0:3].apply(lambda x: '.'.join([y for y in x if y]), axis=0)
    xls_dict['20' + key] = xls_dict['20' + key][3:]

for i in np.arange(10, 12):
    key = str(i)
    file_name = 'file2_20' + key + '.xls'
    xls_dict['20' + key] = pd.read_excel(zip_dict[key].extract(file_name), header = None)
    xls_dict['20' + key][:3] = xls_dict['20' + key][:3].fillna('')
    xls_dict['20' + key].columns = xls_dict['20' + key].iloc[0:3].apply(lambda x: '.'.join([y for y in x if y]), axis=0)
    xls_dict['20' + key] = xls_dict['20' + key][3:]

    
xls_dict['2012'] = pd.read_excel(zip_dict['2012'].extract('retail_sales_2012.xls'), header = None)   
xls_dict['2012'][:3] = xls_dict['2012'][:3].fillna('')
xls_dict['2012'].columns = xls_dict['2012'].iloc[0:3].apply(lambda x: '.'.join([y for y in x if y]), axis=0)
xls_dict['2012'] = xls_dict['2012'][3:]


    
for i in np.arange(2013, 2015):
    key = str(i)
    file_name = 'Sales_Ult_Cust_' + key + '.xls'
    xls_dict[key] = pd.read_excel(zip_dict[key].extract(file_name), header = None)
    xls_dict[key][:3] = xls_dict[key][:3].fillna('')
    xls_dict[key].columns = xls_dict[key].iloc[0:3].apply(lambda x: '.'.join([y for y in x if y]), axis=0)
    xls_dict[key] = xls_dict[key][3:]


xls_dict['2015'] = pd.read_excel(zip_dict['2015'].extract('Sales_Ult_Cust_2015.xlsx'), header = None)
xls_dict['2015'][:3] = xls_dict['2015'][:3].fillna('')
xls_dict['2015'].columns = xls_dict['2015'].iloc[0:3].apply(lambda x: '.'.join([y for y in x if y]), axis=0)
xls_dict['2015'] = xls_dict['2015'][3:]    
    
for year in np.arange(1990, 2016):
    key = str(year)
    rowNum = str(xls_dict[key].shape[0])
    colNum = str(xls_dict[key].shape[1])
    print("for the key ", key, " the dataframe has ", rowNum, " rows and ",
          colNum, " columns")



### STEP 3: CONCATENATE THE DATAFRAMES ###    

###Playing around with the columns...I assume '90-98 is the same formatting
#create a list of the column names between '90 to '98, see if it works
ninety_colname_list = []
ninety_UTILNAME_list = []
ninety_UTILCODE_list =[]
ninety_OWNERSUB_list = []

for year in np.arange(1990, 1999):
    ninety_colname_list = ninety_colname_list + list(xls_dict[str(year)].columns)
    ninety_UTILNAME_list = ninety_UTILNAME_list + list(xls_dict[str(year)]['UTILNAME'])
    ninety_UTILCODE_list = ninety_UTILCODE_list + list(xls_dict[str(year)]['UTILCODE'])
    ninety_OWNERSUB_list = ninety_OWNERSUB_list + list(xls_dict[str(year)]['OWNERSUB'])


##UTILNAME
##UTILCODE
    ##For whatever reasons, there's more unique UTILCODES than UTILNAMES
##(type): FEDERAL, STATE, MUNI, PRIVATE, COOP
##(control area): ASCC, ECAR, ERCOT, MAIN, MAAC, MAPP, NPCC, SERC, SPP, WSCC, 
                  ## HI, PR_TERR
##REV1_#, REV2_# (# = 1-6)
    ###What are the differences, what do the numbers refer to?
##MWH1_#, MWH2_# (# = 1-6)
    ##again, two sets of 6, what are the differences? 
##CONSUM_1, CONSUM_2 (# = 1-6)
    ##still not sure what consumption refers to, but good to see
#STATE, obviously important
#GENERATION...

###Idea: loop through a T/F dataframe, determine if given values are contained
        ##in each year

ninety_colname_list = list(set(ninety_colname_list))
ninety_colname_list.sort()
len(list(set(ninety_UTILNAME_list)))
len(list(set(ninety_UTILCODE_list)))
len(list(set(ninety_OWNERSUB_list)))
xls_dict['1990']['UTILNAME']['Adams Electrical Coop']
pd.DataFrame.lookup(xls_dict['1990'],'Adams Electrical Coop', 'UTILNAME')



for year in np.arange(1990, 1999):
    col_list = list(xls_dict[str(year)].columns)
    print("For year ", str(year), ":")
    print("FEDERAL = ", 'FEDERAL' in col_list)
    print( "STATE = ", "STATE" in col_list)
    print("MUNI = ", "MUNI" in col_list)
    print("COOP = ", "COOP" in col_list)
    print("PRIVATE = ", "PRIVATE" in col_list)
###Note, these are all true! Same names for UTIL Type 1990 - 1998       


for year in np.arange(1990, 1999):
    col_list = list(xls_dict[str(year)].columns)
    print("For year ", str(year), ":")
    print("ASCC = ", 'ASCC' in col_list)
    print( "ECAR = ", "ECAR" in col_list)
    print("ERCOT = ", "ERCOT" in col_list)
    print("MAIN = ", "MAIN" in col_list)
    print("MAPP = ", "MAPP" in col_list)
    print("SERC = ", "SERC" in col_list)
    print("SPP = ", "SPP" in col_list)
    print("WSCC = ", "WSCC" in col_list)
##same for these values, al are in the nineties spreadsheet

##little thought experiment: what's the total number of each type of generator?
    #some embarassing boolean bullshit, but I'm on a United Flight and just paid
    #$30 for no internet, so I can't look up cleaner methods (!?)
##assuming exclusivity of utiltypes...




                            
util_type = pd.DataFrame(columns = ['util_type', '1990', '1991', '1992', '1993',
                                    '1994', '1995', '1996', '1997', '1998'])
util_type['util_type'] = ['FEDERAL', 'STATE', 'MUNI', 'COOP', 'PRIVATE']

for year in np.arange(1990, 1999):
    subset_df = xls_dict[str(year)]
    for type in util_type['util_type']:
        util_type[str(year)][util_type['util_type'][util_type['util_type'] == 
        type].index[0]] = sum(subset_df[type] == 'X')
                  
###I need taht index value!    
print((util_type['util_type'] == 'COOP').index)
df.m`

###figure out formatting of 'merger' files, extract them
###merge utility sales databases into a long file, matching all columns
###filter into a wide file, include % of new load/revenue from merger (previous year proportions)
