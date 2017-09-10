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

zip_dict['2016'] = zipfile.ZipFile(io.BytesIO(requests.get(base_dls + '2016' + 'er.zip').content))
##this is the early release 2016, presumably to be updated w/out 'er' tag

###STEP 2: EXTRACT THE XLS/XLSX FILES FROM THE ZIP FILES ###

###file name conventions
# 90-98: F861TYP1.xls
# 99-00: FILE2.xls
# 01-05: YEAR/file2.xls
# 06: file2.xls
# 07-09: YEAR/file2_YEAR.xls
# 10-11: file2_YEAR.xls
# 12: retail_sales_2012.xls
# 13-15: Sales_Ult_Cust_YEAR.xls
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
# then set them as headers
# df = pd.DataFrame(df[1:], columns = [df[0]] )
for i in np.arange(8, 10):
    key = '0' + str(i)
    file_name = '20' + key + '/file2_20' + key + '.xls'
    xls_dict['20' + key] = pd.read_excel(zip_dict[key].extract(file_name), header=None)
    xls_dict['20' + key][:3] = xls_dict['20' + key][:3].fillna('')
    xls_dict['20' + key].columns = xls_dict['20' + key].iloc[0:3].apply(lambda x: '.'.join([y for y in x if y]), axis=0)
    xls_dict['20' + key] = xls_dict['20' + key][3:]

for i in np.arange(10, 12):
    key = str(i)
    file_name = 'file2_20' + key + '.xls'
    xls_dict['20' + key] = pd.read_excel(zip_dict[key].extract(file_name), header=None)
    xls_dict['20' + key][:3] = xls_dict['20' + key][:3].fillna('')
    xls_dict['20' + key].columns = xls_dict['20' + key].iloc[0:3].apply(lambda x: '.'.join([y for y in x if y]), axis=0)
    xls_dict['20' + key] = xls_dict['20' + key][3:]

xls_dict['2012'] = pd.read_excel(zip_dict['2012'].extract('retail_sales_2012.xls'), header=None)
xls_dict['2012'][:3] = xls_dict['2012'][:3].fillna('')
xls_dict['2012'].columns = xls_dict['2012'].iloc[0:3].apply(lambda x: '.'.join([y for y in x if y]), axis=0)
xls_dict['2012'] = xls_dict['2012'][3:]

for i in np.arange(2013, 2015):
    key = str(i)
    file_name = 'Sales_Ult_Cust_' + key + '.xls'
    xls_dict[key] = pd.read_excel(zip_dict[key].extract(file_name), header=None)
    xls_dict[key][:3] = xls_dict[key][:3].fillna('')
    xls_dict[key].columns = xls_dict[key].iloc[0:3].apply(lambda x: '.'.join([y for y in x if y]), axis=0)
    xls_dict[key] = xls_dict[key][3:]

xls_dict['2015'] = pd.read_excel(zip_dict['2015'].extract('Sales_Ult_Cust_2015.xlsx'), header=None)
xls_dict['2015'][:3] = xls_dict['2015'][:3].fillna('')
xls_dict['2015'].columns = xls_dict['2015'].iloc[0:3].apply(lambda x: '.'.join([y for y in x if y]), axis=0)
xls_dict['2015'] = xls_dict['2015'][3:]

xls_dict['2016'] = pd.read_excel(zip_dict['2016'].extract('Sales_Ult_Cust_2016_Data_Early_Release.xlsx'), header=None)
xls_dict['2016'] = xls_dict['2016'][1:]
xls_dict['2016'][:3] = xls_dict['2016'][:3].fillna('')
xls_dict['2016'].columns = xls_dict['2016'].iloc[0:3].apply(lambda x: '.'.join([y for y in x if y]), axis=0)
xls_dict['2016'] = xls_dict['2016'][3:]
xls_dict['2016'].rename(columns={'': ''})

for i in np.arange(1990, 2017):
    key = str(i)
    print(key)
    print(xls_dict[key].columns)

for year in np.arange(1990, 2017):
    key = str(year)
    rowNum = str(xls_dict[key].shape[0])
    colNum = str(xls_dict[key].shape[1])
    print("for the key ", key, " the dataframe has ", rowNum, " rows and ",
          colNum, " columns")

### STEP 3: CONCATENATE THE DATAFRAMES ###

##First, format the dataframes with consistent column names

##Year
for year in np.arange(1990, 2017):
    key = str(year)
    xls_dict[key]['year'] = pd.Series(np.repeat(year, len(xls_dict[key])))
##State
##1990-2000: STATE
for year in np.arange(1990, 2001):
    key = str(year)
    xls_dict[key] = xls_dict[key].rename(columns={'STATE': 'state'})
    ##2001-2006:State
for year in np.arange(2001, 2007):
    key = str(year)
    xls_dict[key] = xls_dict[key].rename(columns={'State': 'state'})
    ##2007:STATE_CODE
xls_dict['2007'] = xls_dict['2007'].rename(columns={'State': 'state'})
##2008- 2016: State
for year in np.arange(2008, 2017):
    key = str(year)
    xls_dict[key] = xls[key].rename(columns={'State': 'state'})
    # check to make sure abbreviations, etc, are the same
##BA Code/ISO/RTO
##1990-1998: ASCC, ECAR, ERCOT, MAIN, MAAC, MAPP, NPCC, SERC, SPP, WSCC,
## HI, PR_TERR
##1999-2003 : NA (could just match to other years...)

##It's in wide for for the 90s, late 90s maybe gone?
##(control area): ASCC, ECAR, ERCOT, MAIN, MAAC, MAPP, NPCC, SERC, SPP, WSCC,
## HI, PR_TERR
##Utility Name
##1990-2000 : UTILNAME
for year in np.arange(1990, 2001):
    key = str(year)
    xls_dict[key] = xls_dict[key].rename(columns={'UTILNAME': 'utility_name'})
    ##2001-2007 : UTILITY_NAME
for year in np.arange(2001, 2008):
    key = str(year)
    xls_dict[key] = xls_dict[key].rename(columns={'UTILITY_NAME': 'utility_name'})
    ##2008-2016 : Utility Name
for year in np.arange(2001, 2008):
    key = str(year)
    xls_dict[key] = xls_dict[key].rename(columns={'Utility Name': 'utility_name'})
##Utility Code
##1990- 2000: UTILCODE
for year in np.arange(1990, 2001):
    key = str(year)
    xls_dict[key] = xls_dict[key].rename(columns={'UTILCODE': 'utility_id'})
    ##2001- 2006: UTILITY_ID
for year in np.arange(2001, 2007):
    key = str(year)
    xls_dict[key] = xls_dict[key].rename(columns={'UTILITY_ID': 'utility_id'})
    ##2007: utility_id
    ##2008-2016 : Utility Number
for year in np.arange(2008, 2017):
    key = str(year)
    xls_dict[key] = xls_dict[key].rename(columns={'Utility Number': 'utility_id'})
    ##pivot table, codes by name (more codes than name)
##Owner Type (wide form early on)
##1990-1998: individual columns
# FEDERAL, STATE, MUNI, PRIVATE, COOP
##1999-2007: None
##2007- 2016: "Ownership"
# 2008: nan, 'Municipal', 'Investor Owned', 'Unregulated',
# 'Retail Power Marketer', 'State', 'Cooperative', 'Federal',
# 'Wholesale Power Marketer', 'Political Subdivision'
# 2009-2012: nan, 'Municipal', 'Investor Owned', 'Unregulated',
# 'Retail Power Marketer', 'State', 'Cooperative',
# 'Federal', 'Political Subdivision'
# 2013-2015: nan, 'Municipal', 'Investor Owned', 'Behind the Meter',
# 'Retail Power Marketer', 'State', 'Cooperative',
# 'Federal', 'Political Subdivision'
# 2016:{nan, 'Municipal', 'Investor Owned',
# 'Community Choice Aggregator', 'Behind the Meter',
# 'Retail Power Marketer', 'State', 'Cooperative', 'Federal',
# 'Political Subdivision'
##Revenue (Res, Com, Ind, Tran/Hwy, Other, Total)
##1990-1998
for year in np.arange(1990, 1999):
    key = str(year)
    xls_dict[key] = xls_dict[key].rename(columns={'REV1_1': 'revenue_residential',
                                                  'REV1_2': 'revenue_commercial',
                                                  'REV1_3': 'revenue_industrial',
                                                  'REV1_4': 'revenue_transportation',
                                                  'REV1_5': 'revenue_other',
                                                  'REV1_6': 'revenue_total'})
    ##Residential: REV1_1
    ##Commercial: REV1_2
    ##Industrial: REV1_3
    ##Transportation: REV1_4
    ##Other: REV1_5
    ##Total: REV1_6
    ##1999-2000
for year in np.arange(1999, 2001):
    key = str(year)
    xls_dict[key] = xls_dict[key].rename(columns={'RESREV': 'revenue_residential',
                                                  'COMREV': 'revenue_commercial',
                                                  'INDREV': 'revenue_industrial',
                                                  'HWYREV': 'revenue_transportation',
                                                  'OTHREV': 'revenue_other',
                                                  'TOTREV': 'revenue_total'})
    ##Residential: RESREV
    ##Commercial: COMREV
    ##Industrial: INDREV
    ##Transportation: HWYREV
    ##Other: OTHREV
    ##Total: TOTREV
    ##2001-2006
for year in np.arange(2001, 2007):
    key = str(year)
    xls_dict[key] = xls_dict[key].rename(columns={'Res Revenue (000)': 'revenue_residential',
                                                  'Com Revenue (000)': 'revenue_commercial',
                                                  'Ind Revenue (000)': 'revenue_industrial',
                                                  'Trans Revenue (000)': 'revenue_transportation',
                                                  'Total Revenue (000)': 'revenue_total'})

    ##Residential: Res Revenue (000)
    ##Commercial: Com Revenue (000)
    ##Industrial: Ind Revenue (000)
    ##Transportation: Trans Rev (000)
    ##Total: Total Revenue (000)
    ##2007
xls_dict[2007] = xls_dict[2007].rename(columns={'RESIDENTIAL_REVENUES': 'revenue_residential',
                                                'COMMERCIAL_REVENUES': 'revenue_commercial',
                                                'INDUSTRIAL_REVENUES': 'revenue_industrial',
                                                'TRANSPORTATION_REVENUES': 'revenue_transportation',
                                                'TOTAL_REVENUES': 'revenue_total'})
##Residential: RESIDENTIAL_REVENUES
##Commercial: COMMERCIAL_REVENUES
##Industrial: INDUSTRIAL_REVENUES
##Transportation: TRANSPORTATION_REVENUES
##Total: TOTAL_REVENUES
##2008-2012
##Residential: [8]
##Commercial: [11]
##Industrial: [14]
##Transportation: [17]
##Total: [20]
##2013-2015
##Residential: [9]
##Commercial: [12]
##Industrial: [15]
##Transportation: [18]
##Total: [21]
##2016
##Residential: [10]
##Commercial: [13]
##Industrial: [16]
##Transportation: [19]
##Total: [22]
##Volume of Sales
##1990-1998
##Residential: MWH1_1
##Commercial: MWH1_2
##Industrial: MWH1_3
##Transportation: MWH1_4
##Other: MWH1_5
##Total: MWH1_6
##1999-2000
##Residential: RESSALES
##Commercial: COMSALES
##Industrial: INDSALES
##Transportation: HWYSALES
##Other: OTHSALES
##Total: TOTSALES
##2001-2006
##Residential: Res Sales (MWh)
##Commercial: Com Sales (MWh)
##Industrial: Ind Sales (MWh)
##Transportation: Trans Sales (MWh)
##Total: Total Sales (MWh)
##2007
##Residential: RESIDENTIAL_SALES
##Commercial: COMMERCIAL_SALES
##Industrial: INDUSTRIAL_SALES
##Transportation: TRANSPORTATION_SALES
##Total: TOTAL_SALES
##2008-2012
##Residential: [9]
##Commercial: [12]
##Industrial: [15]
##Transportation: [18]
##Total: [21]
##2013-2015
##Residential: [10]
##Commercial: [13]
##Industrial: [16]
##Transportation: [19]
##Total: [22]
##2016
##Residential: [11]
##Commercial: [14]
##Industrial: [17]
##Transportation: [20]
##Total: [23]
##Number of Customers
##1990-1998
##Residential: CONSUM1_1
##Commercial: CONSUM1_2
##Industrial: CONSUM1_3
##Transportation: CONSUM1_4
##Other: CONSUM1_5
##Total: CONSUM1_6
##1999-2000
##Residential: RESCONS_
##Commercial: COMCONS
##Industrial: INDCONS
##Transportation: HWYCONS
##Other: OTHCONS
##Total: TOTCONS
##2001-2006
##Residential: Res Consumers (n)
##Commercial: Com Consumers (n)
##Industrial: Ind Consumers (n)
##Transportation: Trans Consumers (n)
##Total: Total Consumers (n)
##2007
##Residential: RESIDENTIAL_CONSUMERS
##Commercial: COMMERCIAL_CONSUMERS
##Industrial: INDUSTRIAL_CONSUMERS
##Transportation: TRANSPORTATION_CONSUMERS
##Total: TOTAL_CONSUMERS
##2008-2012
##Residential: [10]
##Commercial: [13]
##Industrial: [16]
##Transportation: [19]
##Total: [22]
##2013-2015
##Residential: [11]
##Commercial: [14]
##Industrial: [17]
##Transportation: [20]
##Total: [23]
##2016
##Residential: [12]
##Commercial: [15]
##Industrial: [18]
##Transportation: [21]
##Total: [24]
##Columns of Dataframe
# Year (int)
# State (factor)
# BA Code (factor)
# Utility Name(string)
# Utility Code (int or float)
# Owner Type (factor)
# Revenue(float)
#
# Volume of Sales (float)
#
# Number of Customers (float)



for year in np.arange(1990, 1999):
    col_list = list(xls_dict[str(year)].columns)
    print("For year ", str(year), ":")
    print("FEDERAL = ", 'FEDERAL' in col_list)
    print("STATE = ", "STATE" in col_list)
    print("MUNI = ", "MUNI" in col_list)
    print("COOP = ", "COOP" in col_list)
    print("PRIVATE = ", "PRIVATE" in col_list)
###Note, these are all true! Same names for UTIL Type 1990 - 1998


for year in np.arange(1990, 1999):
    col_list = list(xls_dict[str(year)].columns)
    print("For year ", str(year), ":")
    print("ASCC = ", 'ASCC' in col_list)
    print("ECAR = ", "ECAR" in col_list)
    print("ERCOT = ", "ERCOT" in col_list)
    print("MAIN = ", "MAIN" in col_list)
    print("MAPP = ", "MAPP" in col_list)
    print("SERC = ", "SERC" in col_list)
    print("SPP = ", "SPP" in col_list)
    print("WSCC = ", "WSCC" in col_list)
##same for these values, al are in the nineties spreadsheet

##little thought experiment: what's the total number of each type of generator?
# some embarassing boolean bullshit, but I'm on a United Flight and just paid
# $30 for no internet, so I can't look up cleaner methods (!?)
##assuming exclusivity of utiltypes...

ninetyKey = pd.read_excel(zip_dict['90'].extract('DataElementsDef90-00.xls'))

var_dict = {}

for name in xls_dict['1990'].columns:
    index = np.where(ninetyKey["Form EIA-861 Data Elements Definitions - 1990-2000"] == name)[0]
    if np.isreal(index):
        print(index)
        var_dict[name] = ninetyKey['Unnamed: 1'][index]

var_dict['STCODE2_1']

xls_dict['1990']['MWH2_6'].sum() / xls_dict['1990']['MWH1_6'].sum()

util_type = pd.DataFrame(columns=['util_type', '1990', '1991', '1992', '1993',
                                  '1994', '1995', '1996', '1997', '1998'])
util_type['util_type'] = ['FEDERAL', 'STATE', 'MUNI', 'COOP', 'PRIVATE']

for year in np.arange(1990, 1999):
    subset_df = xls_dict[str(year)]
    for type in util_type['util_type']:
        util_type[str(year)][util_type['util_type'][util_type['util_type'] ==
                                                    type].index[0]] = sum(subset_df[type] == 'X')

###I need taht index value!
print((util_type['util_type'] == 'COOP').index)

###edited to see if I can commit from bash after editing in Spyder

###figure out formatting of 'merger' files, extract them
###merge utility sales databases into a long file, matching all columns
###filter into a wide file, include % of new load/revenue from merger (previous year proportions)
