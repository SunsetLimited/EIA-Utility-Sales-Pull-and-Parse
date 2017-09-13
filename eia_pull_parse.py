# -*- coding: utf-8 -*-
##Wiley is the best
import requests, zipfile, io
import numpy as np
import pandas as pd
from pandas import DataFrame as df


def main():
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
        xls_dict['20' + key].columns = xls_dict['20' + key].iloc[0:3].apply(lambda x: '.'.join([y for y in x if y]),
                                                                            axis=0)
        xls_dict['20' + key] = xls_dict['20' + key][3:]

    for i in np.arange(10, 12):
        key = str(i)
        file_name = 'file2_20' + key + '.xls'
        xls_dict['20' + key] = pd.read_excel(zip_dict[key].extract(file_name), header=None)
        xls_dict['20' + key][:3] = xls_dict['20' + key][:3].fillna('')
        xls_dict['20' + key].columns = xls_dict['20' + key].iloc[0:3].apply(lambda x: '.'.join([y for y in x if y]),
                                                                            axis=0)
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

    xls_dict['2016'] = pd.read_excel(zip_dict['2016'].extract('Sales_Ult_Cust_2016_Data_Early_Release.xlsx'),
                                     header=None)
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
    ##1990-1998: NA
    ##1999-2000 : STATE

    for year in np.arange(1990,1999):
        key = str(year)
        xls_dict[key]['state'] = pd.Series(np.repeat(pd.np.nan, len(xls_dict[key])))

    for year in np.arange(1999, 2001):
        key = str(year)
        xls_dict[key] = xls_dict[key].rename(columns={'STATE': 'state'})
        ##2001-2006:State

    for year in np.arange(2001, 2007):
        key = str(year)
        xls_dict[key] = xls_dict[key].rename(columns={'State': 'state'})
        ##2007:STATE_CODE

    xls_dict['2007'] = xls_dict['2007'].rename(columns={'STATE_CODE': 'state'})

    #2008- 2016: State
    for year in np.arange(2008, 2017):
        key = str(year)
        xls_dict[key] = xls_dict[key].rename(columns={'State': 'state'})



    ##BA Code/ISO/RTO
    for year in np.arange(1990, 1999):
        key = str(year)
        ba_code = pd.Series(xls_dict[key]['ASCC']).str.replace('X', 'ASCC')
        for i in np.arange(0, len(ba_code)):
            if pd.notnull(xls_dict[key]['ECAR'][i]):
                ba_code[i] = 'ECAR'
            elif pd.notnull(xls_dict[key]['ERCOT'][i]):
                ba_code[i] = 'ERCOT'
            elif pd.notnull(xls_dict[key]['MAIN'][i]):
                ba_code[i] = 'MAIN'
            elif pd.notnull(xls_dict[key]['MAAC'][i]):
                ba_code[i] = 'MAAC'
            elif pd.notnull(xls_dict[key]['MAPP'][i]):
                ba_code[i] = 'MAPP'
            elif pd.notnull(xls_dict[key]['NPCC'][i]):
                ba_code[i] = 'NPCC'
            elif pd.notnull(xls_dict[key]['SERC'][i]):
                ba_code[i] = 'SERC'
            elif pd.notnull(xls_dict[key]['SPP'][i]):
                ba_code[i] = 'SPP'
            elif pd.notnull(xls_dict[key]['WSCC'][i]):
                ba_code[i] = 'WSCC'
            elif pd.notnull(xls_dict[key]['HI'][i]):
                ba_code[i] = 'HI'
            elif pd.notnull(xls_dict[key]['PR_TERR'][i]):
                ba_code[i] = 'PR_TERR'
        xls_dict[key]['ba_code'] = ba_code

    ##1990-1998: ASCC, ECAR, ERCOT, MAIN, MAAC, MAPP, NPCC, SERC, SPP, WSCC,
    ## HI, PR_TERR
    ##1999-2012 : NA (could just match to other years...)
    for year in np.arange(1999, 2013):
        key = str(year)
        xls_dict[key]['ba_code'] = pd.Series(np.repeat(pd.np.nan, len(xls_dict[key])))
    ##2013-2016: BA_CODE
    ##It's in wide for for the 90s, late 90s maybe gone?
    ##(control area): ASCC, ECAR, ERCOT, MAIN, MAAC, MAPP, NPCC, SERC, SPP, WSCC,
    ## HI, PR_TERR
    for year in np.arange(2013, 2017):
        key = str(year)
        xls_dict[key] = xls_dict[key].rename(columns = {'BA_CODE':'ba_code'})

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
        ##still even missing some data here
    # FEDERAL, STATE, MUNI, PRIVATE, COOP

    for year in np.arange(1990, 1999):
        key= str(year)
        ownership = pd.Series(xls_dict[key]['COOP']).str.replace('X', 'COOP')
        for i in np.arange(0, len(ownership)):
            if pd.notnull(xls_dict[key]['FEDERAL'][i]):
                ownership[i] = 'FEDERAL'
            elif pd.notnull(xls_dict[key]['STATE'][i]):
                ownership[i] = 'STATE'
            elif pd.notnull(xls_dict[key]['MUNI'][i]):
                ownership[i] = 'MUNI'
            elif pd.notnull(xls_dict[key]['PRIVATE'][i]):
                ownership[i] = 'PRIVATE'
        xls_dict[key]['ownership'] = ownership

    ##1999-2007: None

    for year in np.arange(1999, 2007):
        key = str(year)
        xls_dict[key]['ownership'] = pd.Series(np.repeat(pd.np.nan, len(xls_dict[key])))

    ##2007: "OWNERSHIP"
    xls_dict['2007'] = xls_dict['2007'].rename(columns={'OWNERSHIP':'ownership'})
    xls_dict['2007']['ownership']=xls_dict['2007']['ownership'].replace({'Federal':'FEDERAL',
                                                                        'Municipal':'MUNI',
                                                                        'State':'STATE',
                                                                        'Cooperative':'COOP',
                                                                        'Investor Owned':'IOU',
                                                                        'Unregulated':'UNREGULATED',
                                                                        'Retail Power Marketer':'RETAILER',
                                                                        'Political Subdivision':'POLITICAL SUB'})


    ##2008- 2016: "Ownership"
    for year in np.arange(2008, 2017):
        key=str(year)
        xls_dict[key] = xls_dict[key].rename(columns={'Ownership':'ownership'})

    for year in np.arange(2008, 2017):
        key=str(year)
        xls_dict[key]['ownership'] = xls_dict[key]['ownership'].replace({'Federal':'FEDERAL',
                                                            'Municipal':'MUNI',
                                                            'Retail Power Marketer':'RETAILER',
                                                            'Behind the Meter':'BEHIND METER',
                                                            'Investor Owned':'IOU',
                                                            'Political Subdivision':'POLITICAL SUB',
                                                            'Community Choice Aggregator':'CCA',
                                                            'State':'STATE',
                                                            'Cooperative':'COOP',
                                                            'Unregulated': 'UNREGULATED',
                                                            'Wholesale Power Marketer':'WHOLESALER'})
    owner_list = []
    for year in np.arange(2008, 2017):
        key=str(year)
        owner_list = owner_list + list(xls_dict[key]['ownership'])

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
                                                      'Trans Rev (000)': 'revenue_transportation',
                                                      'Total Revenue (000)': 'revenue_total'})

        ##Residential: Res Revenue (000)
        ##Commercial: Com Revenue (000)
        ##Industrial: Ind Revenue (000)
        ##Transportation: Trans Rev (000)
        ##Total: Total Revenue (000)
        ##2007
    xls_dict['2007'] = xls_dict['2007'].rename(columns={'RESIDENTIAL_REVENUES': 'revenue_residential',
                                                        'COMMERCIAL_REVENUES': 'revenue_commercial',
                                                        'INDUSTRIAL_REVENUES': 'revenue_industrial',
                                                        'TRANSPORTATION_REVENUES': 'revenue_transportation',
                                                        'TOTAL_REVENUES': 'revenue_total'})
    ##Residential: RESIDENTIAL_REVENUES
    ##Commercial: COMMERCIAL_REVENUES
    ##Industrial: INDUSTRIAL_REVENUES
    ##Transportation: TRANSPORTATION_REVENUES
    ##Total: TOTAL_REVENUES

    ##2007
    for year in np.arange(2008, 2013):
        key = str(year)
        xls_dict[key] = xls_dict[key].rename(columns={xls_dict[key].columns[8]:'revenue_residential',
                                                      xls_dict[key].columns[11]:'revenue_commercial',
                                                      xls_dict[key].columns[14]:'revenue_industrial',
                                                      xls_dict[key].columns[17]:'revenue_transportation',
                                                      xls_dict[key].columns[20]:'revenue_total'})

    ##Residential: [8]
    ##Commercial: [11]
    ##Industrial: [14]
    ##Transportation: [17]
    ##Total: [20]

    ##2013-2015
    for year in np.arange(2013, 2016):
        key = str(year)
        xls_dict[key] = xls_dict[key].rename(columns={xls_dict[key].columns[9]:'revenue_residential',
                                                      xls_dict[key].columns[12]: 'revenue_commercial',
                                                      xls_dict[key].columns[15]: 'revenue_industrial',
                                                      xls_dict[key].columns[18]: 'revenue_transportation',
                                                      xls_dict[key].columns[21]: 'revenue_total'
                                                      })
    ##Residential: [9]
    ##Commercial: [12]
    ##Industrial: [15]
    ##Transportation: [18]
    ##Total: [21]

    ##2016

    xls_dict['2016'] = xls_dict['2016'].rename(columns={xls_dict['2016'].columns[10]:'revenue_residential',
                                                        xls_dict['2016'].columns[13]: 'revenue_commercial',
                                                        xls_dict['2016'].columns[16]: 'revenue_industrial',
                                                        xls_dict['2016'].columns[19]: 'revenue_transportation',
                                                        xls_dict['2016'].columns[22]: 'revenue_total'})
    ##Residential: [10]
    ##Commercial: [13]
    ##Industrial: [16]
    ##Transportation: [19]
    ##Total: [22]

    ##Volume of Sales
    ##1990-1998
    for year in np.arange(1990, 1999):
        key = str(year)
        xls_dict[key] = xls_dict[key].rename(columns={'MWH1_1':'sales_residential',
                                                      'MWH1_2':'sales_commercial',
                                                      'MWH1_3':'sales_industrial',
                                                      'MWH1_4':'sales_transportation',
                                                      'MWH1_5':'sales_other',
                                                      'MWH1_6':'sales_total'})
    ##Residential: MWH1_1
    ##Commercial: MWH1_2
    ##Industrial: MWH1_3
    ##Transportation: MWH1_4
    ##Other: MWH1_5
    ##Total: MWH1_6

    ##1999-2000
    for year in np.arange(1999, 2001):
        key = str(year)
        xls_dict[key] = xls_dict[key].rename(columns={'RESSALES':'sales_residential',
                                                      'COMSALES':'sales_commercial',
                                                      'INDSALES':'sales_industrial',
                                                      'HWYSALES':'sales_transportation',
                                                      'OTHSALES':'sales_other',
                                                      'TOTSALES':'sales_total'})
    ##Residential: RESSALES
    ##Commercial: COMSALES
    ##Industrial: INDSALES
    ##Transportation: HWYSALES
    ##Other: OTHSALES
    ##Total: TOTSALES

    ##2001-2006

    for year in np.arange(2001, 2007):
        key = str(year)
        xls_dict[key] = xls_dict[key].rename(columns={'Res Sales (MWh)':'sales_residential',
                                                      'Com Sales (MWh)':'sales_commercial',
                                                      'Ind Sales (MWh)':'sales_industrial',
                                                      'Trans Sales (MWh)':'sales_transportation',
                                                      'Total Sales (MWh)':'sales_total'})
    ##Residential: Res Sales (MWh)
    ##Commercial: Com Sales (MWh)
    ##Industrial: Ind Sales (MWh)
    ##Transportation: Trans Sales (MWh)
    ##Total: Total Sales (MWh)

    ##2007

    xls_dict['2007'] = xls_dict['2007'].rename(columns={'RESIDENTIAL_SALES':'sales_residential',
                                                        'COMMERCIAL_SALES':'sales_commercial',
                                                        'INDUSTRIAL_SALES':'sales_industrial',
                                                        'TRANSPORTATION_SALES':'sales_transportation',
                                                        'TOTAL_SALES':'sales_total'})

    ##Residential: RESIDENTIAL_SALES
    ##Commercial: COMMERCIAL_SALES
    ##Industrial: INDUSTRIAL_SALES
    ##Transportation: TRANSPORTATION_SALES
    ##Total: TOTAL_SALES

    ##2008-2012

    for year in np.arange(2008, 2013):
        key=str(year)
        xls_dict[key]=xls_dict[key].rename(columns={xls_dict[key].columns[9]:'sales_residential',
                                                    xls_dict[key].columns[12]:'sales_commercial',
                                                    xls_dict[key].columns[15]:'sales_industrial',
                                                    xls_dict[key].columns[18]:'sales_transportation',
                                                    xls_dict[key].columns[21]:'sales_total'})
    ##Residential: [9]
    ##Commercial: [12]
    ##Industrial: [15]
    ##Transportation: [18]
    ##Total: [21]

    ##2013-2015

    for year in np.arange(2013, 2016):
        key=str(year)
        xls_dict[key]=xls_dict[key].rename(columns={xls_dict[key].columns[10]:'sales_residential',
                                                    xls_dict[key].columns[13]:'sales_commercial',
                                                    xls_dict[key].columns[16]:'sales_industrial',
                                                    xls_dict[key].columns[19]:'sales_transportation',
                                                    xls_dict[key].columns[22]:'sales_total'})
    ##Residential: [10]
    ##Commercial: [13]
    ##Industrial: [16]
    ##Transportation: [19]
    ##Total: [22]

    ##2016
    xls_dict['2016'] = xls_dict['2016'].rename(columns={xls_dict['2016'].columns[11]: 'sales_residential',
                                                        xls_dict['2016'].columns[14]: 'sales_commercial',
                                                        xls_dict['2016'].columns[17]: 'sales_industrial',
                                                        xls_dict['2016'].columns[20]: 'sales_transportation',
                                                        xls_dict['2016'].columns[23]: 'sales_total'})
    ##Residential: [11]
    ##Commercial: [14]
    ##Industrial: [17]
    ##Transportation: [20]
    ##Total: [23]

    ##Number of Customers
    ##1990-1998
    for year in np.arange(1990, 1999):
        key = str(year)
        xls_dict[key] = xls_dict[key].rename(columns={'CONSUM1_1':'customers_residential',
                                                      'CONSUM1_2':'customers_commercial',
                                                      'CONSUM1_3':'customers_industrial',
                                                      'CONSUM1_4':'customers_transportation',
                                                      'CONSUM1_5':'customers_other',
                                                      'CONSUM1_6':'customers_total'})
    ##Residential: CONSUM1_1
    ##Commercial: CONSUM1_2
    ##Industrial: CONSUM1_3
    ##Transportation: CONSUM1_4
    ##Other: CONSUM1_5
    ##Total: CONSUM1_6

    ##1999-2000
    for year in np.arange(1999, 2001):
        key = str(year)
        xls_dict[key] = xls_dict[key].rename(columns={'RESCONS_':'customers_residential',
                                                      'COMCONS':'customers_commercial',
                                                      'INDCONS':'customers_industrial',
                                                      'HWYCONS':'customers_transportation',
                                                      'OTHCONS':'customers_other',
                                                      'TOTCONS':'customers_total'})
    ##Residential: RESCONS_
    ##Commercial: COMCONS
    ##Industrial: INDCONS
    ##Transportation: HWYCONS
    ##Other: OTHCONS
    ##Total: TOTCONS

    ##2001-2006
    for year in np.arange(2001, 2007):
        key = str(year)
        xls_dict[key] = xls_dict[key].rename(columns={'Res Consumers (n)':'customers_residential',
                                                      'Com Consumers (n)':'customers_commercial',
                                                      'Ind Consumers (n)':'customers_industrial',
                                                      'Trans Consumers (n)':'customers_transportation',
                                                      'Total Consumers (n)':'customers_total'})
    ##Residential: Res Consumers (n)
    ##Commercial: Com Consumers (n)
    ##Industrial: Ind Consumers (n)
    ##Transportation: Trans Consumers (n)
    ##Total: Total Consumers (n)

    ##2007
    xls_dict['2007'] = xls_dict['2007'].rename(columns={'RESIDENTIAL_CONSUMERS':'customers_residential',
                                                        'COMMERCIAL_CONSUMERS':'customers_commercial',
                                                        'INDUSTRIAL_CONSUMERS':'customers_industrial',
                                                        'TRANSPORTATION_CONSUMERS':'customers_transportation',
                                                        'TOTAL_CONSUMERS':'customers_total'})

    ##Residential: RESIDENTIAL_CONSUMERS
    ##Commercial: COMMERCIAL_CONSUMERS
    ##Industrial: INDUSTRIAL_CONSUMERS
    ##Transportation: TRANSPORTATION_CONSUMERS
    ##Total: TOTAL_CONSUMERS

    ##2008-2012
    for year in np.arange(2008, 2013):
        key = str(year)
        xls_dict[key] = xls_dict[key].rename(columns={'xls_dict[key].columns[10]':'customers_residential',
                                                      xls_dict[key].columns[13]:'customers_commercial',
                                                      xls_dict[key].columns[16]:'customers_industrial',
                                                      xls_dict[key].columns[19]:'customers_transportation',
                                                      xls_dict[key].columns[22]:'customers_total'})

    ##Residential: [10]
    ##Commercial: [13]
    ##Industrial: [16]
    ##Transportation: [19]
    ##Total: [22]

    ##2013-2015
    for year in np.arange(2013, 2015):
        key = str(year)
        xls_dict[key] = xls_dict[key].rename(columns={xls_dict[key].columns[11]: 'customers_residential',
                                                      xls_dict[key].columns[14]: 'customers_commercial',
                                                      xls_dict[key].columns[17]: 'customers_industrial',
                                                      xls_dict[key].columns[20]: 'customers_transportation',
                                                      xls_dict[key].columns[23]: 'customers_total'})

    ##Residential: [11]
    ##Commercial: [14]
    ##Industrial: [17]
    ##Transportation: [20]
    ##Total: [23]

    ##2016
    xls_dict['2016'] = xls_dict['2016'].rename(columns={xls_dict['2016'].columns[12]: 'customers_residential',
                                                        xls_dict['2016'].columns[15]: 'customers_commercial',
                                                        xls_dict['2016'].columns[18]: 'customers_industrial',
                                                        xls_dict['2016'].columns[21]: 'customers_transportation',
                                                        xls_dict['2016'].columns[24]: 'customers_total'})
    ##Residential: [12]
    ##Commercial: [15]
    ##Industrial: [18]
    ##Transportation: [21]
    ##Total: [24]

    ##make the final dataframe by concatenating all the ones in the dict

    column_list = ['year','utility_id', 'utility_name', 'state', 'ba_code', 'ownership',
                    'revenue_residential', 'revenue_commercial', 'revenue_industrial',
                    'revenue_transportation', 'revenue_other', 'revenue_total',
                    'sales_residential', 'sales_commercial', 'sales_industrial',
                    'sales_transportation', 'sales_other', 'sales_total',
                    'customers_residential', 'customers_commercial',
                    'customers_industrial', 'customers_transportation',
                    'customers_other', 'customers_total']

    eia_ult_sales_hist = pd.DataFrame(columns = column_list)

    for year in np.arange(1990, 2017):
        key = str(year)
        eia_ult_sales_hist = pd.concat([eia_ult_sales_hist, xls_dict[key][column_list]])

set(eia_ult_sales_hist['year'])

if __name__ == "__main__":
    main()

for item in column_list:
    for year in np.arange(1990, 2017):
        print("Is ", 'ownership', " in ", str(year), "? ")
        print('ownership' in xls_dict[str(year)].columns)