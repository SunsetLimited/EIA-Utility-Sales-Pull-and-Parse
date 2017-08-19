# EIA-Utility-Sales-Pull-and-Parse

The purpose of this script is to extract the annual records of load-seving-entitysales to customers by end use and append them into a single dataframe. This data is not well formatted (and apparently not included in the eia.gov API). Mostly we're dealing with inconsistently fomratted zip files containing all the relevant xls/xlsx files, also not consistently formatted though in the case of the xlsx files it's perhaps understandable, regulatory changes have been considerable over 25 years

I recommend creating a new directory to store the files
    #mkdir eiaUltSales
    #cd eiaUltSales
