# class that gets data from EUROSTAT and retuns it prepared to be inserted on smartdataframe
# calls eurostat api to fetch data
# executes tranformations that simplify dataframe for the LLM
# may store it in csv file to avoid multiple downloads
import time
import pandas
import pandasai
from numpy import random
from urllib.request import urlopen
import os


class GetData:
    #eurostat url
    URL_GROSS_VALUE_ADDED_BY_SECTOR = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/nama_10_a10/A.CP_MEUR.A+B-E+C+F+G-I+J+K+L+M_N+O-Q+R-U.B1G.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=1995&endPeriod=" + time.strftime("%Y")
    URL_AGRICULTURAL_HOLDINGS = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/ef_m_farmleg/A.TOTAL.TOTAL.TOTAL.UAA.HA0+HA_GT0_LT2+HA2-4+HA5-9+HA10-19+HA20-29+HA30-49+HA50-99+HA_GE100.HA+HLD.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=2010&endPeriod=" + time.strftime("%Y")
    URL_WATER_ABSTRACTED_BY_SECTOR_OF_USE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/ten00006/A.MIO_M3.ABS_PWS+ABS_AGR+ABS_IND+ABS_IND_CL+ABS_ELC_CL.FSW.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=2011&endPeriod=" + time.strftime("%Y")

    #c01
    URL_TOTAL_POPULATION = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/demo_gind/A.JAN.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=1995&endPeriod=" + time.strftime("%Y")
    URL_POPULATION_TYPE_REGION = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/urt_gind3/A.JAN.URB+INT+RUR.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=2000&endPeriod=" + time.strftime("%Y")
    URL_POPULATION_DISTRIBUTION_DEGREE_URBANIZATION = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/ilc_lvho01/A.TOTAL.TOTAL.DEG1+DEG2+DEG3.PC.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=2003&endPeriod=" + time.strftime("%Y")
    URL_POPULATION_BY_GENDER = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/demo_pjanbroad/A.NR.TOTAL.M+F.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=1995&endPeriod=" + time.strftime("%Y")
    #c02
    URL_POPULATION_DENSITY = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/demo_r_d3dens/A.PER_KM2.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=1995&endPeriod=" + time.strftime("%Y")
    URL_POPULATION_DENSITY_TYPE_TERRITORY = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/urt_d3dens/A.URB+INT+RUR.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=2018&endPeriod=" + time.strftime("%Y")
    #c03
    URL_POPULATION_BY_AGE_GENDER = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/demo_pjanbroad/A.NR.Y_LT15+Y15-64+Y_GE65.M+F.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=1995&endPeriod=" + time.strftime("%Y")
    URL_POPULATION_BY_AGE_GENDER_TYPE_REGION = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/urt_pjanaggr3/A.M.Y_LT15.URB+INT+RUR.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=1995&endPeriod=" + time.strftime("%Y")
    #c04
    URL_AREA_TYPE_TERRITORY = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/urt_d3area/A.KM2.TOTAL.URB+INT+RUR.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=1995&endPeriod=" + time.strftime("%Y")
    #c06
    URL_EMPLOYMENT_RATE_BY_AGE_GENDER_DEGREE_URBANIZATION = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/lfst_r_ergau/A.PC.DEG1+DEG2+DEG3.Y15-24+Y25-64+Y65-74.M+F.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=1995&endPeriod=" + time.strftime("%Y")
    #c07
    URL_UNEMPLOYMENT_RATE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/tepsr_wc170/A.PC.T.Y15-74.TOTAL.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=2005&endPeriod=" + time.strftime("%Y")
    URL_UNEMPLOYMENT_BY_AGE_GENDER_DEGREE_URNANIZATION = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/lfst_r_urgau/A.PC.DEG1+DEG2+DEG3.Y15-74.T+M+F.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=1995&endPeriod=" + time.strftime("%Y")
    #c08
    URL_EMPLOYMENT_BY_GENDER = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/lfsi_emp_a/A.EMP_LFS.M+F.Y15-64.THS_PER.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=2003&endPeriod=" + time.strftime("%Y")
    URL_EMPLOYMENT_BY_GENDER_ECONOMIC_ACTIVITY = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/lfsa_egan22d/A.THS_PER.Y15-64.T+M+F.TOTAL+A01+A02+A03+B05+B06+B07+B08+B09+C10+C11+C12+C13+C14+C15+C16+C17+C18+C19+C20+C21+C22+C23+C24+C25+C26+C27+C28+C29+C30+C31+C32+C33+D35+E36+E37+E38+E39+F41+F42+F43+G45+G46+G47+H49+H50+H51+H52+H53+I55+I56+J58+J59+J60+J61+J62+J63+K64+K65+K66+L68+M69+M70+M71+M72+M73+M74+M75+N77+N78+N79+N80+N81+N82+O84+P85+Q86+Q87+Q88+R90+R91+R92+R93+S94+S95+S96+T97+T98+U99+NRP+UNK.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=2008&endPeriod=" + time.strftime("%Y")


    URL_AREA_DISTRIBUTION_UNDER_ORGANIC_FARMING = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/sdg_02_40/A.PC_UAA.UAAXK0000.TOTAL.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=2000&endPeriod=" + time.strftime("%Y")
    URL_GREEN_HOUSE_GASES = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/env_air_gge/A.THS_T.GHG.CRF1+CRF2+CRF3+CRF4+CRF5+CRF6.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=1985&endPeriod=" + time.strftime("%Y")
    URL_GDP = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/nama_10_gdp/A.CP_MEUR.P3.BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE?format=SDMX-CSV&lang=en&label=label_only&startPeriod=1975&endPeriod=" + time.strftime("%Y")
    
    #alternative file location to avoid downloading data all the time
    PATH_GROSS_VALUE_ADDED_BY_SECTOR = "estat_nama_10_a10_filtered_en.csv"
    PATH_AGRICULTURAL_HOLDINGS = "estat_ef_m_farmleg_filtered_en.csv"
    PATH_WATER_ABSTRACTED_BY_SECTOR_OF_USE = "estat_ten00006_filtered_en.csv"
    
    
    PATH_GREEN_HOUSE_GASES = "estat_env_air_gge_filtered_en.csv"
    PATH_GDP = "estat_nama_10_gdp_filtered_en.csv"
    PATH_TOTAL_POPULATION = "estat_demo_gind_filtered_en.csv"
    PATH_AREA_DISTRIBUTION_UNDER_ORGANIC_FARMING = "estat_sdg_02_40_filtered_en.csv"
    PATH_POPULATION_TYPE_REGION = "estat_urt_gind3_filtered_en.csv"
    PATH_POPULATION_DISTRIBUTION_DEGREE_URBANIZATION = "estat_ilc_lvho01_filtered_en.csv"
    PATH_POPULATION_BY_GENDER = "estat_demo_pjanbroad_filtered_en.csv"
    PATH_POPULATION_DENSITY = "estat_demo_r_d3dens_filtered_en.csv"
    PATH_POPULATION_DENSITY_TYPE_TERRITORY = "estat_urt_d3dens_filtered_en.csv"
    PATH_POPULATION_BY_AGE_GENDER = "estat_demo_pjanbroad_filtered_en.csv"
    PATH_POPULATION_BY_AGE_GENDER_TYPE_REGION = "estat_urt_pjanaggr3_filtered_en.csv"
    PATH_AREA_TYPE_TERRITORY = "estat_urt_d3area_filtered_en.csv"
    PATH_EMPLOYMENT_RATE_BY_AGE_GENDER_DEGREE_URBANIZATION = "estat_lfst_r_ergau_filtered_en.csv"
    PATH_UNEMPLOYMENT_RATE = "estat_tepsr_wc170_filtered_en.csv"
    PATH_UNEMPLOYMENT_BY_AGE_GENDER_DEGREE_URNANIZATION = "estat_lfst_r_urgau_filtered_en.csv"
    PATH_EMPLOYMENT_BY_GENDER = "estat_lfsi_emp_a_filtered_en.csv"
    PATH_EMPLOYMENT_BY_GENDER_ECONOMIC_ACTIVITY = "estat_lfsa_egan22d_filtered_en.csv"

    @staticmethod
    def WaterAbstractedBySectorOfUse(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_WATER_ABSTRACTED_BY_SECTOR_OF_USE)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_WATER_ABSTRACTED_BY_SECTOR_OF_USE}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "OBS_FLAG", "CONF_STATUS", "unit", "wat_src"], axis=1)
            #drop tuples that have null value
            result = result.dropna()

            result = result.rename(columns={"geo": "Country"})
            
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            
            result = result.rename(columns={"OBS_VALUE": r"Water Cubic meters"})
            result[r"Water Cubic meters"] = pandas.to_numeric(result[r"Water Cubic meters"])
            result["Water Cubic meters"] = result["Water Cubic meters"].apply(lambda x: x * 1000000)


            result = result.rename(columns={"wat_proc": "Sector"})

            #data set description
            description = "Fresh surface water Cubic meters consumption by sector"
            #anonymizing head
            head = result.head()
            randValues = head["Water Cubic meters"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["Water Cubic meters"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})
            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result


    @staticmethod
    def AgriculturalHoldings(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_AGRICULTURAL_HOLDINGS)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_AGRICULTURAL_HOLDINGS}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "OBS_FLAG", "CONF_STATUS", "leg_form", "farmtype","so_eur", "crops", "unit"], axis=1)
            #drop tuples that have null value
            result = result.dropna()

            result = result.rename(columns={"geo": "Country"})
            
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            
            result = result.rename(columns={"OBS_VALUE": r"Hectares utilised in Agriculture"})
            result[r"Hectares utilised in Agriculture"] = pandas.to_numeric(result[r"Hectares utilised in Agriculture"])

            result = result.rename(columns={"uaarea": "Size of farm holding"})

            #data set description
            description = "Land area utilized in agriculture by size of farm holding"
            #anonymizing head
            head = result.head()
            randValues = head["Hectares utilised in Agriculture"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["Hectares utilised in Agriculture"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})
            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result




    @staticmethod
    def GrossValueAddedBySector(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_GROSS_VALUE_ADDED_BY_SECTOR)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_GROSS_VALUE_ADDED_BY_SECTOR}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "OBS_FLAG", "CONF_STATUS", "na_item", "unit"], axis=1)
            #drop tuples that have null value
            result = result.dropna()

            result = result.rename(columns={"geo": "Country"})
            
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            
            result = result.rename(columns={"OBS_VALUE": r"Gross Added Value in Euros"})
            result[r"Gross Added Value in Euros"] = pandas.to_numeric(result[r"Gross Added Value in Euros"])
            result["Gross Added Value in Euros"] = result["Gross Added Value in Euros"].apply(lambda x: x * 1000000)

            result = result.rename(columns={"nace_r2": "Sector"})

            #data set description
            description = "Gross added value by sector of activity"
            #anonymizing head
            head = result.head()
            randValues = head["Gross Added Value in Euros"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["Gross Added Value in Euros"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})

            #filtered_df = result[(result['Country'] == 'Belgium') & (result['Year'].dt.year == 2020)]

            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result



    @staticmethod
    def EmploymentByGenderEconomicActivity(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_EMPLOYMENT_BY_GENDER_ECONOMIC_ACTIVITY)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_EMPLOYMENT_BY_GENDER_ECONOMIC_ACTIVITY}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "OBS_FLAG", "CONF_STATUS", "unit", "age"], axis=1)
            #drop tuples that have null value
            result = result.dropna()

            result = result.rename(columns={"geo": "Country"})
            
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            
            result = result.rename(columns={"OBS_VALUE": r"Employed Population"})
            result[r"Employed Population"] = pandas.to_numeric(result[r"Employed Population"])
            result["Employed Population"] = result["Employed Population"].apply(lambda x: x * 1000)

            result = result.rename(columns={"sex": "Gender"})
            result = result.rename(columns={"nace_r2": "Economic Activity"})

            #data set description
            description = "Total number of employed persons by gender, economic activity and country along the years"
            #anonymizing head
            head = result.head()
            randValues = head["Employed Population"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["Employed Population"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})
            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result
        

    @staticmethod
    def EmploymentByGender(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_EMPLOYMENT_BY_GENDER)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_EMPLOYMENT_BY_GENDER}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "OBS_FLAG", "CONF_STATUS", "unit", "age", "indic_em"], axis=1)
            #drop tuples that have null value
            result = result.dropna()

            result = result.rename(columns={"geo": "Country"})
            
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            
            result = result.rename(columns={"OBS_VALUE": r"Employed Population"})
            result[r"Employed Population"] = pandas.to_numeric(result[r"Employed Population"])
            result["Employed Population"] = result["Employed Population"].apply(lambda x: x * 1000)

            result = result.rename(columns={"sex": "Gender"})

            #data set description
            description = "Total number of employed persons by gender and country along the years"
            #anonymizing head
            head = result.head()
            randValues = head["Employed Population"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["Employed Population"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})
            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result
        

    @staticmethod
    def UnemploymentRateByGenderDegreeUrbanization(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_UNEMPLOYMENT_BY_AGE_GENDER_DEGREE_URNANIZATION)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_UNEMPLOYMENT_BY_AGE_GENDER_DEGREE_URNANIZATION}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "OBS_FLAG", "CONF_STATUS", "unit", "age"], axis=1)
            #drop tuples that have null value
            result = result.dropna()
            
            result = result.rename(columns={"geo": "Country"})
            
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            
            result = result.rename(columns={"OBS_VALUE": r"% Unemployed Population"})
            result[r"% Unemployed Population"] = pandas.to_numeric(result[r"% Unemployed Population"])

            result = result.rename(columns={"sex": "Gender"})
            result = result.rename(columns={"deg_urb": "Degree or urbanization"})

            #data set description
            description = "Country Unemployed rate by gender, degree of ubanization along the years"
            #anonymizing head
            head = result.head()
            randValues = head[r"% Unemployed Population"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["% Unemployed Population"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})
            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result
        
    @staticmethod
    def UnemploymentRate(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_UNEMPLOYMENT_RATE)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_UNEMPLOYMENT_RATE}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "OBS_FLAG", "CONF_STATUS", "unit", "sex", "age", "isced11"], axis=1)
            #drop tuples that have null value
            result = result.dropna()
            
            result = result.rename(columns={"geo": "Country"})
            
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            
            result = result.rename(columns={"OBS_VALUE": r"% Unemployed Population"})
            result[r"% Unemployed Population"] = pandas.to_numeric(result[r"% Unemployed Population"])

            #data set description
            description = "Country Unemployed rate along the years"
            #anonymizing head
            head = result.head()
            randValues = head[r"% Unemployed Population"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["% Unemployed Population"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})
            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result

    @staticmethod        
    def EmploymentRateByAgeGenderDegreeUrbanization(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_EMPLOYMENT_RATE_BY_AGE_GENDER_DEGREE_URBANIZATION)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_EMPLOYMENT_RATE_BY_AGE_GENDER_DEGREE_URBANIZATION}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "OBS_FLAG", "CONF_STATUS", "unit"], axis=1)
            #drop tuples that have null value
            result = result.dropna()
            
            result = result.rename(columns={"geo": "Country"})
            
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            
            result = result.rename(columns={"OBS_VALUE": r"% Population"})
            result[r"% Population"] = pandas.to_numeric(result[r"% Population"])

            result = result.rename(columns={"deg_urb": "Degree of Urbanization"})
            result = result.rename(columns={"age": "Age Group"})
            result = result.rename(columns={"sex": "Gender"})

            #data set description
            description = "Country employment rate of employed persons by gender, degree or urbanization along the years"
            #anonymizing head
            head = result.head()
            randValues = head[r"% Population"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["% Population"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})
            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result

    @staticmethod        
    def AreaByTerritoryType(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_AREA_TYPE_TERRITORY)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_AREA_TYPE_TERRITORY}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "OBS_FLAG", "CONF_STATUS", "landuse", "unit"], axis=1)
            #drop tuples that have null value
            result = result.dropna()
            
            result = result.rename(columns={"geo": "Country"})
            
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            
            result = result.rename(columns={"OBS_VALUE": r"Square kilometers"})
            result[r"Square kilometers"] = pandas.to_numeric(result[r"Square kilometers"])

            result = result.rename(columns={"terrtypo": "Territory Type"})

            #data set description
            description = "Country area in square kilometers by territory type along the years"
            #anonymizing head
            head = result.head()
            randValues = head[r"Square kilometers"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["Square kilometers"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})
            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result
        
    @staticmethod
    def PopulationByAgeGenderTypeRegion(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_POPULATION_BY_AGE_GENDER_TYPE_REGION)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_POPULATION_BY_AGE_GENDER_TYPE_REGION}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "OBS_FLAG", "CONF_STATUS"], axis=1)
            #drop tuples that have null value
            result = result.dropna()
            
            result = result.rename(columns={"geo": "Country"})
            
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            
            result = result.rename(columns={"OBS_VALUE": r"Population"})
            result[r"Population"] = pandas.to_numeric(result[r"Population"])

            result = result.rename(columns={"age": "Age Group"})
            result = result.rename(columns={"sex": "Gender"})
            result = result.rename(columns={"terrtypo": "Territory Type"})

            #data set description
            description = "Country population by age and type of region along the years"
            #anonymizing head
            head = result.head()
            randValues = head[r"Population"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["Population"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})
            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result
    
    
    @staticmethod    
    def PopulationByAgeAndGender(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_POPULATION_BY_AGE_GENDER)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_POPULATION_BY_AGE_GENDER}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "OBS_FLAG", "CONF_STATUS", "unit"], axis=1)
            #drop tuples that have null value
            result = result.dropna()
            
            result = result.rename(columns={"geo": "Country"})
            
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            
            result = result.rename(columns={"OBS_VALUE": r"Population"})
            result[r"Population"] = pandas.to_numeric(result[r"Population"])

            result = result.rename(columns={"age": "Age Group"})
            result = result.rename(columns={"sex": "Gender"})

            #data set description
            description = "Country population by age and gender along the years"
            #anonymizing head
            head = result.head()
            randValues = head[r"Population"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["Population"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})
            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result


    @staticmethod
    def PopulationDensityByTypeofTerritory(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_POPULATION_DENSITY_TYPE_TERRITORY)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_POPULATION_DENSITY_TYPE_TERRITORY}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "OBS_FLAG", "CONF_STATUS"], axis=1)
            #drop tuples that have null value
            result = result.dropna()
            
            result = result.rename(columns={"geo": "Country"})
            
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            
            result = result.rename(columns={"OBS_VALUE": r"Persons per square km"})
            result[r"Persons per square km"] = pandas.to_numeric(result[r"Persons per square km"])

            result = result.rename(columns={"terrtypo": "Type of territory"})

            #data set description
            description = "Country population density in person per square meter by type of territory along the years"
            #anonymizing head
            head = result.head()
            randValues = head[r"Persons per square km"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["Persons per square km"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})
            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result


    @staticmethod
    def PopulationDensity(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_POPULATION_DENSITY)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_POPULATION_DENSITY}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "OBS_FLAG", "CONF_STATUS", "unit"], axis=1)
            #drop tuples that have null value
            result = result.dropna()
            
            result = result.rename(columns={"geo": "Country"})
            
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            
            result = result.rename(columns={"OBS_VALUE": r"Persons per square km"})
            result[r"Persons per square km"] = pandas.to_numeric(result[r"Persons per square km"])

            #data set description
            description = "Country population density in person per square meter along the years"
            #anonymizing head
            head = result.head()
            randValues = head[r"Persons per square km"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["Persons per square km"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})
            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result
        


    @staticmethod
    def PopulationByGender(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_POPULATION_BY_GENDER)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_POPULATION_BY_GENDER}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "OBS_FLAG", "CONF_STATUS", "age", "unit"], axis=1)
            #drop tuples that have null value
            result = result.dropna()
            
            result = result.rename(columns={"geo": "Country"})
            
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            
            result = result.rename(columns={"OBS_VALUE": r"Population"})
            result[r"Population"] = pandas.to_numeric(result[r"Population"])

            result = result.rename(columns={"sex": "Gender"})

            #data set description
            description = "Country population by gender along the years"
            #anonymizing head
            head = result.head()
            randValues = head[r"Population"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["Population"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})
            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result



    @staticmethod
    def DistributionPopulationDwellingType(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_POPULATION_DISTRIBUTION_DEGREE_URBANIZATION)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_POPULATION_DISTRIBUTION_DEGREE_URBANIZATION}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "OBS_FLAG", "CONF_STATUS", "unit", "incgrp", "building"], axis=1)
            #drop tuples that have null value
            result = result.dropna()
            
            result = result.rename(columns={"geo": "Country"})
            
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            
            result = result.rename(columns={"OBS_VALUE": r"% of Population"})
            result[r"% of Population"] = pandas.to_numeric(result[r"% of Population"])

            result = result.rename(columns={"deg_urb": "Dwelling Type"})

            #data set description
            description = "Country percentage of population distribution by dwelling type along the years"
            #anonymizing head
            head = result.head()
            randValues = head[r"% of Population"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf[r"% of Population"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})
            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result


    @staticmethod
    def PopulationTypeRegion(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_POPULATION_TYPE_REGION)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_POPULATION_TYPE_REGION}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "OBS_FLAG", "CONF_STATUS", "indic_de"], axis=1)
            #drop tuples that have null value
            result = result.dropna()
            
            result = result.rename(columns={"geo": "Country"})
            
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            
            result = result.rename(columns={"OBS_VALUE": "Population"})
            result["Population"] = pandas.to_numeric(result["Population"])

            result = result.rename(columns={"terrtypo": "Type of territory"})

            #data set description
            description = "Country population by type of region along the years"
            #anonymizing head
            head = result.head()
            randValues = head[r"Population"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["Population"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})
            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result
    

    @staticmethod    
    def TotalGHGases(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_GREEN_HOUSE_GASES)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_GREEN_HOUSE_GASES}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "unit","OBS_FLAG", "CONF_STATUS", "airpol"], axis=1)
            #drop tuples that have null value
            result = result.dropna()
            
            result = result.rename(columns={"geo": "Country"})
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            result = result.rename(columns={"OBS_VALUE": "Tons Green House Gases"})
            result["Tons Green House Gases"] = pandas.to_numeric(result["Tons Green House Gases"])
            result["Tons Green House Gases"] = result["Tons Green House Gases"].apply(lambda x: x * 1000)
            result = result.rename(columns={"src_crf": "Source Sector"})

            #data set description
            description = "Country produced tons of green house gases along the years"
            #anonymizing head
            head = result.head()
            randValues = head[r"Tons Green House Gases"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["Tons Green House Gases"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})
            
        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result
    

    @staticmethod    
    def DistributionAreaUnderOrganicFarming(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_AREA_DISTRIBUTION_UNDER_ORGANIC_FARMING)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_AREA_DISTRIBUTION_UNDER_ORGANIC_FARMING}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "unit","OBS_FLAG", "CONF_STATUS", "crops", "agprdmet"], axis=1)
            #drop tuples that have null value
            result = result.dropna()

            result = result.rename(columns={"geo": "Country"})
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            result = result.rename(columns={"OBS_VALUE": "% Total utilized Agriculture Area"})
            result["% Total utilized Agriculture Area"] = pandas.to_numeric(result["% Total utilized Agriculture Area"])

            #data set description
            description = "Country percentage of total utilised agricultural area under organic farming along the years"
            #anonymizing head
            head = result.head()
            randValues = head[r"% Total utilized Agriculture Area"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["% Total utilized Agriculture Area"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})

        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result


    @staticmethod
    def TotalGDP(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_GDP)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_GDP}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df.drop(["DATAFLOW", "LAST UPDATE", "freq", "unit", "na_item","OBS_FLAG", "CONF_STATUS"], axis=1)
            #drop tuples that have null value
            result = result.dropna()
            
            result = result.rename(columns={"geo": "Country"})
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            result = result.rename(columns={"OBS_VALUE": "GDP in Euros"})
            result["GDP in Euros"] = result["GDP in Euros"].apply(lambda x: x * 1000000)
            result["GDP in Euros"] = pandas.to_numeric(result["GDP in Euros"])

            #data set description
            description = "Country Gross Domestic Product (GDP) along the years"
            #anonymizing head
            head = result.head()
            randValues = head[r"GDP in Euros"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf[r"GDP in Euros"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})


        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result
        

    @staticmethod
    def TotalPopulation(IsURL:bool = True)-> pandasai.SmartDataframe:
        result = None
        try:
            #get data from eurostat
            if IsURL:
                reply = urlopen(GetData.URL_TOTAL_POPULATION)
            else:
                reply = os.path.join(os.getcwd(), f"resources/{GetData.PATH_TOTAL_POPULATION}")
            #read data
            df = pandas.read_csv(reply)
            # remove not needed
            result = df[["geo", "TIME_PERIOD", "OBS_VALUE"]]
            #drop tuples that have null value
            result = result.dropna()

            result = result.rename(columns={"geo": "Country"})
            result = result.rename(columns={"TIME_PERIOD": "Year"})
            
            result["Year"] = pandas.to_datetime(result["Year"], format='%Y')
            result = result.rename(columns={"OBS_VALUE": "Population"})
            result["Population"] = pandas.to_numeric(result["Population"])

            #data set description
            description = "Country total population along the years"
            #anonymizing head
            head = result.head()
            randValues = head["Population"] * random.rand()
            anonHeadDf = pandas.DataFrame(head)
            anonHeadDf["Population"] = randValues
            #passing result as pandasai SmartDataframe with descripton and configuration
            result = pandasai.SmartDataframe(result, description=description, config={"custom_head": anonHeadDf})

        except Exception as ex:
            print(f"Unable to get data from source: {ex}")
        finally:
            return result