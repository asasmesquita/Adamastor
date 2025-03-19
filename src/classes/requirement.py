import sys
import os
import math
import datetime
from pandas import to_datetime
from pandas import api

from src.classes.PandasAIService import PandasAIService
from src.classes.getData import GetData

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

#confirm if current pandasai implementation observes the functioal requirements for selecting data, performing mathematical operations and forecasting
#assume that users selects datasets to be considered correctly

class Requirement:
    ATTEMPTS_PER_SOURCE = 5
    
    def __init__(self):
        #builder
        self.__dataSets = [
            {"name": "Country Population", "getter": GetData.TotalPopulation},
            {"name": "Gross Domestic Product", "getter": GetData.TotalGDP},
            {"name": "Green House Gases", "getter": GetData.TotalGHGases},
            {"name": "Percentage of Agricultural Area Under Organic Farming", "getter": GetData.DistributionAreaUnderOrganicFarming},
            {"name": "Population by Type of Region", "getter": GetData.PopulationTypeRegion},
            {"name": "Percentage of Population by Dwelling Type", "getter": GetData.DistributionPopulationDwellingType},
            {"name": "Population By Gender", "getter": GetData.PopulationByGender},
            {"name": "Population Density", "getter": GetData.PopulationDensity},
            {"name": "Population Density by Type of Territory", "getter": GetData.PopulationDensityByTypeofTerritory},
            {"name": "Population by Age and Gender", "getter": GetData.PopulationByAgeAndGender},
            {"name": "Population by Age, Gender and Type of Region", "getter": GetData.PopulationByAgeGenderTypeRegion},
            {"name": "Area by Territory Type", "getter": GetData.AreaByTerritoryType},
            {"name": "Employment Rate by Age, Gender and Degree of Urbanization", "getter": GetData.EmploymentRateByAgeGenderDegreeUrbanization},
            {"name": "Unemployment Rate", "getter": GetData.UnemploymentRate},
            {"name": "Unemployment Rate by Gender and Degree of Urbanization", "getter": GetData.UnemploymentRateByGenderDegreeUrbanization},
            {"name": "Employment by Gender", "getter": GetData.EmploymentByGender},
            {"name": "Employment by Gender and Economic Activity", "getter": GetData.EmploymentByGenderEconomicActivity}
        ]
        self.__log = []
        pass

    
    def DumpLog(self):
        try:
            with open("select.log", "a") as file:
                for result in self.__log:
                    file.write(result)
        
        except Exception as ex:
            print(f"Unable to write test logs: {ex}")

    def SelectValueCell(self)-> float:
        passed = 0
        totalIterations = 0
        try:
            #get data for each data source
            for data in self.__dataSets:
                df = data["getter"]()
                llmService = PandasAIService(df)
                #execute and compare
                for i in range(self.ATTEMPTS_PER_SOURCE):
                    totalIterations += 1
                    #get random parameters from dataset
                    sample = df.dataframe.pandas_df.sample(n=1).iloc[0]
                    country = sample["Country"]
                    year = str(sample["Year"])
                    year_str = to_datetime(year).strftime("%Y")

                    #get result from human defined method assuming that column with value is numeric and not country or year
                    value_column = None
                    other_columns = []
                    for col in sample.index:
                        if col not in ["Country", "Year"] and api.types.is_numeric_dtype(sample[col]) and value_column is None:
                            value_column = col
                        #if other columns exist
                        if col not in ["Country", "Year"] and not api.types.is_numeric_dtype(sample[col]):
                            other_columns.append(col)

                    if value_column is not None:
                        value = sample[value_column]
                    else:
                        raise Exception

                    #get result from llm
                    additionalColumns = "for "
                    for name in other_columns:
                        additionalColumns += f"{name} as {sample[name]}"
                        additionalColumns += ", "
                    if len(other_columns) > 0:
                        prompt = f"what is the {value_column} of {country} {additionalColumns} in {year_str}. Your reply is a number"
                    else:
                        prompt = f"what is the {value_column} of {country} in {year_str}. Your reply must be a number"
                    #llm
                    aiResponse = llmService.GetReplyAndCode(prompt)
                    
                    #compare
                    result = ""
                    llmString = f"{aiResponse[0]['value']}"
                    if aiResponse[0] is None or llmString == "No data was found on the selected datasets." or llmString == "" or llmString == None or not math.isclose(aiResponse[0]["value"], value, abs_tol=0.1):
                        result = "failed"
                    else :
                        passed +=1
                        result = "passed"
                    
                    log = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, {result}, {prompt}, LLM reply: {aiResponse[0]['value']}, True Value: {value}\n"
                    #add to log
                    self.__log.append(log) 
            
            self.__log.append(f"###########################\nSuccess rate was: {passed/totalIterations:.2f}%\n###########################\n")
            return passed / totalIterations

        except Exception as ex:
            print(f"Error in calculating SelectValueCell: {ex}")
    
    
    def SelectColumnByCountry(self)-> float:
        passed = 0
        totalIterations = 0
        try:
            #get data for each data source
            for data in self.__dataSets:
                df = data["getter"]()
                llmService = PandasAIService(df)
                #execute and compare
                for i in range(self.ATTEMPTS_PER_SOURCE):
                    totalIterations += 1
                    #get random parameters from dataset
                    sample = df.dataframe.pandas_df.sample(n=1).iloc[0]
                    country = sample["Country"]
                    year = str(sample["Year"])
                    year_str = to_datetime(year).strftime("%Y")

                    #get result from human defined method assuming that column with value is numeric and not country or year
                    value_column = None
                    other_columns = []
                    for col in sample.index:
                        if col not in ["Country", "Year"] and api.types.is_numeric_dtype(sample[col]) and value_column is None:
                            value_column = col
                        #if other columns exist
                        if col not in ["Country", "Year"] and not api.types.is_numeric_dtype(sample[col]):
                            other_columns.append(col)

                    if value_column is not None:
                        value = sample[value_column]
                    else:
                        raise Exception

                    #get result from llm
                    additionalColumns = "for "
                    for name in other_columns:
                        additionalColumns += f"{name} as {sample[name]}"
                        additionalColumns += ", "
                    if len(other_columns) > 0:
                        prompt = f"what is the {value_column} of {country} {additionalColumns} in {year_str}. Your reply is a number"
                    else:
                        prompt = f"what is the {value_column} of {country} in {year_str}. Your reply must be a number"
                    #llm
                    aiResponse = llmService.GetReplyAndCode(prompt)
                    
                    #compare
                    result = ""
                    llmString = f"{aiResponse[0]['value']}"
                    if aiResponse[0] is None or llmString == "No data was found on the selected datasets." or llmString == "" or llmString == None or not math.isclose(aiResponse[0]["value"], value, abs_tol=0.1):
                        result = "failed"
                    else :
                        passed +=1
                        result = "passed"
                    
                    log = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, {result}, {prompt}, LLM reply: {aiResponse[0]['value']}, True Value: {value}\n"
                    #add to log
                    self.__log.append(log) 
            
            self.__log.append(f"###########################\nSuccess rate was: {passed/totalIterations:.2f}%\n###########################\n")
            return passed / totalIterations

        except Exception as ex:
            print(f"Error in calculating SelectValueCell: {ex}")
    



