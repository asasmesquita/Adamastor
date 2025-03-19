#connet to llm api service
#allows for the use of chat 
import os
import datetime
from pandas import DataFrame

from pandasai import SmartDatalake
from pandasai.llm.local_llm import LocalLLM
from pandasai.skills import skill

class PandasAIService:
    OPENROUTER_URL = "https://openrouter.ai/api/v1"
    OPENROUTER_API = "OPENROUTER_API"
    
    AGRI_URL = "https://ai.agri.srv4dev.net/api/v1"
    AGRI_API = "agri_ai_token"

    baseBehaviour = """
Select the data sets based on the description, column names and data types.
If no data exists or if you are unable to predict the a reply to the posed question, reply that "No data was found on the selected datasets."
If the user does not define a date, use the lastest year to present the result. Include the year the result.
You can call the functions defined between the tags <function></function> that have been pre-defined for you. You do not need to define these functions and should use them in your reply in accordance with the docstring defined.
EU means European Union. Values of the EU are obtained by adding all the values of the countries that are part of the EU.
eu_countries = ["Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czechia", "Denmark", "Estonia", "Finland", "France", "Germany", "Greece", "Hungary", "Ireland", "Italy", "Latvia", "Lithuania", "Luxembourg", "Malta", "Netherlands", "Poland", "Portugal", "Romania", "Slovakia", "Slovenia", "Spain", "Sweden"]
The question to reply or action to perform is: 
"""

    def __init__(self, data:list = [], modelName:str = "mistralai/codestral-2501"):
        self.__dataLake = SmartDatalake(data, config={"llm": PandasAIService.GetModel(modelName)})
        self.__dataLake.add_skills(PandasAIService.PlotLinearGraph, PandasAIService.SimpleLinearRegression)
        pass


    def GetReplyAndCode(self, prompt:str)->tuple:
        metaPrompt = PandasAIService.baseBehaviour + prompt
        self.__dataLake.chat(metaPrompt)
        return (self.__dataLake.last_result, self.__dataLake.last_code_executed)
    
    def GetModel(modelName:str)-> LocalLLM:
        url = ""
        apiKey = ""
        try:
            if modelName == "mistralai/codestral-2501" or modelName == "qwen/qwen-2.5-coder-32b-instruct":
                url = PandasAIService.OPENROUTER_URL
                apiKey = os.getenv(PandasAIService.OPENROUTER_API)
            else:
                url = PandasAIService.AGRI_URL
                apiKey = os.getenv(PandasAIService.AGRI_API)
            if apiKey is None:
                raise ValueError(f"Environmental variable for the API KEY is not defined.")
            model = LocalLLM(
                api_base=url,
                api_key=apiKey,
                model=modelName
            )
            return model
        
        except Exception as ex:
            print(ex)
            raise ex
    
    
    @skill
    @staticmethod
    def PlotLinearGraph(df:DataFrame, unit:str, filePath:str):
        """
        Generate and save a line chart having the years on x-axis and the values on y-axis with the country names in the legend
        Args:
        - df is a pandas.DataFrame that has the following columns:
            -countries (list[str]): Column with Country names used on the legend
            -years (list): Column with years, used in x-axis is a set of datetime.datetime objects that represent a moment in time
            -values (list): Column with name equal to the unit argument, is used in y-axis, is a set of numeric values
        - unit (str): a string that is the name of dataframe column that containes the values
        - filePath (str): string with the location to save the generated 
        """
        import matplotlib.pyplot
        import matplotlib.ticker
        from pandas import DataFrame
        import datetime#just in case this is not yet imported 
        
        #define size of image
        matplotlib.pyplot.figure(figsize=(12, 6))

        for country in df["Country"].unique():
            countryData = df[df["Country"] == country]
            matplotlib.pyplot.plot(countryData["Year"], countryData[unit], label=country)

        #add lables and titles
        matplotlib.pyplot.xlabel("Year")
        matplotlib.pyplot.ylabel(unit)
        matplotlib.pyplot.title("Country " + unit + " along the years")
        matplotlib.pyplot.legend(loc="upper right", bbox_to_anchor=(1.12, 1), fontsize="x-small")

        #turn off scientific notation
        axis = matplotlib.pyplot.gca()#get current axis
        formatter = matplotlib.ticker.ScalarFormatter(useOffset=False)
        formatter.set_scientific(False)
        axis.yaxis.set_major_formatter(formatter = formatter)

        matplotlib.pyplot.grid(True)
        
        matplotlib.pyplot.savefig(filePath)

    
    @skill
    @staticmethod
    def SimpleLinearRegression(dataSet: DataFrame, columnName:str, target_year:int):
        """
        Method that uses a simple linear regression method to forecast the value for the parameter target.
        The line equation is calculated based on the dataSet passed on argument.
        Args:
            - dataSet is a pandas.DataFrame :
                -countries (list[str]): Column with Country name used on the selection
                -years (list[int]): Column with years, used in x-axis for the independent variable, is a list of int that represent year
                -values (list): Column with name equal to the columnName argument, is used in y-axis for the dependent variable, is a list of numeric values
            - columnName (str) is the name of the dataSet column that contains the values used in y-axis for the dependent variable
            - target_year (int) is the year %YYYY in the x-axis independed variable value that will be used on the prediction

        Returns: A numeric value that is the y-axis dependent variable value that the linear regression returns by using the target in the x-axis or None if and exception occurs.
        """
        from pandas import DataFrame
        from pandas import to_datetime
        result = None
        try:
            #prepare the data by getting x-axis from column 'Year' and y-axis from column 'columnName'            
            df = DataFrame({
                "Year": dataSet["Year"],
                f"{columnName}": dataSet[f"{columnName}"]
            })
            #calculate line equation, y = bx + a, based on dataSet with columns x = dataSet[0], y = dataSet[1]
            #calculate the means
            means = df.mean()#will a column with the mean
            #compute the differences from the mean
            df[2] = df.apply(lambda row: row.iloc[0] - means.iloc[0], axis=1)
            df[3] = df.apply(lambda row: row.iloc[1] - means.iloc[1], axis=1)
            # multiply column 2 with 3
            df[4] = df.apply(lambda row: row.iloc[2] * row.iloc[3], axis=1)
            #square of column 2
            df[5] = df.apply(lambda row: row.iloc[2] * row.iloc[2], axis=1)
            b = df[4].sum() / df[5].sum()
            #calculate the intercept point with x=0 based on the average of all points
            a = means.iloc[1] - b * means.iloc[0]
            #calculate dependent variable value of with independent variable equal to target
            result = b * target_year + a
        except Exception as ex:
            print(f"Unable to predict value for {target_year}: " + ex)
            raise ex
        finally:
            return result
    
    