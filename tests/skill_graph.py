
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.classes.getData import GetData

from pandas import DataFrame
import datetime

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

sdf = GetData.TotalPopulation(False)
df = DataFrame(sdf.dataframe.pandas_df)
unit = df.columns[-1]
PlotLinearGraph(df, unit, "exports/charts/image.png")
