import unittest
import sys
import os
import math
import datetime
import pandas

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.classes.PandasAIService import PandasAIService
from pandas import DataFrame
from src.classes.getData import GetData

class Test_Skill(unittest.TestCase):
    def test_LinearRegression_1(self):
        #arrange
        data = [
            [2, 80],
            [4, 90],
            [6, 95],
            [8, 98],
            [10, 100]
        ]
        df = DataFrame(data)
        df = df.rename(columns={0: "Year"})
        df = df.rename(columns={1: "Dependent"})
        #act
        lr_11 = PandasAIService.SimpleLinearRegression(df, "Dependent", 11)
        #assert
        self.assertEqual(lr_11, 104.6)

    def test_LinearRegression_2(self):
        #arrange
        data = [
            [1852, 316000],
            [1975, 277000],
            [1176, 155000],
            [1550, 253000],
            [1458, 211000],
            [2689, 329000],
            [2259, 317000],
            [2763, 360000],
            [1325, 204000],
            [1992, 250000]
        ]
        df = DataFrame(data)
        df = df.rename(columns={0: "Year"})
        df = df.rename(columns={1: "Values"})
        #act
        result_1 = PandasAIService.SimpleLinearRegression(df, "Values", 1)
        result_2 = PandasAIService.SimpleLinearRegression(df, "Values", 2)
        result_3 = PandasAIService.SimpleLinearRegression(df, "Values", 3)
        result_4 = PandasAIService.SimpleLinearRegression(df, "Values", 4)
        
        #assert
        self.assertEqual(True, math.isclose(result_1, 61027.79, abs_tol=0.1))
        self.assertEqual(True, math.isclose(result_2, 61136.14, abs_tol=0.1))
        self.assertEqual(True, math.isclose(result_3, 61244.49, abs_tol=0.1))
        self.assertEqual(True, math.isclose(result_4, 61352.83, abs_tol=0.1))

    def test_LinearRegression_Real(self):
        #arrange
        dfs = GetData.TotalPopulation(False)
        df = dfs.dataframe.pandas_df
        portugal_data = df[df["Country"] == "Portugal"]
        portugal_data["Year"] = pandas.to_datetime(portugal_data["Year"])
        portugal_data['Year'] = portugal_data['Year'].dt.year
        #act
        predicted_population = PandasAIService.SimpleLinearRegression(portugal_data, 'Population', 2030)

        #assert
        self.assertEqual(True, math.isclose(predicted_population, 10612700.6, abs_tol=0.1))

