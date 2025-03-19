import unittest
import sys
import os
from src.classes.getData import GetData
from src.classes.requirement import Requirement


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))




class Test_Requirement(unittest.TestCase):
    
    def test_SelectValueCell(self):
        #arrange
        req = Requirement()
        #act
        result = req.SelectValueCell()
        req.DumpLog()
        
        #Assert
        self.assertGreaterEqual(result, 0.97)
    
    # test_SelectValueCell(French, German, etc)
    # test_SelectRow