#static class with methods that modify received list in accordance with code

from pandas import DataFrame

class Metadata:
    def Add(data:list)-> DataFrame:
        result = None
        try:
            #get the code and execute specific logic
            code = data[0][0] # first element of first tuple
            if code == 'C.01_a':
                result = Metadata.C01()
        except Exception as ex:
            print(f"Unable to add metadada to code {code}: ex")
        finally:
            return result
    

    def C01_a(data:list)-> DataFrame:
        return DataFrame(data)