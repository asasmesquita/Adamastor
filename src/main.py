import streamlit
import streamlit_authenticator
#from streamlit_authenticator import Hasher
import yaml
from yaml.loader import SafeLoader
import os
#from classes.Db import Db
#from classes.metadata import Metadata
from classes.getData import GetData
from classes.PandasAIService import PandasAIService
from classes.LLMRequest import LLMRequest

def main():  
    #########################pmef datasets #####################
    dataSets = [
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
        {"name": "Employment by Gender and Economic Activity", "getter": GetData.EmploymentByGenderEconomicActivity},
        {"name": "Gross value added by sector", "getter": GetData.GrossValueAddedBySector},
        {"name": "Agriculture holdings", "getter": GetData.AgriculturalHoldings},
        {"name": "Water abstracted by sector", "getter": GetData.WaterAbstractedBySectorOfUse}
    ]#page 38 share od total employment vy type of region
    ##########################################frontend####################################################
    #frontend, cleaning streamlit chart store location
    imageFolder = os.path.join(os.getcwd(), f"exports/charts/")
    for fileName in os.listdir(imageFolder):
        if fileName.endswith(".png"):
            os.remove(os.path.join(imageFolder, fileName))
    ########################authentication do not use in Production##############################
    pathToFile = os.path.join(os.getcwd(), "resources/config.yaml")
    with open(pathToFile, "r", encoding="utf-8") as file:
        config = yaml.load(file, Loader=SafeLoader)
    #Hasher.hash_passwords(config["credentials"])
    authenticator = streamlit_authenticator.Authenticate(
        config["credentials"],
        config["cookie"]["name"],
        config["cookie"]["key"],
        config["cookie"]["expiry_days"]
    )
    #start up streamlit
    try:
        streamlit.title("AGRIVIEW ANALYSIS")
        authenticator.login()
        if streamlit.session_state["authentication_status"]:
            authenticator.logout()
            prompt = streamlit.text_area("Enter your analysis request:")
            result = None
            #restricting the datasets in order to improve quality of the results
            #generate checkbox with dataset names
            with streamlit.sidebar:
                streamlit.header("Select the data sets:")
                selection = []
                for dataset in dataSets:
                    selection.append(streamlit.checkbox(dataset["name"]))

            if streamlit.button("Generate"):
                if prompt:
                    #########################get selected datasets to load into smartdatalake#####################
                    selectedDataSets = [dataframe["getter"](False) for dataframe, selected in zip(dataSets, selection) if selected]
                    ##################################connect to llm service########################################
                    llmService = PandasAIService(selectedDataSets)

                    with streamlit.spinner("Generating response..."):
                        #delete charts if exists in default location
                        imageFolder = os.path.join(os.getcwd(), f"exports/charts/")
                        for fileName in os.listdir(imageFolder):
                            if fileName.endswith(".png"):
                                os.remove(os.path.join(imageFolder, fileName))
                        #execute prompt
                        result = llmService.GetReplyAndCode(prompt)
                        #show prompt result and show image if it exists
                        streamlit.write(result[0]["value"])
                        for fileName in os.listdir(imageFolder):
                            if fileName.endswith(".png"):
                                streamlit.image(os.path.join(imageFolder, fileName))
                        
            if result is not None:
                streamlit.subheader("Code executed to extract reply from data set:")
                code = result[1]
                streamlit.code(code, language="python")
                streamlit.subheader("Explanation of code executed.")
                with streamlit.spinner("Generating explanation:"):
                    agriLLM = LLMRequest("codellama:13b")
                    explanation = agriLLM.ExplainPythonCode(code)
                    streamlit.text(explanation)

        elif streamlit.session_state["authentication_status"] is False:
            streamlit.error("Username/password are incorrect")

        elif streamlit.session_state["authentication_status"] is None:
            streamlit.error("Enter your username and password")

        
                
    except Exception as ex:
        streamlit.error(f"Error executing request: {ex}")
    
    


if __name__ == "__main__":
    main()