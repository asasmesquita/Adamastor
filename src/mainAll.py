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
        {"name": "Population by Type of Region", "getter": GetData.PopulationTypeRegion},
        {"name": "Area by Territory Type", "getter": GetData.AreaByTerritoryType},
        {"name": "Green House Gases", "getter": GetData.TotalGHGases},
        {"name": "Gross Domestic Product", "getter": GetData.TotalGDP},
        {"name": "Gross value added by sector", "getter": GetData.GrossValueAddedBySector},
        {"name": "Agriculture holdings", "getter": GetData.AgriculturalHoldings},
        {"name": "Water abstracted by sector", "getter": GetData.WaterAbstractedBySectorOfUse}

    ]#page 38 share od total employment vy type of region
    selectedDataSets = []
    for dataset in dataSets:
        df = dataset["getter"]()
        selectedDataSets.append(df)
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

            if streamlit.button("Generate"):
                if prompt:
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