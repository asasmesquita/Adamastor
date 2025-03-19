import requests
import os
 
#connect to model
#model = LocalLLM(
#    api_base="http://ollama-llm.agri.srv4dev.net:11434/v1",
#    model="llama3.2"
#)

url = "https://ai.agri.srv4dev.net/api/chat/completions"
apiKey = os.getenv("agri_ai_token")

code = """
            # TODO: import the required dependencies
import pandas as pd

# EU countries list
eu_countries = [
    "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czechia", "Denmark",
    "Estonia", "Finland", "France", "Germany", "Greece", "Hungary", "Ireland",
    "Italy", "Latvia", "Lithuania", "Luxembourg", "Malta", "Netherlands",
    "Poland", "Portugal", "Romania", "Slovakia", "Slovenia", "Spain", "Sweden"
]

# Function to check if a DataFrame contains EU population data
def is_eu_population_dataframe(df):
    required_columns = {"Country", "Year", "Population"}
    if required_columns.issubset(df.columns):
        if all(df["Country"].isin(eu_countries)):
            return True
    return False

# Initialize an empty DataFrame to store EU population data
eu_population_df = pd.DataFrame(columns=["Country", "Year", "Population"])

# Loop through the DataFrames in dfs and filter EU population data
for df in dfs:
    if is_eu_population_dataframe(df):
        eu_population_df = pd.concat([eu_population_df, df])

# Calculate EU population by summing the populations of EU countries for each year
eu_population_df = eu_population_df.groupby("Year").sum().reset_index()
eu_population_df["Country"] = "EU"

# Declare result var:
result = {
    "type": "dataframe",
    "value": eu_population_df
}

# If no EU population data is found
if eu_population_df.empty:
    result = {
        "type": "string",
        "value": "No data was found on the selected datasets."
    }
"""

payload = {
    "model": "mistral:latest",
    "messages": [
        {
        "role": "user",
        "content": "Explain the following python code in simple bullet points:" + code
        }
    ]
}
headers = {
    "Authorization": f"Bearer {apiKey}",
    "Content-Type": "application/json"
}

response = requests.post(url, headers=headers, json=payload)

if response.status_code == 200:
    messageContent = response.json()
    replyToUser = messageContent["choices"][0]["message"]["content"]
    print(replyToUser)
else:
    print(f"Error {response.status_code}: {response.text}")
