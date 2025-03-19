Objective
- allow for AGRI internal users to interact with pmef data via natural language (full range of real business questions)
- generation of table and graphical representations
- selection and filtering of data
- execution of mathematical (logic and statistical) operations

Tasks
- get data from agrivew from prod db
- connect to llm and instantiate pandasai SmartDatalake as a service
- use streamlit as front end


Architectural options and conditions
- data is extracted from prod db, url from eurostat or csv file (dev)
- pandasai generates python code based on user prompt
- llm explain executed code to user
- basic auth is implemented
- skill have been added to agent

Pre-requisites
- pip install -r requirements.txt
