import requests
import os
 
#using ollama

url = "http://localhost:11434/api/embed"

response = requests.post(url=url,
                         json={
                             'model': 'llama3.2:latest',
                             'input': 'Text to get the embeddings from'
                         }
                         )

if response.status_code == 200:
    messageContent = response.json()
    vector = messageContent["embeddings"]
    print(vector[0])
    print(f"Embedding vector size is {len(vector[0])}")
else:
    print(f"Error {response.status_code}: {response.text}")