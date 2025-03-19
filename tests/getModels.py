import requests
import os
 
url = "https://ai.agri.srv4dev.net/api/models"
apiKey = os.getenv("agri_ai_token")

headers = {
    "Authorization": f"Bearer {apiKey}",
    "Content-Type": "application/json"
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    messageContent = response.json()
    replyToUser = messageContent["data"]
    print(replyToUser)
else:
    print(f"Error {response.status_code}: {response.text}")
