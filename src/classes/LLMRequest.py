import os
import requests

class LLMRequest:
    AGRI_URL = "https://ai.agri.srv4dev.net/api/chat/completions"
    AGRI_API = os.getenv("AGRI_OPENWEBUI_API")

    def __init__(self, model:str):
        self.__headers = {
                    "Authorization": f"Bearer {LLMRequest.AGRI_API}",
                    "Content-Type": "application/json"
                }
        self.__model = model
        pass

    def ExplainPythonCode(self, code:str)->str:
        payload = {
                    "model": self.__model,
                    "messages":
                    [
                        {
                        "role": "user",
                        "content": "Explain in simple non technical terms the following python code, only use short bullet points: " + code
                        }
                    ]
                }
        reply = ""
        try:
            response = requests.post(LLMRequest.AGRI_URL, headers=self.__headers, json=payload)
            if response.status_code == 200:
                messageContent = response.json()
                reply = messageContent["choices"][0]["message"]["content"]
            else:
                reply = f"Error {response.status_code}: { response.text}"

        except Exception as ex:
            reply = ex
        finally:
            return reply

        
