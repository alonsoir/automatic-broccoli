import os
from embedchain import App

os.environ["OPENAI_API_KEY"] = "YOUR-API-KEY-HERE"
app = App()

# Query the app
response = app.query("give me a resumen.")
print(f"response is {response}")