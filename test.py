from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv()

import requests
import json

response = requests.post(
  url=os.getenv('OPENROUTER_BASE_URL'),
  headers={
    "Authorization": f"Bearer {os.getenv('OPENROUTER_API_KEY')}",
    "Content-Type": "application/json",
  },
  data=json.dumps({
    "model": os.getenv('OPENROUTER_MODEL'),
    "messages": [
      {
        "role": "user",
        "content": "Quem matou Odete Roitmann?"
      }
    ],
    'provider': { 
      'sort': 'price'
      ,
      'data_collection': 'deny'
    }    
  })
)

# Parse the JSON response
response_data = json.loads(response.text)

# Extract and print only the message content
message_content = response_data['choices'][0]['message']['content']
print(message_content)
