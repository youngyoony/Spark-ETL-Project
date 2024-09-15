import requests

url = "https://yt-api.p.rapidapi.com/dl"

querystring = {"id":"arj7oStGLkU"}

headers = {
	"x-rapidapi-key": "7c8a2c6a79msh04674dc52dbd72dp125f50jsn20be7524e794",
	"x-rapidapi-host": "yt-api.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())