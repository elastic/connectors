import aiohttp
import asyncio

async def make_request():
    url = 'https://icanhazdadjoke.com/'
    headers = {'Authorization': 'Bearer YOUR_ACCESS_TOKEN', 'Accept': 'application/json'}
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            response_text = await response.text()
            print(response_text)

# Run the asynchronous function
asyncio.run(make_request())
