import asyncio
from sanic import Sanic
from sanic.response import json
from sanic.exceptions import NotFound
from time import sleep, ctime
import sanic

app = Sanic(name="pyapp")

async def task_sleep():
    print('-', ctime())
    await asyncio.sleep(3)
    print('+', ctime())
    return '3'

@app.route('/')
async def test(request):
    myLoop = request.app.loop
    myLoop.create_task(task_sleep())
    return json({'hello': 'world'})

if __name__ == '__main__':
    app.error_handler.add(
        NotFound,
        lambda r, e: sanic.response.empty(status=404)
    )
    app.run(host='0.0.0.0', port=80)