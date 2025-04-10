from datetime import datetime
from services import db
from quart import Quart
from services import get_unusual_deals

app = Quart(__name__)

@app.before_serving
async def startup():
    await db.connect()

@app.after_serving
async def shutdown():
    await db.close()

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/filter', methods=['POST'])
async def filter():
    df = await get_unusual_deals({
        "date_start": datetime(2025, 1, 8),
        "date_finish": datetime(2025, 4, 1),
        "currency" : "USD"
    })



if __name__ == '__main__':
    app.run()
