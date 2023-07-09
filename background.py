from flask import Flask
from threading import Thread
import datetime as dt

app = Flask('')

@app.route('/')
def home():
    return "I'm alive. Now is {}".format(dt.datetime.now() + dt.timedelta(hours=3))

def run():
    app.run(host='0.0.0.0', port=80)

def keep_alive():
    t = Thread(target=run)
    t.start()
