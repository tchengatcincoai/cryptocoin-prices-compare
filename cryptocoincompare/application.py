import datetime
import logging

import requests
from flask import Flask, render_template, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] (%(name)s) %(message)s')
log = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////local_storage.db'
db = SQLAlchemy(app)

CRYPTOCOMPARE_PRICE_URL = 'https://min-api.cryptocompare.com/data/price?fsym={fsym}&tsyms={tsyms}&e={exchange}'
EXCHANGES = ['CCCAGG', 'Coinbase', 'Bitfinex', 'Bitstamp', 'Kraken']
COINS = ['BTC', 'ETH', 'LTC']
CURRENCY = 'USD'


class CoinStatus(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    coin = db.Column(db.String(50), nullable=False)
    exchange = db.Column(db.String(50), nullable=False)
    price = db.Column(db.FLOAT, nullable=False)
    updated_at = db.Column(db.DATETIME, nullable=False)

    def __init__(self, coin, exchange, price, updated_at):
        self.coin = coin
        self.exchange = exchange
        self.price = price
        self.updated_at = updated_at

    def __repr__(self):
        return 'CoinStatus(coin={coin}, exchange={exchange}, price={price}, updated_at={updated_at}'.format(
            coin=self.coin,
            exchange=self.exchange,
            price=self.price,
            updated_at=self.updated_at)


db.create_all()


def get_bitcoin_status():
    updated_at = datetime.datetime.today()
    try:
        for coin in COINS:
            for exchange in EXCHANGES:
                url = CRYPTOCOMPARE_PRICE_URL.format(fsym=coin, tsyms=CURRENCY, exchange=exchange)
                data = requests.get(url=url).json()
                price = data[CURRENCY]
                entry = CoinStatus(coin=coin, exchange=exchange, price=price, updated_at=updated_at)
                db.session.add(entry)
        db.session.commit()
    except Exception as e:
        log.exception(e)


sched = BackgroundScheduler()
sched.add_job(get_bitcoin_status, 'interval', seconds=10,
              start_date=datetime.datetime.today() + datetime.timedelta(minutes=5), max_instances=1)
sched.start()

@app.route('/')
def default_page():
    return redirect(url_for('index'))


@app.route('/index', endpoint='index')
def index_page():
    return render_template("index.html")


if __name__ == '__main__':
    app.run(host='0.0.0.0')
