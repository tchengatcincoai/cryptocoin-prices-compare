import datetime
import decimal
import json
import logging
import time
from multiprocessing.dummy import Pool as ThreadPool

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, render_template, redirect, url_for, request, make_response, jsonify
from flask_sqlalchemy import SQLAlchemy

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] (%(name)s) %(message)s')
log = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///local_storage.db'
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


def collect_coin_status():
    try:
        for coin in COINS:
            # for every coin, get price from different exchange with multi threads
            coin_currency_exchange_tuple_list = []
            pool = ThreadPool(len(EXCHANGES) + 1)
            for exchange in EXCHANGES:
                coin_currency_exchange_tuple_list.append((coin, CURRENCY, exchange))
            results = pool.map(get_price_data, coin_currency_exchange_tuple_list)
            pool.close()
            pool.join()
            for result in results:
                if result:
                    db.session.add(result)
            db.session.commit()
            # wait 1s to make sure wont hit 1 second rate limiter
            time.sleep(1)
    except Exception as e:
        log.exception(e)


def get_price_data(coin_currency_exchange_tuple):
    coin = coin_currency_exchange_tuple[0]
    currency = coin_currency_exchange_tuple[1]
    exchange = coin_currency_exchange_tuple[2]
    url = CRYPTOCOMPARE_PRICE_URL.format(fsym=coin, tsyms=currency, exchange=exchange)
    updated_at = datetime.datetime.today()
    data = requests.get(url=url).json()
    if CURRENCY in data:
        price = data[currency]
        entry = CoinStatus(coin=coin, exchange=exchange, price=price, updated_at=updated_at)
        return entry
    else:
        log.info(data)
        return None


sched = BackgroundScheduler()
sched.add_job(collect_coin_status, 'interval', seconds=10,
              start_date=datetime.datetime.today() + datetime.timedelta(minutes=10), max_instances=1)
sched.start()


def json_handler(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj


def json_response(data, status_code):
    response = app.response_class(
        response=json.dumps(data),
        status=status_code,
        mimetype='application/json'
    )
    return response


@app.route('/')
def default_page():
    return redirect(url_for('index'))


@app.route('/index', endpoint='index')
def index_page():
    return render_template("index.html")


@app.route('/get_coin_status', methods=['GET'])
def get_coin_status():
    # return data from db and return as json
    result = {}
    try:
        for coin in COINS:
            result[coin] = {}
            for exchange in EXCHANGES:
                data = CoinStatus.query.filter_by(coin=coin, exchange=exchange).order_by(
                    CoinStatus.id.desc()).first()
                if data:
                    result[coin][exchange] = {'price': data.price, 'updated_at': data.updated_at.strftime('%Y-%m-%d %H:%M:%S')}
                else:
                    log.info('{0){1}'.format(coin, exchange))
        return json_response(result, 200)
    except Exception as e:
        log.exception(e)
        return json_response({'error': e}, 500)


if __name__ == '__main__':
    app.run(host='0.0.0.0')
