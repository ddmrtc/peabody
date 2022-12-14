from polygon import RESTClient
import pandas
import logging, datetime as dt
import pdb
import time

import matplotlib.pyplot as plt, mplcursors

#Получаю все стоки для которых есть опционы
#Получаю все PUT опционы для стоков со страйком меньше 10
#Гружу все исторические данные OHLC для опционов и стоков что бы рассчитать % на вложенный капитал и отдаленность от цены акции
#Перегрупирую по дням
#Пишу логику 


pandas.set_option('display.width', None)
#pandas.set_option('display.max_rows', None)

class MyFormatter(logging.Formatter):
    converter=dt.datetime.fromtimestamp
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s.%03d" % (t, record.msecs)
        return s

name = 'polygon'
prefix = ''
date = dt.datetime.now()

def init():
    log = f"log\\{name}-{prefix}-{date.strftime('%Y-%m-%d-%H-%M-%S')}"
    log_info = log + '-INFO.log'
    log = log + '.log'

    logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s.%(msecs)06f - %(funcName)s - %(levelname)s - %(message)s',
                            datefmt='%H:%M:%S',
                            handlers=[logging.StreamHandler(),
                                      logging.FileHandler(log, 'w', 'utf-8-sig'),
                                      logging.FileHandler(log_info, 'w', 'utf-8-sig'),
                                      ])

    formatter = MyFormatter(fmt='%(asctime)s - %(funcName)s - %(levelname)s - %(message)s',datefmt='%H:%M:%S.%f')

    logging.getLogger().handlers[0].setLevel(logging.INFO)
    logging.getLogger().handlers[1].setLevel(logging.DEBUG)
    logging.getLogger().handlers[2].setLevel(logging.INFO)
    logging.getLogger('chardet.charsetprober').setLevel(logging.INFO)

    for i in logging.getLogger().handlers:
        i.setFormatter(formatter)


def list_tickers():
    tickers = client.list_tickers(limit=1000)

    i = 1

    tickersList = []

    for t in tickers:
        #print(t)
        logging.info(f"[{i}] {t.ticker}")
        tickersList.append(t.__dict__)
        i += 1
        #pdb.set_trace()
        time.sleep(0.01)
        #break

    tickersDict = pandas.DataFrame(tickersList)

    print(tickersDict)

    tickersDict.to_csv('tickers.csv', index = False, sep = ';', decimal = ',', encoding = 'utf-8-sig')

    #print(tickers)


def list_options_contracts():
    contracts = client.list_options_contracts(limit=1000)

    i = 1

    contractsList = []

    for c in contracts:
        #print(t)
        logging.info(f"[{i}] {c.ticker}")
        contractsList.append(c.__dict__)
        i += 1
        #pdb.set_trace()
        time.sleep(0.01)

    contractsDict = pandas.DataFrame(contractsList)

    print(contractsDict)

    contractsDict.to_csv('contracts.csv', index = False, sep = ';', decimal = ',', encoding = 'utf-8-sig')

    #print(tickers)


def get_aggs(ticker):

    aggs = client.get_aggs(ticker, 1, 'day', '2017-01-01', '2023-01-01', limit=50000)

    aggsDict = pandas.DataFrame(aggs)
    aggsDict['timestamp'] = pandas.to_datetime(aggsDict['timestamp'],unit='ms')

    print(aggsDict)
    #aggsDict.timestamp.to_datetime()


    fig, axes = plt.subplots(nrows=2, ncols=1)

    #axes[0].plot(tt, price, marker = 'o', color ='r', label = 'sell')

    aggsDict.plot(y = 'close',  x = 'timestamp', ax = axes[0])
    aggsDict.plot(y = 'volume', x = 'timestamp', ax = axes[1])

    mplcursors.cursor(hover=True)

    plt.show()


if __name__ == "__main__":
    init()
    logging.debug('Started')
    client = RESTClient("DzWlFydCcEwwf48FVMICzRjtRS9_7_h3") # api_key is used

    ticker = 'O:SPCE240119C00020000'

    #list_options_contracts()
    get_aggs(ticker)
