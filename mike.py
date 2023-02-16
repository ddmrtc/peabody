from polygon import RESTClient
from polygon import exceptions
import pandas
import logging, datetime
import pdb
import time
import pathlib
import pickle
from common import *

#import matplotlib.pyplot as plt, mplcursors

pandas.set_option('display.width', None)
pandas.set_option('display.max_rows', None)

############
#           Что я вообще тут делаю:
# Есть два словаря
# Словарь тикеров
# Словарь опционов с двуия опциями: активные / просроченные
# Мне нужно сравнить два списка, что бы в словаре тикеров знать у кого есть опционы, у кого нету
# Словарь тикеров мне пока не нужен
# Словарь опционов мне нужен
# Просроченные мне нужны что бы проверить робота исторически
# Моя задача разметить недельные/месячные опционы
# Скачать данные для месячных, отметить какие уже скачал
# Я хочу еще сразу написать круто) и сделать анализ локальной копии данных
#
############



class MyFormatter(logging.Formatter):
    converter=datetime.datetime.fromtimestamp
    def formatTime(self, record, datefmt=None):
        ct=self.converter(record.created)
        if datefmt:
            s=ct.strftime(datefmt)
        else:
            t=ct.strftime("%Y-%m-%d %H:%M:%S")
            s="%s.%03d" % (t, record.msecs)
        return s

name = 'mike'
prefix = ''
date = datetime.datetime.now()
logFolder = 'log'
dataFolder = 'data'
apiKey = 'DzWlFydCcEwwf48FVMICzRjtRS9_7_h3'
expirationDate = '2021-01-01'

pathlib.Path(logFolder).mkdir(parents=True, exist_ok=True)
pathlib.Path(dataFolder).mkdir(parents=True, exist_ok=True)

def init():
    if prefix:
        fileName = f'{name}-{prefix}'
    else:
        fileName = name

    log = f"{logFolder}//{fileName}-{date.strftime('%Y-%m-%d-%H-%M-%S')}"
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

def updateTickers(sleep=0, market='', type=''):
    tickers = client.list_tickers(market=market, type=type, limit=1000)

    i = 1

    tickersList = []

    for t in tickers:
        logging.info(f"[{i}] {t.ticker}")
        tickersList.append(t.__dict__)
        i += 1
        time.sleep(sleep)

    tickersDict = pandas.DataFrame(tickersList)

    print(tickersDict)

    try:
        tickersDictOld = pandas.read_csv(f'{dataFolder}//tickers.csv')
    except:
        tickersDictOld = pandas.DataFrame()

    tickersDict.to_csv(f'{dataFolder}//tickers.csv', index=False, sep=';', decimal=',', encoding='utf-8-sig')    


def getOptionTickers(ticker='', expired=False, expiration_date_gt=None, expiration_date_lt=None, sleep=0):
    contracts = client.list_options_contracts(limit=1000, underlying_ticker=ticker, expired=expired, expiration_date_gt=expiration_date_gt, expiration_date_lt=expiration_date_lt)
    
    i = 1

    contractsList = []

    for c in contracts:
        logging.info(f"[{ticker}][{i}] {c.ticker}")
        contractsList.append(c.__dict__)
        i += 1
        time.sleep(sleep)

    contractsDict = pandas.DataFrame(contractsList)
    if contractsDict.empty:
        logging.info(f'[{ticker}] NO NEW DATA')
    else:
        logging.info(f'[{ticker}] {len(contractsDict)} Options Contracts')
        logging.debug(f'\n{contractsDict}')

    return contractsDict


def updateOptionTickers(ticker, expired = False):
    typeId = 'contracts'
    if expired:
        expiredFlag = 'Expired '
    else:
        expiredFlag = ''
    file = f'{dataFolder}//{ticker}{typeId}{expiredFlag.strip()}.pickle'

    contracts = readFile(file, typeId=typeId, ticker=ticker)

    if contracts.empty:
        # Загружаем всё
        logging.info(f'[{ticker}] Get {expiredFlag}Contracts')
        contractsNew = getOptionTickers(ticker=ticker, expired=expired, expiration_date_gt=expirationDate, sleep=0.01)
        contractsNew = contractsNew.drop_duplicates(subset='ticker', keep='first')
        writeFile(file, contractsNew, typeId=typeId, ticker=ticker)
    else:
        # Загружаем с последней даты
        if expired:
            expirationDate1 = sorted(list(set(contracts['expiration_date'].to_list())))[-1]
            logging.info(f'[{ticker}] Get {expiredFlag}Contracts from date {expirationDate1}')
        else:
            expirationDate1 = expirationDate
            logging.info(f'[{ticker}] Get {expiredFlag}Contracts')
        contractsNew = getOptionTickers(ticker=ticker, expired=expired, expiration_date_gt=expirationDate1, sleep=0)
        contractsNew = contractsNew.drop_duplicates(subset='ticker', keep='first')
        if not contractsNew.empty:
            diff, diffData = checkPandasDiff(contracts, contractsNew, key='ticker', typeId=typeId, checkRemoved=False)
            if diff:
                logging.info(f'[{ticker}] {{diffData}}')
                if diffData['add'] > 0:
                    logging.info(f'[{ticker}] ADDED {diffData["add"]}')
                    contractsNew = pandas.concat([contracts, contractsNew])
                    print(contractsNew)
                    writeFile(file, contractsNew, typeId=typeId, ticker=ticker)
            else:
                logging.info(f'[{ticker}] NO NEW DATA')



def readFile(file, typeId = '', ticker = ''):
    if ticker:
        ticker = f'[{ticker}] '
    try:
        logging.info(f'{ticker}Loading {typeId.upper()}..')
        with open(file, 'rb') as fileName:
            data = pickle.load(fileName)
        logging.info(f'{ticker}Loaded {len(data)} {typeId.upper()}')
        logging.debug(f'\n{data}')
        return data
    except:
        logging.info(f'{ticker}No data was loaded')
        return pandas.DataFrame()


def writeFile(file, data, typeId = '', ticker = ''):
    if ticker:
        ticker = f'[{ticker}] '

    logging.info(f'{ticker}Save {len(data)} {typeId.upper()}..')
    with open(file, 'wb') as filename:
        pickle.dump(data, filename)
    
    data.to_csv(file.replace('pickle', 'csv'), index=False, sep=';', decimal=',', encoding='utf-8-sig')
    return None


def getThirdFriday(year, month):
    # First day of the month
    d = datetime.datetime(int(year), int(month), 1)
    # Move to the first Friday of the month
    if d.weekday() > 4:
        d = d + datetime.timedelta(days=7 - d.weekday() + 4)
    else:
        d = d + datetime.timedelta(days=4 - d.weekday())
    # Move to the third Friday of the month
    d = d + datetime.timedelta(days=7 * 2)
    d = d.strftime('%Y-%m-%d')
    return d

def closestDate(dates, givenDate):
    givenDate = datetime.datetime.strptime(givenDate, "%Y-%m-%d")
    dates = [datetime.datetime.strptime(d, "%Y-%m-%d") for d in dates]
    return min(dates, key=lambda x: abs(x - givenDate)).strftime('%Y-%m-%d')


if __name__ == "__main__":
    init()
    logging.info('Started')
    client = RESTClient(apiKey)

    # type='CS'
    # updateTickers(market='stocks', type=type, sleep=0.01)

#    type='ETF'
#    updateTickers(market='stocks', type=type, sleep=0.01)

    # Работа с контрактами = опцион
    ticker = 'CVNA'

    # Загрузка завершенных=просроченных опционов
    updateOptionTickers(ticker=ticker, expired=True)

    # Загрузка активных опционов
    updateOptionTickers(ticker=ticker)

    exit()
    
    logging.info(f'[{ticker}] Get Contracts')
    contractsNew = getOptionTickers(ticker=ticker, expired=False, expiration_date_gt=expirationDate, sleep=0)

    logging.info(f'[{ticker}] Check Contracts')
    diff, diff_data = checkPandasDiff(contracts, contractsNew, key='ticker', typeId=typeId)
    print(diff)
    print(diff_data)


    exit()
    logging.info(f'[{ticker}] Loading Options Contracts')
    contractsDict = pandas.read_csv(f'{dataFolder}//{ticker}contracts.csv', sep=';', decimal=',', encoding='utf-8-sig')

    logging.info(f'[{ticker}] Options Contracts:\n{contractsDict}')

    dates = sorted(list(set(contractsDict['expiration_date'].to_list())))

    months = sorted(list(set([d[0:7] for d in dates])))

    logging.info(f'[{ticker}] Expiration Dates in {len(months)} months')

    for m in months:
        logging.info(f'[{ticker}] Check Dates in {m}')
        date = m.split('-')
        thirdFriday = getThirdFriday(date[0], date[1])
        logging.info(f'[{ticker}]  3rd Friday is {thirdFriday}')
        datesInMonth = [x for x in dates if x[0:7] == m]
        checkFriday = [x for x in datesInMonth if x == thirdFriday]
        if checkFriday:
            logging.info(f'[{ticker}] Montly Option in {m} matched: {checkFriday[0]}')
        else:
            logging.info(f'[{ticker}] Montly Option in {m} is NOT matched: {thirdFriday} not in {datesInMonth}')
            closestDate1 = closestDate(datesInMonth, thirdFriday)
            logging.info(f'[{ticker}] closest date is {closestDate1}')


    # for d in dates:
    #     print(d)