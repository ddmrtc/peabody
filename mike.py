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
dateStr = date.strftime('%Y-%m-%d')
logFolder = 'log'
dataFolder = 'data'
candlesFolder = f'{dataFolder}//candles'
apiKey = 'DzWlFydCcEwwf48FVMICzRjtRS9_7_h3'
fromDate = '2021-01-01'
sleep = 0.01

pathlib.Path(logFolder).mkdir(parents=True, exist_ok=True)
pathlib.Path(dataFolder).mkdir(parents=True, exist_ok=True)
pathlib.Path(candlesFolder).mkdir(parents=True, exist_ok=True)

def init():
    if prefix:
        fileName = f'{name}-{prefix}'
    else:
        fileName = name

    log = f'{logFolder}//{fileName}-{date.strftime("%Y-%m-%d-%H-%M-%S")}'
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
        logging.info(f'[{i}] {t.ticker}')
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
        logging.info(f'[{ticker}][{i}] {c.ticker}')
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


def updateOptionTickers(ticker, data, expired = False):
    typeId = 'contracts'
    if expired:
        expiredFlag = 'Expired '
    else:
        expiredFlag = ''
    
    # Загружаем с последней даты
    if expired:
        expirationDate = sorted(list(set(data['expiration_date'].to_list())))[-1]
        logging.info(f'[{ticker}] Get {expiredFlag}Contracts from date {expirationDate}')
    else:
        expirationDate = fromDate
        logging.info(f'[{ticker}] Get {expiredFlag}Contracts')

    dataNew = getOptionTickers(ticker=ticker, expired=expired, expiration_date_gt=expirationDate, sleep=sleep)
    dataNew = dataNew.drop_duplicates(subset='ticker', keep='first')

    if not dataNew.empty:
        diff, diffData = checkPandasDiff(data, dataNew, key='ticker', typeId=typeId, checkRemoved=False, excludes=['expired', 'monthly'])
        if diff:
            logging.info(f'[{ticker}] {diffData}')
            if diffData['add'] > 0:
                logging.info(f'[{ticker}] ADDED {diffData["add"]}')
                if expired:
                    data = pandas.concat([data, dataNew])
                else:
                    data = dataNew
        else:
            logging.info(f'[{ticker}] DATA IS EQUALS')

    return data


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
    closest = min(dates, key=lambda x: abs(x - givenDate))
    days = abs((givenDate - closest).days)
    return closest.strftime('%Y-%m-%d'), days


def setDates(data, ticker):
    dates = sorted(list(set(data['expiration_date'].to_list())))

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
            data.loc[data['expiration_date'] == checkFriday[0], 'monthly'] = True
        else:
            logging.info(f'[{ticker}] Montly Option in {m} is NOT matched: {thirdFriday} not in {datesInMonth}')
            closest, days = closestDate(datesInMonth, thirdFriday)
            if days < 7:
                logging.info(f'[{ticker}] closest date is {closest}, days: {days}')
                data.loc[data['expiration_date'] == closest, 'monthly'] = True
            else:
                logging.info(f'[{ticker}] closest date {closest} points to next week, it can be monthly')
    return data


def getAggs(ticker, underlyingTicker='', date=''):
    
    if underlyingTicker:
        underlyingTicker += '//'
    if date:
        date += '//'
    candlesPath = f'{candlesFolder}//{underlyingTicker}{date}'
    pathlib.Path(candlesPath).mkdir(parents=True, exist_ok=True)
    file = f'{candlesPath}{ticker.replace("O:","")}.csv'

    
    try:
        aggs = pandas.read_csv(file, sep = ';', decimal = ',', encoding = 'utf-8-sig')
        return len(aggs)
    except:
        pass

    aggs = None
    while aggs is None:
        try:
            aggs = client.get_aggs(ticker, 1, 'day', fromDate, dateStr, limit=50000)
            time.sleep(6)
        except exceptions.NoResultsError as e:
            if underlyingTicker:
                logging.info(f"[{underlyingTicker.replace('//','')}][{ticker}]\tNO DATA")
            else:
                logging.info(f"[{ticker}]\tNO DATA")
            time.sleep(12)
            return 0
        except exceptions.BadResponse as e:
            logging.info(e)
            time.sleep(30)

    aggs = pandas.DataFrame(aggs)
    aggs['timestamp'] = pandas.to_datetime(aggs['timestamp'],unit='ms')

    aggs.to_csv(file, index = False, sep = ';', decimal = ',', encoding = 'utf-8-sig')

    return len(aggs)


def process(ticker, update = True):
    # Загрузка данных, которые есть
    typeId = 'contracts'
    file = f'{dataFolder}//{ticker}{typeId}.pickle'

    contracts = readFile(file, typeId=typeId, ticker=ticker)
    if update:
        if contracts.empty:
            logging.info(f'[{ticker}] Get Expired Contracts')
            contracts                   = getOptionTickers(ticker=ticker, expired=True , expiration_date_gt=fromDate, expiration_date_lt='2023-01-01', sleep=sleep)
            contracts                   = contracts.drop_duplicates(subset='ticker', keep='first')
            contracts['expired']        = True
            logging.info(f'[{ticker}] Get Active Contracts')
            contractsActive             = getOptionTickers(ticker=ticker, expired=False, expiration_date_gt=fromDate, expiration_date_lt='2024-01-01', sleep=sleep)
            contractsActive             = contractsActive.drop_duplicates(subset='ticker', keep='first')
            contractsActive['expired']  = False
            
            contracts = pandas.concat([contracts, contractsActive], ignore_index=True)
            contracts = contracts.sort_values(by=['expiration_date', 'ticker'])
            #contracts = contracts.reset_index()
            contracts['monthly'] = False
            contracts = setDates(contracts, ticker)
            writeFile(file, contracts, ticker=ticker)
        else:
            expired = contracts['expired']
            logging.info(f'[{ticker}] Update {len(contracts[expired])} Expired Contracts')
            update = updateOptionTickers(ticker, contracts[expired], expired=True)
            logging.info(f'[{ticker}] Updated {len(update)}')
            update.loc[update['expired'].isna(), 'expired'] = True

            active = contracts['expired'] == False
            logging.info(f'[{ticker}] Update {len(contracts[active])} Contracts')
            updateActive = updateOptionTickers(ticker, contracts[active], expired=False)
            logging.info(f'[{ticker}] Updated {len(updateActive)}')
            if 'expired' in updateActive:
                updateActive.loc[updateActive['expired'].isna(), 'expired'] = False
            else:
                updateActive['expired']  = False

            contracts = pandas.concat([update, updateActive], ignore_index=True)
            contracts = contracts.sort_values(by=['expiration_date', 'ticker'])
            contracts.loc[contracts['monthly'].isna(), 'monthly'] = False
            contracts = setDates(contracts, ticker)
            writeFile(file, contracts, ticker=ticker)

    monthly = contracts['monthly'] == True
    total = len(contracts[monthly])
    totalLen = len(str(total))
    logging.info(f'[{ticker}] MONTHLY {total}')
    j = 0
    for i, c in contracts[monthly].iterrows():
        j += 1
        logging.info(f"[{ticker}][{j:>{totalLen}}/{total}] {c['expiration_date']} {c['contract_type'].upper():<4} {c['strike_price']}\t{c['ticker']}\t{c.get('candles', None)}")
        if pandas.isnull(c.get('candles', None)):
            candles = getAggs(c['ticker'], underlyingTicker=ticker, date=c['expiration_date'])
            contracts.loc[i, 'candles'] = candles
            if j % 10 == 0:
                writeFile(file.replace(typeId,typeId+'1'), contracts, ticker=ticker)


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
    process(ticker, update=False)

    