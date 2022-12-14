# peabody

какие есть api по options
https://polygon.io/
  есть исторические данные, но нету open interest
https://datashop.cboe.com/
  кажется если все, но дорого и не понятно
https://eodhistoricaldata.com/financial-apis/stock-options-data/
  нету исторических данных

Получаю инфу по стокам
Stocks Grouped Daily (Bars)
/v2/aggs/grouped/locale/us/market/stocks/{date}
для всех дней за
2022 год 365 запросов
2021 год 365 запросов
730 запросов / 5 запросов в минуту = 146 минут


Получаю инфу по опционам
Options Contracts
/v3/reference/options/contracts
включая закрытые

Гружу для всех них свечи
Aggregates (Bars)
/v2/aggs/ticker/{optionsTicker}/range/{multiplier}/{timespan}/{from}/{to}

приоритет
put < 10
месячные

что мне интересно
1. ставка годовых при максимальном отдалении от цены акции
2. фильтр по объему
3. открытый интерес, изменение открытого интереса
4. не типичный объем на контракте

получать открытый интерес от биржи можно по
Option Contract
/v3/snapshot/options/{underlyingAsset}/{optionContract
но нет исторических данных, нужна подписка
Options Starter
$29/month

возможно open interest можно посчитать из trades если в нет есть направление сделки для которой нужна уже следующая подписка
Options Developer
$79/month
