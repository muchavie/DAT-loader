-- (c) DAT, All Rights Reserved
-- Author: Peter Ogilvie
-- Date  : 12/29/2014
-- Counts the number of Tickers to be updated by an Odin run

   select Ticker.rowid, ticker_name, ticker_active, max(julianday(sp_date))
     from Ticker join StockPrice on Ticker.rowid = StockPrice.sp_id
    where ticker_active = 'true'
 group by sp_id having max(julianday(sp_date)) < 2456690 limit 8;
