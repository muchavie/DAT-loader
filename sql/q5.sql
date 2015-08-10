-- (c) DAT, All Rights Reserved
-- Author: Peter Ogilvie
-- Date  : 12/25/2014
-- Finds 8 Tickers in the StockPrice Table which need an update

   select ticker_name, sp_id, sp_date
     from Ticker join StockPrice on Ticker.rowid = StockPrice.sp_id
    where ticker_active = 'true'
 group by sp_id having max(julianday(sp_date)) < 2456658 limit 8;
