-- (c) DATLLC 2014 All Rights Reserved
--  Author: Peter Ogilvie
--    Date: 12/8/2014
--    File: today.sql
-- Summary: Latest Price of all the stocks in our database

select ticker_name, sp_price
  from Ticker join StockPrice
    on Ticker.rowid = StockPrice.sp_id;
