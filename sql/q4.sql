-- (c) DAT, All Rights Reserved
-- Author: Peter Ogilvie
-- Date  : 12/2/2014
-- Copies data (minues a few coloums) from the TimeSeries.db to the Analytic.db
-- I'm finding the orginal concept of using ticker rowid as a foriegn key to
-- expensive for the number of rows that we're looking at here.
-- This insert is called query4 inside loader.c.  This file is used to test the sql
-- by hand.
--  
-- query4.sql

attach database 'analytic.db' as Analytic;
attach database 'timeSeries.db' as TimeSeries;

insert into Analytic.Option
    (op_symbol, op_type, op_expiration, op_dataDate, op_strike, op_last, op_bid, op_ask, op_volume, op_openInterest )
    select UnderlyingSymbol ,Type, Expiration, DataDate ,Strike ,Last , Bid , Ask ,Volume ,OpenInterest
    from TimeSeries.options_20080102;
