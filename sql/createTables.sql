-- (c) DATLLC 2014 All Rights Reserved
--   Author: Peter Ogilvie
--     Date: 12/8/2014
--     File: createTables.sql
--  Summary: Creates the schema for the analytic database (analytic.db)
--           This script should be run from the same directory as timeSeries.db
--           % cd /space/db1
--           % sqlite3 < createTables.sql
--
attach database 'analytic.db' as Analytic;
attach database 'timeSeries.db' as TimeSeries;

create table Analytic.Ticker (
      ticker_name text unique,
      ticker_active boolean default true
);

create table Analytic.TimeSeriesMetaData (
        tsmd_name text,
      tsmd_rowcnt integer default 0,
   tsmd_processed boolean default false
);

create table Analytic.StockPrice (
            sp_id integer references ticker (rowid),
         sp_price real,
          sp_date date,
          unique (sp_id, sp_date)
);

create table Analytic.Option (
      op_tickerid integer references ticker (rowid),
        op_symbol text,  -- hack  way too expensive to track id as foriegn key
          op_type text,
    op_expiration date,
      op_dataDate date,
        op_strike real,
          op_last real,
           op_bid real,
           op_ask real,
        op_volume integer,
  op_openInterest integer
);

insert into Analytic.TimeSeriesMetaData (tsmd_name)
    select name from TimeSeries.sqlite_master where type = 'table';
