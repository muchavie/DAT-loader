-- (c) DATLLC 2014 All Rights Reserved
--   Author: Peter Ogilvie
--     Date: 12/10/2014
--     File: createTablesMYSQL.sql
--  Summary: Creates the schema for the MYSQL version of the analytic database
--           (analytic.db)

create database Analytic;

create table Analytic.Ticker (
            rowid serial,
      ticker_name char(6) unique
);

create table Analytic.StockPrice (
            sp_id integer references ticker (rowid),
         sp_price real,
          sp_date date,
          unique (sp_id, sp_date)
);


grant select on Analytic.Ticker to 'darkangel'@'localhost';
grant insert on Analytic.Ticker to 'darkangel'@'localhost';
grant delete on Analytic.Ticker to 'darkangel'@'localhost';
grant select on Analytic.StockPrice to 'darkangel'@'localhost';
grant insert on Analytic.StockPrice to 'darkangel'@'localhost';
grant delete on Analytic.StockPrice to 'darkangel'@'localhost';
