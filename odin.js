#!/usr/local/bin/node
// (c) DATLLC 2014 All Rights Reserved
//   Author: Peter Ogilvie
//     Date: 12/27/2014
//     File: odin.js
//  Summary: Uses YQL to plull data from yahoo finance and store this data in
//           StockPrice table.    This script plugs our data gap where our
//           option data in timeSeries.db ends before updateStockPrice.js
//           up and running on ohm.  Eventually odin will be modified to just
//           to do updates after trading closes on trading days.

var   sql3 = require('sqlite3'),
      yql  = require('yql');
 oneMinute = 1000 * 60,
   oneHour = oneMinute * 60;                        // time in milliseconds

var db,
    select = 'select ticker_name, sp_id, sp_date ',
    from   = 'from Ticker join StockPrice on Ticker.rowid = StockPrice.sp_id ',
    where  = "where ticker_active = 'true' ",
    group  = "group by sp_id having max(julianday(sp_date)) < julianday('2014-04-01') limit 30",
       q5  = select + from + where + group;

db = new sql3.Database('/space/db1/analytic.db');

function invalidateTicker (symbol)
{
    var update = "update Ticker set ticker_active = 'false'  where ticker_name = ?";

    db.run(update, symbol, function (sql3err) {

        if (sql3err)
            throw(sql3err);

        console.log("Ticker Marked INVALID: " + symbol);

    });
}

function insertTickerData (id, quote)
{
    var insert = 'insert into StockPrice (sp_id, sp_price, sp_date) values (?,?,?)';

    quote.forEach(function (q) {
        db.run(insert, id, q.Close, q['Date'], function(sql3err) {

            if (sql3err)
                throw(sql3err);

            console.log('insert:' + id + '/' + q.Symbol + '/' + q['Date'] + '/' + q.Close);
        });
    });
}

var m = function ()
{
    db.all(q5, function (sql3Err, rows) {

        if (sql3Err)
            throw(sql3Err);

        if (rows.length == 0)
            throw('All Done');

        console.log('Q5 found ' + rows.length + ' rows');

        rows.forEach(function (row) {
            var queryPrefix = 'select * from yahoo.finance.historicaldata where symbol = "',
                querySuffix = '" and startDate = "2014-04-01" and endDate = "2014-06-30"',
                query, symbol, id, yq;

                id = row.sp_id;
            symbol = row.ticker_name;
             query = queryPrefix + symbol + querySuffix;

                yq = new yql(query);

            yq.exec(function (yqlerr, yqlresult) {
                var quote;

                if (yqlerr)
                    throw(yqlerr);

                if (yqlresult.query.count == 0) {
                    // invalid symbol
                    invalidateTicker(symbol);
                } else {
                    // insert row in StockPrice table
                    quote = yqlresult.query.results.quote;
                    insertTickerData(id, quote)
                }
            });
        });
    });
};

// Call m (short for main) immediately then call it every hour + 1 minute after
m(); setInterval(m, oneHour + oneMinute);
