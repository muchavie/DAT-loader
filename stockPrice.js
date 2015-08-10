#!/usr/local/bin/node
// (c) 2014 DATLLC All Rights Reserved
//   Author: Peter Ogilvie
//     date: 12/8/2014
//     File: stockPrice.js
//  Summary: This script is designed to be run from cron daily to update the
//           current stock prices for companies in the Ticker table.

var           sql3 = require('sqlite3'),
               yql = require('yql'),
    analyticDBPath = 'analytic.db',
                db = new sql3.Database(analyticDBPath),
             today = new Date();


db.serialize(function() {
    db.each('select rowid, ticker_name from Ticker' , function(sql3err, row) {

        var symbol, id, yq,
            startq = 'select price from yahoo.finance.oquote where symbol = "',
              endq = '"';

        if (sql3err)
            throw(sql3err);

            symbol = row.ticker_name;
                id = row.rowid;

                yq = new yql(startq + symbol + endq);

        yq.exec(function(yqlerr, yqlresult) {
            var quote, insert, values, price, month, day, year, date;

            if (yqlerr)
                throw(yqlerr);

             quote = yqlresult.query.results.option;

            console.log(quote);

             price = quote.price;

            //       0123456789      0123456789
            // ydate yyyy-mm-dd date mm/dd/yyyy

               day = today.getDate();
             month = today.getMonth() + 1;
              year = today.getUTCFullYear();

              date = month + '/' + day + '/' + year

            insert = 'insert into StockPrice (sp_id, sp_price, sp_date) values (?, ?, ?)';
            db.run(insert, id, price, date, function() {
                console.log("inserted: " + id + ', ' + price + ', ' + date);
            });
        });
    });
});
