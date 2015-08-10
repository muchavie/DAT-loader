#!/usr/local/bin/node
// (c) 2014 DATLLC All Rights Reserved
//   Author: Peter Ogilvie
//     date: 12/10/2014
//     File: xloader.js
//  Summary: This script queries the rows from the specified sqlite table
//           of the analytic database and insert the same rows in a mysql
//           version of the analytic database.
//
//           By convention variables ending in '3' are sqlite3 driver related.
//
//           By convention variables starting with 'my' are mysql related.
//
//           This script must be run from the same directory as sqlite
//           analytic.db
//
//     Pred: (1) createTablesMYSQL.sql must have been run.   This script is DML
//               only no DDL
//           (2) Ticker must be loaded before StockPrice in order to maintain
//               foriegn key contraints on StockPrice.
//    usage: xloader.js <Ticker|StockPrice>

var           sql3 = require('sqlite3'),
             mysql = require('mysql'),
   analyticDBPath3 = '/space/db1/analytic.db',
               db3 = new sql3.Database(analyticDBPath3, sql3.OPEN_READONLY);
      myConnection = mysql.createConnection( {   host : 'localhost',
                                                 user : 'darkangel',
                                             password : ''           } );

myConnection.connect(function(err) {

    if (err)
        throw(err);

    console.log('Connection to mysql successful with id ' +  myConnection.threadId);

});

db3.serialize(function() {
    var remaining, countsql, lower, upper;

    if (process.argv.length != 4) {
        console.log('usage: xloader.js <lowser bound date> <upper bound date>');
        console.log("example: ./xloader 2008-01-01 2009-01-01");
        myConnection.end();
        return;
    }
    lower = process.argv[2];
    upper = process.argv[3];

    lower = "'" + lower + "'";
    upper = "'" + upper + "'";

    countsql = "select count(*) from StockPrice where sp_date >= date(" + lower + ") and sp_date < (" + upper + ")";
    db3.get(countsql, function (sql3err, row) {

        if (sql3err)
            throw(sql3err);

        remaining = row["count(*)"];

        console.log(remaining);
    })
    db3.serialize(function() {
        var getsql;

        getsql = "select sp_id, sp_price, sp_date from StockPrice where sp_date >= date(" + lower + ") and sp_date < (" + upper + ")";
console.log(getsql);
        db3.each(getsql, function(sql3err, row) {

            var symbol, id, myInsert, myData, price, date;

            if (sql3err)
                throw(sql3err);

                    id = row.sp_id;
                 price = row.sp_price;
                  date = row.sp_date;
                myData = {sp_id: id, sp_price : price, sp_date : date};
              myInsert = 'insert into Analytic.StockPrice SET ?';

            console.log(myData);

            myConnection.query(myInsert, myData, function (myErr, myResult) {

                if (myErr)
                    throw(myErr);

                remaining = remaining - 1;

                console.log(remaining);
                if (!remaining)
                    myConnection.end();

            });
        });
    });
});
