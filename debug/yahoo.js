#!/usr/local/bin/node

var yql = require('yql'),
   YHOO = new yql('select Date,Close from yahoo.finance.historicaldata where symbol = "YHOO" and startDate = "2014-01-01" and endDate = "2014-11-01"');


YHOO.exec(function (err, qresult) {

    if (err)
        throw(err);

    console.log('rows found: ' + qresult.query.count);

    qresult.query.results.quote.forEach(function(row) {
        console.log(row);
    });

});
