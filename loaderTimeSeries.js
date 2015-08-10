// (c) 2014 DAT, All Rights Reserved
//   Author: Peter Ogilvie
//  Version: 20141123
//     File: loaderTimeSeries.js
//  Summary:

var                  fs = require('fs'),
                  spawn = require('child_process').spawn,
               zipChild, runSQL, fileExt, sqlChild,
     historicalDataPath = '/space/historical_data',
            scratchPath = '/space/scratch',
                tmpPath = '/space/tmp',
                unzipEx = '/usr/bin/unzip',
                  sqlEx = '/usr/bin/sqlite3'
             columnDefs = '(UnderlyingSymbol text,UnderlyingPrice real ,Exchange text,OptionRoot text ,OptionExt text,Type text,Expiration date, DataDate date,Strike real,Last real, Bid real, Ask real,Volume integer,OpenInterest integer)',
         tmpSQLFileName = tmpPath + '/' + 't.sql';

function cleanUP() {
    var files;

    files = fs.readdirSync(scratchPath);
    files.forEach(function (file) {
        var path = scratchPath + '/' + file;
        console.log('removing: ' + path);
        fs.unlinkSync(path);
    });

    files = fs.readdirSync(tmpPath);
    files.forEach(function (file) {
        var path = tmpPath + '/' + file;
        console.log('removing: ' + path);
        fs.unlinkSync(path);
    });
}
 // on exit event:  fires after uncompress operation completes
var runSQL = function (code, signal) {

    console.log('unzip: ' +  'exit code:' + code + ' signal:' + signal);

    if (code !== 0) {
        console.log('uncompress exit code non zero: ' + code);
        return;
    }

    fs.readdir(scratchPath, function (err, csvFiles) {

        if (err)
            throw (err);

        csvFiles.forEach(function (csvFileName) {
            var newTableName, createTableSQL, importSTMT, importCMD, trimSQL;

            // 01234567890123456789012
            // bb_options_20130903.csv
            newTableName  = csvFileName.slice(3, 19);

            // CREATE TABLE options_yyyymmdd
            createTableSQL = 'create table ' + newTableName + ' ' + columnDefs + ';\n'
            fs.appendFileSync(tmpSQLFileName, createTableSQL);

            // import data into table options_yyyymmdd from bb_options_yyyymmdd.csv
            importCMD = '.import ' + scratchPath + '/' + csvFileName + ' ' + newTableName + '\n';
            fs.appendFileSync(tmpSQLFileName, importCMD);

            trimSQL = 'delete from ' + newTableName + ' where UnderlyingSymbol ISNULL;\n';
            fs.appendFileSync(tmpSQLFileName, trimSQL);

            trimSQL = 'delete from ' + newTableName + ' where UnderlyingSymbol = \'UnderlyingSymbol\';\n';

        });

        fs.open(tmpSQLFileName, 'r', function (err, input) {

            if (err)
                throw(err);

            sqlChild = spawn(sqlEx, ['timeSeries.db'], {
                  cwd : '/space/db1',
                stdio : [input, process.stdout, process.stderr]
            });

            sqlChild.on('exit', function (sqlExitCode, sqlSignal) {
               cleanUP();
            });
        });
    });
}

// cleanup from previous runs
cleanUP();

fileName = process.argv[2];

fileExt  = fileName.slice(fileName.length - 4, fileName.length);
// separator for .csv files is ','
fs.appendFileSync(tmpSQLFileName, '.separator ,\n');
console.log(fileName, fileExt);

if (fileExt !== '.zip')
    throw('bad file type');

console.log("processing " + fileName);

zipChild = spawn(unzipEx, [historicalDataPath + '/' + fileName], {
                cwd : '/space/scratch'
            });

zipChild.on('exit', runSQL);
