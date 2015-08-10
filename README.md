Intro
=======================
This directory contains code developed to load, cross load and maintain historical financial data
for DATLLC and is posted with permission of DATLLC.

Custom Date Function
====================
sqlite3 date function expect a string with dates formatted as yyyy-mm-dd where yyyy is the four digit year mm is the two digit month and dd is the two digit day of the month.  One expresses January 2nd 1981 as '1981-01-02' in this format.  In the US 'mm/dd/yyyy' is commonly used to express dates.  

The cdf() function allows dates expressed and stored in sqlite3 database in the common US text format with the built in sqlite3 date functions.

cdf() packaged as a shared library and loaded dynamically at runtime.  Code is portable but creating and using dynamic shared libraries is not. Only linux has been tested and only Linux .so (shared object) file format is supprted by the Makefile.

files
-----
* Makefile
* cdf.c

building
--------
    % make cdf.so

    gcc  -c -fPIC -g cdf.c

    ld -shared -o cdf.so cdf.o -lsqlite3

loading automatically at startup
--------------------------------
add the line below to you ~/.sqliterc file

    .load /home/pogilvie/loader/cdf

Using from SQL
--------------
    select usDate from table order by cdf(usDate);


Loader
======
Utility which pulls data from a sqlite3 times eries database
created from CVS files to a normalized analytic star schema

files
-----
* Makefile
* loader.c

building
--------
    make loader


running
-------
    ./loader

ODIN
====
Uses YQL to plull data from yahoo finance and store this data in
StockPrice table.  This script plugs our data gap where our option
data in timeSeries.db ends before updateStockPrice.js up and
running on ohm.  Eventually odin will be modified to just
to do updates after trading closes on trading days.

Requires
--------
nodeJS running time installed on host machine.   see odin.js for required
packages.


    node odin.js
