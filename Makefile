# (c) 2014 DAT, all rights reserved
# Author: Peter Ogilvie
# Date  : 12/2/2014
# File  : loader.c
# loader Pulls time series data from timeSeries.db and store it analytic.db
# cdf    SQLite extenstion function to common US formated dates

all : loader cdf.so

cdf.so : cdf.o
	ld -shared -o cdf.so cdf.o -lsqlite3

cdf.o : cdf.c
	gcc  -c -fPIC -g cdf.c

loader : loader.c
	gcc -g -o loader loader.c -lsqlite3

clean :
	rm cdf.o loader

clobber :
	rm analytic.db
