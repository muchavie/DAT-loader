// (c) 2014 DAT, all rights reserved
// Author: Peter Ogilvie
// Date  : 12/2/2014
// File  : loader.c
// Implements a sqlite3 data loader from database /space/db1/timeSeries.db to
// tickerData.db with schema defined by sql/createTables.sql

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const char *timeSeriesDB = "timeSeries.db";
const char *analyticDB = "analytic.db";

#define APPERR(OP, P, EC) ((void) printf("%s %s:%s\n", OP, P, sqlite3_errstr((EC))))
#define PREPARE(DB, STMT) (sqlite3_prepare_v2(DB, STMT.sql, strlen(STMT.sql), &(STMT.stmtp), (const char **)0))
#define FINALIZE(STMT) (sqlite3_finalize(STMT.stmtp))
#define STEP(STMT) (sqlite3_step(STMT.stmtp))
#define CLOSE(DB) (sqlite3_close(DB))
#define BINDSTR(STMT, INDEX, STRING) (sqlite3_bind_text (STMT.stmtp, INDEX, STRING, strlen(STRING), SQLITE_STATIC))
#define BINDINT(STMT, INDEX, VALUE) (sqlite3_bind_int(STMT.stmtp,INDEX,VALUE))
#define BINDDBL(STMT, INDEX, VALUE) (sqlite3_bind_double(STMT.stmtp, INDEX, VALUE))

sqlite3 *adb;
sqlite3 *tdb;

typedef struct stmt {
    const char   *sql;
    sqlite3_stmt *stmtp;
} stmt;


#define TICKEREXISTS "select ticker_name, rowid from Ticker where ticker_name = ?"
stmt tickerexists;

#define SQL_DISTINCTTICKER "select distinct UnderlyingSymbol from %s"
stmt stmt_distinctticker;
#define TMBSZ 8192
char sql_buffer[TMBSZ];

#define SQL_INSERTTICKER "insert or fail into ticker (ticker_name) values (?)"
stmt stmt_insertticker;

// find all the new ticker symbols which may have appeared that day and store them
// away so they have rowids

int insertTicker(char *ticker_symbol)
{
    int ec;

    if (!strcmp(ticker_symbol, "UnderlyingSymbol"))
        return 0;

    stmt_insertticker.sql = SQL_INSERTTICKER;
    ec = PREPARE(adb, stmt_insertticker);
    if (ec != SQLITE_OK) {
        APPERR("prepare", stmt_insertticker.sql, ec);
        exit(1);
    }

    ec = BINDSTR(stmt_insertticker, 1, ticker_symbol);
    if (ec != SQLITE_OK) {
        APPERR("bind", stmt_insertticker.sql, ec);
        exit(1);
    }

    ec = STEP(stmt_insertticker);
    if (ec == SQLITE_DONE) {
        ec = FINALIZE(stmt_insertticker);
        printf("insert of symbol %s succeeded\n", ticker_symbol);
        if (ec != SQLITE_OK) {
            APPERR("finalize", stmt_insertticker.sql, ec);
            exit(1);
        }
        return 1;
    } else if (ec == SQLITE_CONSTRAINT) {
        return 0;
    } else {
        APPERR("STEP", stmt_insertticker.sql, ec);
        exit(1);
    }


}

void update_day_pass1(char *table_name)
{
    char *ticker_symbol;
    int ec, symbols = 0, inserts = 0;


    stmt_distinctticker.sql = sql_buffer;
    sprintf(sql_buffer, SQL_DISTINCTTICKER, table_name);
    ec = PREPARE(tdb, stmt_distinctticker);
    if (ec != SQLITE_OK) {
        APPERR("prepare", stmt_distinctticker.sql, ec);
        exit(1);
    }

    do {

        ec = STEP(stmt_distinctticker);
        if (ec == SQLITE_ROW) {

            ticker_symbol = (char *)sqlite3_column_text(stmt_distinctticker.stmtp, 0);

            symbols++;
            if (insertTicker(ticker_symbol))
                inserts++;
        }

    } while (ec == SQLITE_ROW);

    printf("PASS1 %s %d %d\n", table_name, symbols, inserts);

    if (ec != SQLITE_DONE) {
        APPERR("step", stmt_distinctticker.sql, ec);
        exit(1);
    }

    ec = FINALIZE(stmt_distinctticker);
    if (ec != SQLITE_OK) {
        APPERR("finalize", stmt_distinctticker.sql, ec);
        exit(1);
    }
}

#define Q1 "select rowid, ticker_name from Ticker"
stmt stmt_q1;
#define Q2 "select distinct UnderlyingPrice, DataDate from %s where UnderlyingSymbol = '%s'"
stmt stmt_q2;
#define Q3 "insert or ignore into StockPrice (sp_id, sp_price, sp_date) values (?,?,?)"
stmt stmt_q3;

int insert_stock_price(int sp_id, double sp_price, char *sp_date)
{
    int ec;

    stmt_q3.sql = Q3;
    ec = PREPARE(adb, stmt_q3);
    if (ec != SQLITE_OK) {
        APPERR("prepare", stmt_q3.sql, ec);
        exit(1);
    }

    ec = BINDINT(stmt_q3, 1, sp_id);
    if (ec != SQLITE_OK) {
        APPERR("bind sp_id", stmt_q3.sql, ec);
        exit(1);
    }

    ec = BINDDBL(stmt_q3, 2, sp_price);
    if (ec != SQLITE_OK) {
        APPERR("bind sp_price", stmt_q3.sql, ec);
        exit(1);
    }

    ec = BINDSTR(stmt_q3, 3, sp_date);
    if (ec != SQLITE_OK) {
        APPERR("bind sp_date", stmt_q3.sql, ec);
        exit(1);
    }

    ec = STEP(stmt_q3);
    if (ec == SQLITE_DONE) {

        printf("insert of sp_id %d sp_price $%.2f sp_date %s succeeded\n",
                sp_id, sp_price, sp_date);

        ec = FINALIZE(stmt_q3);
        if (ec != SQLITE_OK) {
            APPERR("finalize", stmt_q3.sql, ec);
            exit(1);
        }
        return 1;

    } else if (ec == SQLITE_CONSTRAINT) {

        return 0;

    } else {

        APPERR("STEP", stmt_q3.sql, ec);
        exit(1);

    }
}

void get_stock_prices(int sp_id, char *table_name, char *symbol)
{
    int ec;
    double stock_price;
    char *data_date;

    stmt_q2.sql = sql_buffer;
    sprintf(sql_buffer, Q2, table_name, symbol);
    ec = PREPARE(tdb, stmt_q2);
    if (ec != SQLITE_OK) {
        APPERR("get_stock_prices:prepare", stmt_q2.sql, ec);
        exit(1);
    }

    do {

        ec = STEP(stmt_q2);
        if (ec == SQLITE_ROW) {
            stock_price = sqlite3_column_double(stmt_q2.stmtp, 0);
            data_date = (char *)sqlite3_column_text(stmt_q2.stmtp, 1);
            insert_stock_price(sp_id, stock_price, data_date);
        }

    } while (ec == SQLITE_ROW);

    if (ec != SQLITE_DONE) {
        APPERR("step", stmt_q2.sql, ec);
        exit(1);
    }

    ec = FINALIZE(stmt_q2);
    if (ec != SQLITE_OK) {
        APPERR("finalize", stmt_q2.sql, ec);
        exit(1);
    }
}



void update_day_pass2(char *table_name)
{
    int ec, sp_id;
    char *ticker_symbol;

    printf("PASS2\n");

    stmt_q1.sql = Q1;
    ec = PREPARE(adb, stmt_q1);
    if (ec != SQLITE_OK) {
        APPERR("prepare", stmt_q1.sql, ec);
        exit(1);
    }

    do {

        ec = STEP(stmt_q1);
        if (ec == SQLITE_ROW) {

            sp_id = sqlite3_column_int(stmt_q1.stmtp, 0);
            ticker_symbol = (char *)sqlite3_column_text(stmt_q1.stmtp, 1);

            get_stock_prices(sp_id, table_name, ticker_symbol);

        }

    } while (ec == SQLITE_ROW);

    if (ec != SQLITE_DONE) {
        APPERR("step", stmt_q1.sql, ec);
        exit(1);
    }

}

#define BEGIN "begin transaction"
stmt begin;
void begin_transaction(void)
{
    int ec;

    begin.sql = BEGIN;
    ec = PREPARE(adb, begin);
    if (ec != SQLITE_OK) {
        APPERR("prepare", begin.sql, ec);
        exit(1);
    }

    ec = STEP(begin);
    if (ec = SQLITE_OK) {
        APPERR("step", begin.sql, ec);
        exit(1);
    }


    ec = FINALIZE(begin);
    if (ec = SQLITE_OK) {
        APPERR("finalize", begin.sql, ec);
        exit(1);
    }
}


#define END "end transaction"
stmt end;
void end_transaction(void)
{
    int ec;

    end.sql = END;
    ec = PREPARE(adb, end);
    if (ec != SQLITE_OK) {
        APPERR("prepare", end.sql, ec);
        exit(1);
    }

    ec = STEP(end);
    if (ec = SQLITE_OK) {
        APPERR("step", end.sql, ec);
        exit(1);
    }

    ec = FINALIZE(end);
    if (ec = SQLITE_OK) {
        APPERR("finalize", end.sql, ec);
        exit(1);
    }
}

#define ATTACH "attach ? as ?"
stmt stmt_attach;
void attach(char *dbfile, char *dbname)
{
    int ec;

    stmt_attach.sql = ATTACH;
    ec = PREPARE(adb, stmt_attach);
    if (ec != SQLITE_OK) {
        APPERR("prepare", stmt_attach.sql, ec);
        exit(1);
    }

    ec = BINDSTR(stmt_attach, 1, dbfile);
    if (ec != SQLITE_OK) {
        APPERR("bind", stmt_attach.sql, ec);
        exit(1);
    }

    ec = BINDSTR(stmt_attach, 2, dbname);
    if (ec != SQLITE_OK) {
        APPERR("bind", stmt_attach.sql, ec);
        exit(1);
    }


    ec = STEP(stmt_attach);
    if (ec = SQLITE_OK) {
        APPERR("step", stmt_attach.sql, ec);
        exit(1);
    }

    ec = FINALIZE(stmt_attach);
    if (ec = SQLITE_OK) {
        APPERR("finalize", stmt_attach.sql, ec);
        exit(1);
    }
}


#define DETACH "detach ?"
stmt stmt_detach;
void detach(char *dbname)
{
    int ec;

    stmt_detach.sql = DETACH;
    ec = PREPARE(adb, stmt_detach);
    if (ec != SQLITE_OK) {
        APPERR("prepare", stmt_detach.sql, ec);
        exit(1);
    }

    ec = BINDSTR(stmt_detach, 1, dbname);
    if (ec != SQLITE_OK) {
        APPERR("bind", stmt_detach.sql, ec);
        exit(1);
    }

    ec = STEP(stmt_detach);
    if (ec = SQLITE_OK) {
        APPERR("step", stmt_detach.sql, ec);
        exit(1);
    }

    ec = FINALIZE(stmt_detach);
    if (ec = SQLITE_OK) {
        APPERR("finalize", stmt_detach.sql, ec);
        exit(1);
    }
}



#define Q4 "insert into %s.Option (op_symbol) select UnderlyingSymbol from %s.%s"
stmt stmt_q4;

void update_day_pass3(char *table_name)
{
    int ec;

    printf("PASS3: %s\n", table_name);

    attach("analytic.db", "analytic");
    attach("timeSeries.db", "timeSeries");

    stmt_q4.sql = sql_buffer;
    sprintf(sql_buffer, Q4, "analytic", "timeSeries", table_name);
    ec = PREPARE(adb, stmt_q4);
    if (ec != SQLITE_OK) {
        APPERR("prepare", stmt_q4.sql, ec);
        exit(1);
    }

    ec = STEP(stmt_q4);
    if (ec = SQLITE_OK) {
        APPERR("step", stmt_q4.sql, ec);
        exit(1);
    }

    ec = FINALIZE(begin);
    if (ec = SQLITE_OK) {
        APPERR("finalize", stmt_q4.sql, ec);
        exit(1);
    }

    detach("analytic");
    detach("timeSeries");
}

#define SQL_TIMESERIESMETADATA "select rowid, tsmd_name, tsmd_rowcnt, tsmd_processed from TimeSeriesMetaData where tsmd_name like '%2008%'"
stmt stmt_timeseriesmetadata;
void update(int pass)
{
    char *tsmd_name;
    int tsmd_rowcnt, tsmd_processed, tsmd_rowid, ec;

    stmt_timeseriesmetadata.sql = SQL_TIMESERIESMETADATA;

    ec = PREPARE(adb, stmt_timeseriesmetadata);
    if (ec != SQLITE_OK) {
        APPERR("prepare", stmt_timeseriesmetadata.sql, ec);
        exit(1);
    }

    do {

        ec = STEP(stmt_timeseriesmetadata);
        if (ec == SQLITE_ROW) {
            /*
             *     0          1            2               3
             * rowid, tsmd_name, tsmd_rowcnt, tsmd_processed
             */
            tsmd_rowid = sqlite3_column_int(stmt_timeseriesmetadata.stmtp, 0);
            tsmd_name = (char *)sqlite3_column_text(stmt_timeseriesmetadata.stmtp, 1);
            tsmd_rowcnt = sqlite3_column_int(stmt_timeseriesmetadata.stmtp, 2);
            tsmd_processed = sqlite3_column_int(stmt_timeseriesmetadata.stmtp, 3);

            printf("Processing rowid %d name %s rowcnt %d processed %d\n",
                    tsmd_rowid, tsmd_name, tsmd_rowcnt, tsmd_processed);


            switch(pass) {

                case 1:
                    update_day_pass1(tsmd_name);
                    break;
                case 2:
                    begin_transaction();
                        update_day_pass2(tsmd_name);
                    end_transaction();
                    break;
                case 3:
                    update_day_pass3(tsmd_name);
                    break;
                default:
                    printf("invalude pass number %d\n", pass);
                    exit(1);
            }
        }

    } while (ec == SQLITE_ROW);

    if (ec != SQLITE_DONE) {
        APPERR("step", stmt_timeseriesmetadata.sql, ec);
        exit(1);
    }

    ec = FINALIZE(stmt_timeseriesmetadata);
    if (ec != SQLITE_OK) {
        APPERR("finalize", stmt_timeseriesmetadata.sql, ec);
        exit(1);
    }

}


int main(int argc, char *argv[])
{
    int ec,  pass;


    if (argc < 2)  {
        printf("usuage ./loader <digit>\n");
        exit(1);
    }

    pass = atoi(argv[1]);

    printf("pass argument %d\n", pass);

    /* open ananysis and time series databases
    */
    if ( (ec = sqlite3_open(analyticDB, &adb)) != SQLITE_OK ) {
        APPERR("open database", analyticDB, ec);
        exit(1);
    }

    if ( (ec = sqlite3_open(timeSeriesDB, &tdb)) != SQLITE_OK ) {
        APPERR("open database", timeSeriesDB, ec);
        exit(1);
    }

    update(pass);

    CLOSE(adb);
    CLOSE(tdb);
}
