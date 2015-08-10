// (c) 2014 DAT, all rights reserved
// Author : Peter Ogilvie
// Date   : 12/25/2014
// cfd.c  : Common Fixed Date
// Summary: SQLite custom function to allow sqlite to deal with dates with the
//          date format mm/dd/yyyy reasonably.

#include <sqlite3ext.h>
#include <stdio.h>
#include <string.h>
SQLITE_EXTENSION_INIT1;

char sql_date[11];
char cdf_date[11];

static void cdf(sqlite3_context *ctx, int num_values, sqlite3_value **values)
{
    strncpy((char *)cdf_date, (char *)sqlite3_value_text(values[0]), 11);

    // 0123456789    0123456789
    // yyyy-mm-dd <- mm/dd/yyyy

    sql_date[0] = cdf_date[6];
    sql_date[1] = cdf_date[7];
    sql_date[2] = cdf_date[8];
    sql_date[3] = cdf_date[9];
    sql_date[4] = '-';
    sql_date[5] = cdf_date[0];
    sql_date[6] = cdf_date[1];
    sql_date[7] = '-';
    sql_date[8] = cdf_date[3];
    sql_date[9] = cdf_date[4];

    sqlite3_result_text(ctx, sql_date, strlen(sql_date), SQLITE_STATIC);
}

int sqlite3_extension_init(sqlite3 *db, char **error, const sqlite3_api_routines *api)
{
    int rc = SQLITE_OK;
    SQLITE_EXTENSION_INIT2(api);

    sqlite3_create_function(db, "cdf", 1, SQLITE_UTF8, NULL, cdf, NULL, NULL);

    return rc;
}
