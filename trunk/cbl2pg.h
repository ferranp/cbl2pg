#ifndef _DLL_H_
#define _DLL_H_

//#if BUILDING_DLL
# define DLLIMPORT __declspec(dllexport)
//#else /* Not BUILDING_DLL */
//# define DLLIMPORT __declspec(dllimport)
//#endif /* Not BUILDING_DLL */

#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <time.h>

#include "libpq-fe.h"
/*
   sqlca :
*/

#define SQLERRMC_LEN	70

struct sqlca
{
	char		sqlcaid[8];
	long		sqlabc;
	long		sqlcode;
	struct
	{
		short int	sqlerrml;
		char		sqlerrmc[SQLERRMC_LEN];
	}			sqlerrm;
	char		sqlerrp[8];
	long		sqlerrd[6];
	// Element 0: empty
	// 1: OID of processed tuple if applicable
	// 2: number of rows processed
	// after an INSERT, UPDATE or
	// DELETE statement
	// 3: empty
	// 4: empty
	// 5: empty
	char		sqlwarn[8];
	// Element 0: set to 'W' if at least one other is 'W'
	// 1: if 'W' at least one character string
	// value was truncated when it was
	// stored into a host variable.

	//
	// 2: if 'W' a (hopefully) non-fatal notice occured
	// 3: empty
	// 4: empty
	// 5: empty
	// 6: empty
	// 7: empty

	char		sqlext[8];
};
/*
    Longitud : 130

*/

struct format_camp {
    char nom[20];
    char len[3];
    char dec[2];
    char key;
    char tipo;
};

#define MAX_CONNECTIONS		100

DLLIMPORT void sql_debug_on (void);
DLLIMPORT void sql_debug_off (void);

DLLIMPORT void sql_trace_on (void);
DLLIMPORT void sql_trace_off (void);

DLLIMPORT int sql_connect(PGconn **conn,char *db);
DLLIMPORT int sql_disconnect(PGconn **conn);

DLLIMPORT int sql_error_text(PGconn **conn,char *text);

DLLIMPORT int sql_query(PGconn **conn,PGresult **res,struct sqlca *ca,char *sql);
DLLIMPORT int sql_query_free(PGresult **res);

DLLIMPORT int sql_get_item(PGresult **res,int *lin,int *col,char *value);
DLLIMPORT int sql_get_item_by_name(PGresult **res,int *lin,char *colname,char *value);
DLLIMPORT int sql_get_line(PGresult **res,int *lin,char *value);
DLLIMPORT int sql_get_info(PGresult **res,int *lin,int *col,char *value);

DLLIMPORT int sql_escape(char *sql, int len);

DLLIMPORT int sql_make_update(PGconn **conn,struct sqlca *psqlca,char *data,struct format_camp *format);
DLLIMPORT int sql_make_insert(PGconn **conn,struct sqlca *psqlca,char *data,struct format_camp *format);
DLLIMPORT int sql_make_create(PGconn **conn,struct sqlca *psqlca,struct format_camp *format);

DLLIMPORT int sql_exec_file(PGconn **conn,struct sqlca *psqlca,char *filename);
#endif /* _DLL_H_ */
