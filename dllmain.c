/*
    Copyright (C) 2009  Ferran Pegueroles <ferran@pegueroles.com>
    This file is part of cbl2pg.

    cbl2pg is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    cbl2pg is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
*/
/***************************************************************************
 *                                                                         *
 ***************************************************************************/

#include "cbl2pg.h"

static int debug = 0;
static int trace = 0;
static int max_connections = 200;

static char *aplic_encoding = NULL;
static char *trace_filepath = NULL;

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
char *va_string(const char *format, va_list args)
{
    char str[10000];
    vsnprintf(str, 10000, format, args);

    int s_pos = strlen(str) - 1;
    while (s_pos > 0 && str[s_pos] == ' ') s_pos--;
    str[s_pos + 1]='\0';

    return strdup(str);
}

void va_box(const int flags, const char *title, const char *format, ...)
{
    va_list args;
    char *str;

    va_start(args, format);
    str = va_string(format, args);
    va_end(args);

    MessageBox(0, str, title, flags);
    free(str);
    return;
}

DLLIMPORT void sql_dll_compilation (void)
{
    va_box(MB_ICONINFORMATION, "Date compilation","%s %s",__DATE__,__TIME__);
    return;
}

DLLIMPORT void sql_print_encoding (PGconn **conn)
{
    /* UTF8 o LATIN9 */
    va_box(MB_ICONINFORMATION, "sql_get_encoding", PQparameterStatus (*conn, "client_encoding"));
    return;
}

DLLIMPORT int sql_error_text(PGconn **conn,char *text)
{
    strncpy(text,PQerrorMessage(*conn),70);
    return 0;
}

void debug_box(int level, const char *title, const char *format, ...)
{
    va_list args;
    char *str;
    int flags;

    if (level > debug) {
       return;
    }

    va_start(args, format);
    str = va_string(format, args);
    va_end(args);

    str = realloc(str, strlen(str) + 50 * sizeof(char));
    strcat(str,"\n\nContinue debugging?");
    if (MessageBox(0, str, title, MB_ICONINFORMATION|MB_OKCANCEL) == IDCANCEL)
        sql_debug_off();
    free(str);
    return;
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/

DLLIMPORT void sql_set_trace_filepath (const char *filepath)
{
    if (trace_filepath) {
        free(trace_filepath);
        trace_filepath = NULL;
    }
    if (!filepath){
        debug_box(90, "sql_set_trace_filepath","remove trace filepath");
        return;
    }
    trace_filepath=calloc(100,sizeof(char));
    memcpy(trace_filepath,filepath,strcspn(filepath," "));
}

void sql_trace(const char *format, ...)
{
    if(!debug && !trace) return;

    if (!trace_filepath) {
        sql_set_trace_filepath("cbl2pg.log");
    }
    FILE *f = fopen(trace_filepath,"a+");
    if (!f) return;

    va_list args;
    char *str;

    va_start(args, format);
    str = va_string(format, args);
    va_end(args);

    time_t t = time(NULL);
    fprintf(f,"%s [%d] %s %s\n", strtok(ctime(&t), "\n"), getpid(), getenv("USERNAME"), str);
    fclose(f);
    free(str);
    return;
}

DLLIMPORT void sql_trace_on (void)
{
    if (trace) return;
    trace = 1;
    sql_trace("# Begin trace");
    return;
}

DLLIMPORT void sql_trace_off (void)
{
    if (!trace) return;
    sql_trace("# End trace");
    trace = 0;
    return;
}

DLLIMPORT void ext_trace (const char *text)
{
    int bak_trace = trace;
    trace = 1;
    sql_trace("EXT >>> %s", text);
    trace = bak_trace; 
    return;
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/

DLLIMPORT void sql_set_debug (const char *level)
{
    sql_trace("# end debug %d", debug);
    debug = atoi(level);
    sql_trace("# start debug %d", debug);
}

DLLIMPORT void sql_debug_on (void)
{
    if (debug) return;
    sql_set_debug("30");
    return;
}

DLLIMPORT void sql_debug_off (void)
{
    if (!debug) return;
    sql_set_debug("0");
    return;
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT void sql_set_client_encoding (const char *encoding)
{
    if (aplic_encoding){
        free(aplic_encoding);
        aplic_encoding = NULL;
    }
    if (!encoding){
        debug_box(90, "sql_set_client_encoding","remove client encoding");
        return;
    }
    aplic_encoding=calloc(100,sizeof(char));
    memcpy(aplic_encoding,encoding,strcspn(encoding," "));
}

DLLIMPORT void sql_charset_conversion_on (void)
{
    sql_set_client_encoding("ISO-8859-15");
    return;
}

DLLIMPORT void sql_charset_conversion_off (void)
{
    sql_set_client_encoding(NULL);
    return;
}
/***************************************************************************
 *                                                                         *
 ***************************************************************************/

struct result_encoding {
    PGresult *res;
    PGconn *conn;
    int client_encoding;
};
struct result_encoding **results=NULL;

void print_results(const char *prefix) {
    if (!results){
       debug_box(70,"print_results","%s\nno result encodings", prefix);
       return;
    }

    struct result_encoding *re;
    int n;
    char txt[100];
    char msg[8100];
    msg[0]='\0';

    for (n=0;results[n];n++) {
        re = results[n];
        sprintf(txt,"re %X result %X conn %X encoding %d\n",re, re->res, re->conn, re->client_encoding);
        strcat(msg,txt);
        if (strlen(msg) > 8000){
            strcat(msg,"etc.");
            break;
        }
    }
    debug_box(70,"print_results","%s\n%s", prefix, msg);
    return;
}

void set_result_encoding (PGresult *res, PGconn *conn){
    struct result_encoding *re;

    if (!results){
        results = malloc(sizeof(struct result_encoding *));
        results[0] = NULL;
    }

    int n = 0;
    while ((results[n]) && (results[n]->res != res)){
        n++;
    }
    if (results[n]){
        re = results[n];
    }else{
        results = realloc(results, (n + 2) * sizeof(struct result_encoding *));
        re = malloc(sizeof(struct result_encoding));
        results[n] = re;
        results[n + 1] = NULL;
    }

    re->res = res;
    re->conn = conn;
    re->client_encoding = PQclientEncoding(conn);

    if (debug) print_results("set_result_encoding");
    return;
}

int get_result_encoding (PGresult *res){
    int n;
    for (n=0;results[n];n++) {
        if (results[n]->res == res){
            return results[n]->client_encoding;
        }
    }
    return 0;
}

void free_result_encodings (void *pref){
    int n,m;
    for (n=0,m=0; results[n]; n++){
        if ((results[n]->res == (PGresult *) pref) ||
            (results[n]->conn == (PGconn *) pref)) {
           free(results[n]);
        } else {
           results[m] = results[n];
           m++;
        }
    }
    results = realloc(results,(m + 1) * sizeof(struct result_encoding *));
    results[m]=NULL;
    if (debug) print_results("free_result_encodings");
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/

PGconn **connections=NULL;

DLLIMPORT void sql_set_max_connections (const char *max_str)
{
    max_connections = atoi(max_str);
    sql_trace("# set max connections %d", max_connections);
}

void store_connection(PGconn *conn) {
    if (!connections){
        connections = malloc(sizeof(PGconn *));
        connections[0] = NULL;
    }
    int n = 0;
    for (n=0; connections[n]; n++);
    connections = realloc(connections, (n + 2) * sizeof(PGconn *));
    connections[n] = conn;
    connections[n + 1] = NULL;
    if (n >= max_connections) {
       va_box(MB_ICONWARNING, "sql_connect","Max connections %d\nCurrent connection %d",
                               max_connections, (n + 1));
    };
    sql_trace("%X # Connect (%d current connections)", conn, n + 1);
    return;
}


void remove_connection(PGconn *conn) {
    int n,m;
    for (n=0,m=0; connections[n]; n++){
        if (connections[n] != conn){
           connections[m] = connections[n];
           m++;
        }
    }
    connections = realloc(connections,(m + 1) * sizeof(PGconn *));
    connections[m]=NULL;
    sql_trace("%X # Disconnect (%d current connections)", conn, m);
    return;
}

void print_connections(const char *prefix) {
    if (!connections){
       debug_box(40,"print_connections","%s\nno connections started",prefix);
       return;
    }

    int n;
    char txt[100];
    char msg[8100];
    msg[0]='\0';

    for (n=0; connections[n]; n++) {
        sprintf(txt,"connection %d %X\n",(n + 1), connections[n]);
        strcat(msg,txt);
        if (strlen(msg) > 8000){
            strcat(msg,"etc.");
            break;
        }
    }
    if (!n){
       debug_box(40,"print_connections","%s\nno connections",prefix);
       return;
    }
    debug_box(40,"print_connections","%s\n%s",prefix,msg);
    return;
}

void sql_trace_connections(const char *prefix) {
    if (!connections){
       sql_trace("# [CONNECTIONS] %s: no connections started", prefix);
       return;
    }

    int n;
    for (n=0; connections[n]; n++) {
        sql_trace("# [CONNECTIONS] %s: %X", prefix, connections[n]);
    }
    if (!n){
        sql_trace("# [CONNECTIONS] %s: no connections", prefix);
    }
    return;
}

DLLIMPORT int sql_connect(PGconn **conn,char *db)
{
    // db es string de conexio pic x(200), ho finalitzem amb un null
    db[199]='\0';

    if (debug) print_connections("sql_connect");

    *conn = PQconnectdb(db);
    //*conn = PQconnectStart(db);

    if (*conn == NULL){
        return -1;
    }
    while (1){
        if (PQstatus(*conn) == CONNECTION_BAD) break;
        if (PQstatus(*conn) == CONNECTION_OK) break;
    }
    if (PQstatus(*conn) == CONNECTION_BAD){
        return -1;
    }

    /* Keep connection for disconnecting after */
    store_connection(*conn);
   
    if (aplic_encoding) {
        PQsetClientEncoding(*conn,aplic_encoding);
    }
    
    return 0;
}

DLLIMPORT int sql_disconnect(PGconn **pgconn)
{
    PGconn *conn = *pgconn;
    remove_connection(conn);

    if (debug) print_connections("sql_disconnect");

    free_result_encodings(conn);
    PQfinish(conn);
    return 0;
}

void close_open_connections(){
    debug_box(60,"close_open_connections","Close %d connections", max_connections);
    while (connections[0]) {
        sql_disconnect(&connections[0]);
    }
}

DLLIMPORT void sql_disconnect_all() {
    close_open_connections();
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/


struct iconv_group {
  unsigned int client_encoding;
  char *encoding;
  iconv_t pg2cp;
  iconv_t cp2pg;
};
struct iconv_group **iconv_group_set = NULL;

void print_iconv_group_set(const char *prefix) {
    if (!iconv_group_set){
       debug_box(80, "print_iconv_group_set", "%s\nno iconv groups", prefix);
       return;
    }

    struct iconv_group *ig;
    int n;
    char txt[100];
    char msg[8100];
    msg[0]='\0';

    for (n=0;iconv_group_set[n];n++){
        ig = iconv_group_set[n];
        sprintf(txt,"ig %X encoding %d %s pg2cp %X cp2pg %X\n",
                 ig, ig->client_encoding, ig->encoding, ig->pg2cp, ig->cp2pg);
        strcat(msg,txt);
        if (strlen(msg) > 8000){
            strcat(msg,"etc.");
            break;
        }
    }
    debug_box(80,"print_iconv_group_set","%s\n%s", prefix, msg);
    return;
}

struct iconv_group *new_iconv_group (int client_encoding){
    const char *ICONV_SUFFIX = "//IGNORE";
    //const char *ICONV_SUFFIX = "//TRANSLIT";
    //const char *ICONV_SUFFIX = "";

    struct iconv_group *ig;
    char tocode[30];

    ig = malloc(sizeof(struct iconv_group));
    ig->client_encoding=client_encoding;
    ig->encoding = calloc(30,sizeof(char));
    strcpy(ig->encoding,pg_encoding_to_char(client_encoding));
    if (!strcmp(ig->encoding,"UTF8"))
        strcpy(ig->encoding,"UTF-8");
    if (!strcmp(ig->encoding,"LATIN9"))
        strcpy(ig->encoding,"LATIN-9");

    sprintf(tocode,"CP850%s",ICONV_SUFFIX);
    ig->pg2cp=iconv_open(tocode,ig->encoding);
    if (ig->pg2cp < 0){
        va_box(MB_ICONERROR,"new_iconv_group","Error pg2cp");
        return NULL;
    }

    sprintf(tocode,"%s%s",ig->encoding,ICONV_SUFFIX);
    ig->cp2pg=iconv_open(tocode,"CP850");
    if (ig->cp2pg < 0){
        va_box(MB_ICONERROR,"new_iconv_group","Error cp2pg");
        return NULL;
    }
    return ig;
}

struct iconv_group *get_iconv_group (int client_encoding) {
    if (!client_encoding){
        return NULL;
    }

    if (!iconv_group_set){
        iconv_group_set = malloc(sizeof(struct iconv_group *));
        iconv_group_set[0] = NULL;
    }

    int n;
    for (n=0; iconv_group_set[n]; n++){
        if (iconv_group_set[n]->client_encoding == client_encoding){
            return iconv_group_set[n];
        }
    }

    iconv_group_set[n] = new_iconv_group(client_encoding);
    iconv_group_set = realloc(iconv_group_set, (n + 2) * sizeof(struct iconv_group *));
    iconv_group_set[n + 1] = NULL;

    if (debug) print_iconv_group_set("new_iconv_group");
    return iconv_group_set[n];
}

int pg2cp_iconv(PGresult *res, char *src, int src_sz, char *dst, int dst_sz) {
    struct iconv_group *ig = get_iconv_group(get_result_encoding(res));

    return do_iconv((ig)? ig->pg2cp : NULL, src, src_sz, dst, dst_sz);
}


int cp2pg_iconv(PGconn *conn, char *src, int src_sz, char *dst, int dst_sz) {
    struct iconv_group *ig = get_iconv_group(PQclientEncoding(conn));

    return do_iconv((ig)? ig->cp2pg : NULL, src, src_sz, dst, dst_sz);
}


int do_iconv(iconv_t pd, const char *src, int src_sz, char *dst, int dst_sz) {
    if (!pd){
        memcpy(dst, src, src_sz);
        return src_sz;
    }
    int no_rv_chrs;
    int mem_sz=dst_sz;
    
    no_rv_chrs = iconv(pd, &src, &src_sz, &dst, &dst_sz);
    
    if (no_rv_chrs < 0){
        va_box(MB_ICONERROR, "do_iconv","Error %d: %s\nsrc: %s\nsrc_sz: %d\ninit dst_sz: %d\ndst_sz: %d",
                    errno, strerror(errno), src, src_sz, dst_sz, mem_sz);
    }
    return (mem_sz - dst_sz);
}


/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT int sql_create_field_table(PGconn **conn,struct format_camp *format){
    char sql[100];
    char table[30];
    char tmps[10];
    int columns,col;
    //int size;
    int oid,mod;

    int digits,decimals;
    //int i;

    PGresult *res;
    if (conn == NULL){
        va_box(MB_ICONERROR, "sql_create_field_table", "PGconn == NULL");
        return -1 ;
    }
    if (*conn == NULL){
        va_box(MB_ICONERROR, "sql_create_field_table", "*PGconn == NULL");
        return -1 ;
    }
    memcpy ( table, format[0].nom, 27 );
    table[27] = 0;
    snprintf(sql,100,"select * from %s where 1=0", table);

    PQsetnonblocking(*conn,1);
    if (PQstatus(*conn) != CONNECTION_OK){
        va_box(MB_ICONERROR, "sql_create_field_table", "BAD CONNECTION");
        PQreset(*conn);
    }
    res=PQexec(*conn,sql);
    switch(PQresultStatus(res)){
        case PGRES_TUPLES_OK:         // ok select
            break;
        case PGRES_EMPTY_QUERY:       // ERROR
        case PGRES_COMMAND_OK:        // ok update o insert o delete
        case PGRES_COPY_OUT:          // copy cap al server
        case PGRES_COPY_IN:           // copy des del server
        case PGRES_BAD_RESPONSE:      // ERROR
        case PGRES_NONFATAL_ERROR:    // ERROR
        case PGRES_FATAL_ERROR:       // ERROR
        default:
            va_box(MB_ICONERROR, "sql_create_field_table", "ERROR : sql_create_field_table");
            PQclear(res);
            return -1;
            break;
    }

    columns = PQnfields(res);
    if (columns < 1)
        return -1;

    for(col=0;col<columns;col++){
        memcpy(format[col+1].nom, PQfname(res,col),strlen(PQfname(res,col)));
        oid=PQftype(res,col);
        mod=PQfmod(res,col);
        //size=PQfsize(res,col);
        switch(oid){
            case 1700: // numeric
                digits= ((mod - 4) >> 16) ; //-4
                decimals=((mod - 4) & 0x0000ffff);
                if (mod == -1) {
                    digits = 14;
                    decimals = 2;
                }
                snprintf(tmps,10,"%03d",digits + 1);
                memcpy(format[col+1].len,tmps,3);
                snprintf(tmps,10,"%02d",decimals);
                memcpy(format[col+1].dec,tmps,2);
                format[col+1].tipo='N';
                break;
            case 21: // int2 s9(4) sts
                memcpy(format[col+1].len,"005",3);
                memcpy(format[col+1].dec,"00",2);
                format[col+1].tipo='N';
                break;
            case 23: // int4 s9(9) sts
                memcpy(format[col+1].len,"010",3);
                memcpy(format[col+1].dec,"00",2);
                format[col+1].tipo='N';
                break;
            case 20: // int8    s9(18) sts
                memcpy(format[col+1].len,"019",3);
                memcpy(format[col+1].dec,"00",2);
                format[col+1].tipo='N';
                break;
            case 1042: // char (x)
                digits = mod - 4;
                snprintf(tmps,10,"%03d",digits);
                memcpy(format[col+1].len,tmps,3);
                memcpy(format[col+1].dec,"00",2);
                format[col+1].tipo='C';
                break;
            case 1043: // varchar (x)
                //snprintf(tmp2,200,"Col:%d, oid:%d, mod:%d, size:%d , len:%d, valor<%s>",
                //   col,oid,mod,size,len,c);

                digits = mod - 4;
                snprintf(tmps,10,"%03d",digits);
                memcpy(format[col+1].len,tmps,3);
                memcpy(format[col+1].dec,"00",2);
                format[col+1].tipo='C';
                break;
            case 25: // text
                digits = mod - 4;
                snprintf(tmps,10,"%03d",digits);
                memcpy(format[col+1].len,tmps,3);
                memcpy(format[col+1].dec,"00",2);
                format[col+1].tipo='C';
                break;
            case 1082: // date
                memcpy(format[col+1].len,"008",3);
                memcpy(format[col+1].dec,"00",2);
                format[col+1].tipo='D';
                break;
            case 1083: // time
                // paseem de format hh:mm:ss a hhmmss
                memcpy(format[col+1].len,"006",3);
                memcpy(format[col+1].dec,"00",2);
                format[col+1].tipo='T';
                break;
            case 1184:
            case 1114: // timestamp aaaa-mm-dd hh:mm:ss mmmmmm -> ddmmaaaahhmmssmmmmmm
                memcpy(format[col+1].len,"020",3);
                memcpy(format[col+1].dec,"00",2);
                format[col+1].tipo='S';
                break;
            default: // altres, es copies les primeres 30 posicions del resultat
                va_box(MB_ICONERROR, "sql_create_field_table", "Unknown field type");
                break;
        }
    }
    format[columns + 1].nom[0]=0;
    PQclear(res);
    return 0;
}


/***************************************************************************
 *                                                                         *
 ***************************************************************************/
int sql_query_raw(PGconn **conn,PGresult **res,struct sqlca *psqlca,char *sql)
{
    struct sqlca ca;

    if (conn == NULL){
        va_box(MB_ICONERROR, "sql_query", "PGconn == NULL");
        return -1 ;
    }
    if (*conn == NULL){
        va_box(MB_ICONERROR, "sql_query", "*PGconn == NULL");
        return -1 ;
    }
    PQsetnonblocking(*conn,1);

    if (PQstatus(*conn) != CONNECTION_OK){
        va_box(MB_ICONERROR, "sql_query", "BAD CONNECTION");
        PQreset(*conn);
    }

    *res=PQexec(*conn,sql);

    debug_box(20,"sql_query",sql);
    sql_trace("%X %s", *conn, sql);

    memset(&ca,'\0',sizeof(struct sqlca));

    switch(PQresultStatus(*res)){
        case PGRES_EMPTY_QUERY:       // ERROR
            ca.sqlcode=-200;
            break;
        case PGRES_COMMAND_OK:        // ok update o insert o delete
            ca.sqlcode=0;
            if ((PQntuples(*res) == 0) &&
                    (strtol(PQcmdTuples(*res),NULL, 0) == 0))
                ca.sqlcode=100;
            break;
        case PGRES_TUPLES_OK:         // ok select
            ca.sqlcode=0;
            if (PQntuples(*res) == 0)
                ca.sqlcode=100;
            break;
        case PGRES_COPY_OUT:          // copy cap al server
            ca.sqlcode=-100;
            break;
        case PGRES_COPY_IN:           // copy des del server
            ca.sqlcode=-100;
            break;
        case PGRES_BAD_RESPONSE:      // ERROR
            ca.sqlcode= -200;
            break;
        case PGRES_NONFATAL_ERROR:    // ERROR
            ca.sqlcode= -200;
            break;
        case PGRES_FATAL_ERROR:       // ERROR
            ca.sqlcode= -200;
            break;
        default:
            ca.sqlcode= -300;
            break;
    }

    ca.sqlerrm.sqlerrml=ca.sqlcode;

    strncpy (ca.sqlerrm.sqlerrmc,PQresultErrorMessage(*res),70);
    if(ca.sqlcode < 0){
        debug_box(20, "sql_query_raw", "Error : %s", PQresultErrorMessage(*res));
        sql_trace("%X SQLERROR %s", *conn, strtok(PQresultErrorMessage(*res), "\n"));
    }
    ca.sqlerrd[0]=0;
    ca.sqlerrd[1]=PQoidValue(*res);
    ca.sqlerrd[2]=strtol(PQcmdTuples(*res),NULL, 0);
    ca.sqlerrd[3]=PQntuples(*res);
    ca.sqlwarn[0]=ca.sqlwarn[1]=' ';
    memcpy(psqlca,&ca,sizeof(struct sqlca));

    set_result_encoding(*res,*conn);

    return ca.sqlcode;
}

DLLIMPORT int sql_query(PGconn **conn,PGresult **res,struct sqlca *psqlca,char *sql)
{
    char pg_enc_str[30000];
    int pg_len;
    int cp_len;

    for (cp_len=strlen(sql); cp_len > 1 && sql[cp_len - 1] == ' '; cp_len--);
    pg_len = cp2pg_iconv(*conn, sql, cp_len, pg_enc_str, sizeof(pg_enc_str));
    pg_enc_str[pg_len] = '\0';

    return sql_query_raw(conn, res, psqlca, pg_enc_str);
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT int sql_query_free(PGresult **res){
    if (*res){
        free_result_encodings(*res);
        PQclear(*res);
        *res=NULL;
    }

    return 0;
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
int format_item(PGresult *res,int lin,int col,char *result){

    int oid,mod;
    long int inum;
    int i;
    long long int lnum;
    // char str[200];
    int digits,decimals;
    char *c;
    double num;
    char tmp[21];
    //char tmp2[200];

    oid=PQftype(res,col);
    mod=PQfmod(res,col);
    c=PQgetvalue(res,lin,col);
    //sql_trace("format_item type:%d len:%d value:%s", oid, mod, c);

    //sql_print_info(&res,&lin,&col);
        case 21: // int2 s9(4) sts

            inum = strtol (c, NULL, 0);
            snprintf(tmp,21,"%020ld",inum);

            if (inum < 0) result[4]='-';
            else          result[4]='+';

            if (inum < 0) inum = inum * -1;

            memcpy(result,tmp + 16 ,4);

            return 5;
            break;

    // taula de oid a
    switch(oid){
        case 1700: // numeric
            digits= ((mod - 4) >> 16) ; //-4
            decimals=((mod - 4) & 0x0000ffff);
            if (mod == -1) {
                digits = 14;
                decimals = 2;
            }
            //sprintf(str,"mod:%d , numeric , len,dec:%d,%d ",
            //   mod, digits, decimals) ;
            //MessageBox (0, str, c, MB_ICONINFORMATION);

            // la longitud del camp es digits i el signe al final

            num=strtod (c, NULL);
            // multipliquem per 10**decimals per a treure el punt
            num = num * pow(10,decimals);
            // posem el signe al final
            if (num < 0) result[digits]='-';
            else         result[digits]='+';
            // traiem el signe
            if (num < 0) num = num * -1;
            // formatejem
            snprintf(tmp,21,"%020.0f",num);
            // Pose e a 0 els espais inicials
            for (i=0;i<21;i++)
            {
                if (tmp[i] == ' ') tmp[i] = '0';
            }
            tmp[20]=0;
            //snprintf(tmp2,200,"num: %f dig: %d dec: %d",num,digits,decimals);
            //MessageBox (0, tmp2, tmp, MB_ICONINFORMATION);
            memcpy(result,tmp + 20 - digits ,digits);
            return (digits + 1);
            break;
        case 16: // boolean
            if (PQgetisnull(res,lin,col)){
                
               return 1;
               break;
            }

            

            inum = strtol (c, NULL, 0);
            snprintf(tmp,21,"%020ld",inum);

            if (inum < 0) result[4]='-';
            else          result[4]='+';

            if (inum < 0) inum = inum * -1;

            memcpy(result,tmp + 16 ,4);

            return 5;
            break;
        case 21: // int2 s9(4) sts

            inum = strtol (c, NULL, 0);
            snprintf(tmp,21,"%020ld",inum);

            if (inum < 0) result[4]='-';
            else          result[4]='+';

            if (inum < 0) inum = inum * -1;

            memcpy(result,tmp + 16 ,4);

            return 5;
            break;
        case 23: // int4 s9(9) sts

            inum = strtol (c, NULL, 0);
            snprintf(tmp,21,"%020ld",inum);

            if (inum < 0) result[9]='-';
            else          result[9]='+';

            if (inum < 0) inum = inum * -1;

            memcpy(result,tmp + 11 ,9);

            return 10;
            break;
        case 20: // int8    s9(18) sts
            lnum = strtoll (c, NULL, 0);
            snprintf(tmp,21,"%020I64d",lnum);

            if (lnum < 0) result[18]='-';
            else          result[18]='+';

            if (lnum < 0) inum = lnum * -1;

            memcpy(result,tmp + 2,18);

            return 19;
            break;
        case 1042: // char (x)
            digits = (mod == -1) ? strlen(c) : mod - 4;
            memset(result,' ',digits);
            if (strlen(c)) 
               pg2cp_iconv(res, c, strlen(c), result, digits);
            return digits;
            
        case 1043: // varchar (x)
            digits = (mod == -1) ? strlen(c) : mod - 4;
            memset(result,' ',digits);
            if (strlen(c)) 
               pg2cp_iconv(res, c, strlen(c), result, digits);
            return digits;

        case 25: // text
            digits = strlen(c);
            if (strlen(c)) 
               pg2cp_iconv(res, c, strlen(c), result, digits);
            return digits;

        case 1082: // date
            // paseem de format aaaa-mm-dd a ddmmaaaa
            if (strlen(c)!=0) {
                if (strncmp(c,"0001-01-01",10)==0){
                    memcpy(result,"00000000",8);
                    return 8;
                }
                memcpy(result + 4,c    ,4); // any
                memcpy(result + 2,c + 5,2); // mes
                memcpy(result    ,c + 8,2); // dia
            }
            return 8;
            break;
        case 1083: // time
            // paseem de format hh:mm:ss a hhmmss
            if (strlen(c)!=0) {
                memcpy(result    ,c    ,2); // hora
                memcpy(result + 2,c + 3,2); // min
                memcpy(result + 4,c + 6,2); // seg
            }
            return 6;
            break;
        case 1184:
        case 1114: // timestamp aaaa-mm-dd hh:mm:ss mmmmmm -> ddmmaaaahhmmssmmmmmm
            if (strlen(c)!=0) {
                memset(result,'0',20);
                memcpy(result +  4,c     ,4); // aaaa
                memcpy(result +  2,c +  5,2); // mm
                memcpy(result     ,c +  8,2); // dd
                memcpy(result +  8,c + 11,2); // hh
                memcpy(result + 10,c + 14,2); // mm
                memcpy(result + 12,c + 17,2); // ss
                memcpy(result + 14,c + 20,min(6, strlen(c) - 20)); // mmmmmm
            }
            return 20;

        default: // altres, es copies les primeres 30 posicions del resultat
            memcpy(result,c,min(30,strlen(c)));
            va_box(MB_ICONWARNING, "format_item", "tipus de camp desconegut\n%d,val<%s>", oid, c);
            return min(30,strlen(c));
            break;
    }

    return 0;
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT int sql_get_item(PGresult **res,int *lin,int *col,char *value){
    int linea,columna;
    linea=(signed int)*lin;
    columna=(signed int)*col;

    return format_item(*res,(unsigned int)linea,(unsigned int)columna,value);
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT int sql_get_item_by_name(PGresult **res,int *lin,char *colname,char *value){
    int col;
    col=PQfnumber(*res,colname);
    if (col<0) return -1;
    return format_item(*res,*lin,col,value);
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT int sql_get_line(PGresult **res,int *lin,char *value){
    char valors[8000];
    int columns,i,len,linea;
    char *p;

    valors[0]='\0';
    p=value;
    linea=(signed int)*lin;

    /* falta libpq.h i pg_whcar.h */
    
    columns = PQnfields(*res);
    if (columns < 0)
        return columns;
    if (*lin >= PQntuples(*res)){
        return PQntuples(*res);
    }
    for(i=0;i<columns;i++){
        len = format_item(*res,(unsigned int)linea,i,p);
        
        if (debug){
            strncat(valors,p,len);
            strcat(valors,"\n");
            //sql_trace("column %d len %d", i, len);
        }
        p+=len;
    }
    debug_box(30, "sql_get_line", valors);
    return 0;

}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT int sql_get_info(PGresult **res,int *lin,int *col,char *value){
    char *c,str[1000];
    int oid,size,mod,len;
    oid=PQftype(*res,*col);
    mod=PQfmod(*res,*col);
    size=PQfsize(*res,*col);
    len=PQgetlength(*res,*lin,*col);
    //name=PQfname(*res,*col);
    c=PQgetvalue(*res,*lin,*col);
    /*void PQprint(FILE *fout,  // output stream 
             const PGresult *res,
             const PQprintOpt *po);*/
    switch(oid){
        case 1700:
            sprintf(str,"col:%d , numeric , len,dec:%d,%d ",
                    *col, ((mod - 4) & 0xffff0000) >> 16, ((mod - 4) & 0x0000ffff)) ;
            break;
        default:
            sprintf(str,"col: %d\noid: %d\nmod: %d\nsize: %d\nlen: %d\nvalue: %s\0",
                    *col,oid,mod,size,len,c);
            break;
    }

    strcpy(value,str);
    return 0;
}

DLLIMPORT int sql_print_info(PGresult **res,int *lin,int *col){
    char str[1000];
    
    sql_get_info(res,lin,col,str);
    va_box(MB_ICONINFORMATION, "sql_print_info", str);
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT int sql_escape(char *sql, int len){
    char *sqlto;
    sqlto=malloc(len + 1);
    PQescapeString(sqlto,sql,len);
    memcpy(sql,sqlto,len);
    free(sqlto);
    return 0;
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
int formatejar_camp(PGconn *conn, char *sql, char *pdata, struct format_camp *pformat, char *valors){
    char tmp[20];
    char pg_enc_str[1000];
    char pg_esc_str[1000];
    int pg_len;
    int len,dec;
    int i;
    char *ini_sql = strrchr(sql,'\0');

    strncpy(tmp,pformat->len,3);
    tmp[3]=0;
    len = strtol(tmp,NULL,10);

    strncpy(tmp,pformat->dec,2);
    tmp[2]=0;
    dec = strtol(tmp,NULL,10);

    switch(pformat->tipo){
        case 'C':
            pg_len = cp2pg_iconv(conn, pdata, len, pg_enc_str, sizeof(pg_enc_str));
            PQescapeStringConn(conn,pg_esc_str,pg_enc_str,pg_len, NULL);
            for (i=strlen(pg_esc_str) - 1; i > 0 && pg_esc_str[i] == ' '; i--) {
                pg_esc_str[i]='\0';
            }
            strcat(sql,"'");
            strcat(sql,pg_esc_str);
            strcat(sql,"'");
            break;
        case 'D':
            if ((strncmp(pdata,"00000000",8)==0) || (strncmp(pdata,"        ",8)==0)) {
                strcat(sql,"'0001-01-01'");
                break;
            }
            strcat(sql,"'");
            strncat(sql,pdata+4,4);
            strcat(sql,"-");
            strncat(sql,pdata+2,2);
            strcat(sql,"-");
            strncat(sql,pdata,2);
            strcat(sql,"'");
            break;
        case 'N':
            pg_esc_str[0]=pdata[len - 1];
            pg_esc_str[1]=0;
            strncat(pg_esc_str,pdata,len - dec - 1);
            pg_esc_str[len - dec]=0;
            if ( dec > 0 ) {
                strcat(pg_esc_str,".");
                strncat(pg_esc_str,pdata + len - dec - 1,dec);
            }
            strcat(sql,"'");
            strcat(sql,pg_esc_str);
            strcat(sql,"'");
            break;
        case 'T':
            strcat(sql,"'");
            strncat(sql,pdata,2);
            strcat(sql,":");
            strncat(sql,pdata+2,2);
            strcat(sql,":");
            strncat(sql,pdata+4,2);
            strcat(sql,"'");
            break;
        case 'S':
            if ((strncmp(pdata,"00000000",8)==0) || (strncmp(pdata,"        ",8)==0)) {
                strcat(sql,"'00010101'");
                break;
            }
            strcat(sql,"'");
            strncat(sql,pdata+4,4);
            strncat(sql,pdata+2,2);
            strncat(sql,pdata,2);
            if (strncmp(pdata+8," ",1)==0){
                strcat(sql,"'");
                break;
            }
            strcat(sql," ");
            strncat(sql,pdata+8,6);
            if (strncmp(pdata+14," ",1)==0) {
                strcat(sql,"'");
                break;
            }
            strcat(sql,".");
            strncat(sql,pdata+14,6);
            strcat(sql,"'");
            break;
    }
    if (debug) {
        strncat(valors,pformat->nom,20);
        strcat(valors,"=");
        strcat(valors,ini_sql);
        strcat(valors,"\n");
    }
    return len;
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
int asignar_camp(PGconn *conn, char *sql,char *pdata,struct format_camp *pformat, char *valors){
    char tmp[50];
    int len;
    int long_nom;

    strncpy(tmp,pformat->len,3);
    tmp[3]=0;
    len = strtol(tmp,NULL,10);

    long_nom=strcspn(pformat->nom," ");
    if (long_nom > 20) {
        long_nom = 20;
    }

    strncat(sql,pformat->nom,long_nom);
    strcat(sql,"=");
    formatejar_camp(conn,sql,pdata,pformat,valors);
    return len;
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT int sql_make_update(PGconn **conn,struct sqlca *psqlca,char *data,struct format_camp *format){
    char valors[12000];
    char sql[12000];
    char tmp[20];
    char *pdata;
    PGresult *res;
    struct format_camp *pformat;
    int camps,i,long_nom;

    if (debug) valors[0]=0;

    // busquem quants camps hi ha (el primer es el nom de la taula)
    camps = strlen((char *)format) / sizeof(struct format_camp) - 1;

    sql[0]=0;

    // format->nom[strcspn(format->nom," ")]=0;
    //  sprintf(sql,"update %s set \0",format->nom);
    strcpy(sql,"update ");
    long_nom=strcspn(format->nom," ");
    if (long_nom > sizeof(struct format_camp)){
        long_nom=sizeof(struct format_camp);
    }
    strncat(sql,format->nom,long_nom);
    strcat(sql," set ");

    pformat=format;
    pdata=data;

    for(i=0;i< camps;i++){
        pformat++;
        if ((pformat->tipo != ' ') && (pformat->key==' '))  {
            asignar_camp(*conn,sql,pdata,pformat,valors);
            strcat(sql," , ");
        }
        strncpy(tmp,pformat->len,3);
        tmp[3]=0;
        pdata+=strtol(tmp,NULL,10);

    }
    sql[strlen(sql) - 3]=0;
    if (debug){
        debug_box(10, "sql_make_update", valors);
        valors[0]='\0';
    }

    strcat(sql," where ");
    pformat=format;
    pdata=data;
    for(i=0;i< camps;i++){
        pformat++;
        if (pformat->key=='K' || pformat->key=='k'){
            asignar_camp(*conn,sql,pdata,pformat,valors);
            strcat(sql," and ");
        }
        strncpy(tmp,pformat->len,3);
        tmp[3]=0;
        pdata+=strtol(tmp,NULL,10);
    }
    sql[strlen(sql) - 5]=0;

    i = sql_query_raw(conn,&res,psqlca,sql);
    sql_query_free(&res);
    return i;
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT int sql_make_insert(PGconn **conn,struct sqlca *psqlca,char *data,struct format_camp *format){
    char sql[12000];
    char valors[12000];
    char tmp[20];
    char *pdata;
    PGresult *res;
    struct format_camp *pformat;
    int camps,i,long_nom;

    // busquem quants camps hi ha (el primer es el nom de la taula)
    camps = strlen((char *)format) / sizeof(struct format_camp) - 1;

    if (debug) valors[0]=0;

    strcpy(sql,"insert into ");
    long_nom=strcspn(format->nom," ");
    if (long_nom > sizeof(struct format_camp)){
        long_nom=sizeof(struct format_camp);
    }
    strncat(sql,format->nom,long_nom);
    strcat(sql," (");

    pformat=format;
    for(i=0;i< camps;i++){
        pformat++;
        if ((pformat->key != 'k') && (pformat->tipo != ' ')) {
            long_nom=strcspn(pformat->nom," ");
            if (long_nom > 20) {
                long_nom = 20;
            }
            strncat(sql,pformat->nom,long_nom);
            strcat(sql,",");
        }
    }
    sql[strlen(sql) - 1]=0;
    strcat(sql,") values (");

    pformat=format;
    pdata=data;
    for(i=0;i< camps;i++){
        pformat++;
        if ((pformat->key != 'k') && (pformat->tipo != ' ')) {
            formatejar_camp(*conn,sql,pdata,pformat,valors);
            strcat(sql,",");
        }
        strncpy(tmp,pformat->len,3);
        tmp[3]=0;
        pdata+=strtol(tmp,NULL,10);
    }
    sql[strlen(sql) - 1]=0;
    strcat(sql,")");
    debug_box(10, "sql_make_insert", valors);
    i = sql_query_raw(conn,&res,psqlca,sql);
    sql_query_free(&res);
    return i;
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT int sql_make_create(PGconn **conn,struct sqlca *psqlca,struct format_camp *format){
    char sql[10000],tmp[50];
    PGresult *res;
    struct format_camp *pformat;
    int camps,i,long_nom,len,dec;

    // busquem quants camps hi ha (el primer es el nom de la taula)
    camps = strlen((char *)format) / sizeof(struct format_camp) - 1;

    strcpy(sql,"create table ");
    long_nom=strcspn(format->nom," ");
    strncat(sql,format->nom,long_nom);
    strcat(sql," (");

    pformat=format;
    for(i=0;i< camps;i++){
        pformat++;
        long_nom=strcspn(pformat->nom," ");
        strncat(sql,pformat->nom,long_nom);

        strncpy(tmp,pformat->len,3);
        tmp[3]=0;
        len = strtol(tmp,NULL,10);
        strncpy(tmp,pformat->dec,2);
        tmp[2]=0;
        dec = strtol(tmp,NULL,10);

        switch (pformat->tipo){
            case 'C':
                sprintf(tmp," char (%d) not null,",len);
                strcat(sql,tmp);
                break;
            case 'D':
                sprintf(tmp," date not null,");
                strcat(sql,tmp);
                break;
            case 'N':
                sprintf(tmp," numeric(%d,%d) not null default 0,",len - 1,dec);
                strcat(sql,tmp);
                break;
        }
    }
    strcat(sql,"Primary key (");

    pformat=format;
    for(i=0;i< camps;i++){
        pformat++;
        if (pformat->key == 'K') {
            long_nom=strcspn(pformat->nom," ");
            strncat(sql,pformat->nom,long_nom);
            strcat(sql,",");
        }
    }
    sql[strlen(sql) - 1]=0;
    strcat(sql,"))");

    debug_box(60, "sql_make_create",sql);

    i = sql_query_raw(conn,&res,psqlca,sql);
    sql_query_free(&res);
    return i;
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT int sql_exec_file(PGconn **conn,struct sqlca *psqlca,char *filepath){
    FILE *f;
    char buffer[1000];
    char *sql;
    int sql_size = 1000,size,ret;
    PGresult *res;
    debug_box(60, "Fitxer", filepath);

    f = fopen(filepath,"r");
    if (f == NULL)
        return -1;
    sql = malloc(sql_size + 1);

    while (fgets(buffer,1000,f)){
        buffer[strlen(buffer) - 1] = 0;
        size=strlen(sql) + strlen(buffer);
        if (!(size < sql_size)){
            sql_size =  size;
            sql = realloc(sql,sql_size + 1);
        }
        strcat(sql,buffer);
    }

    debug_box(60, "sql_exec_file", sql);
    ret = sql_query_raw(conn,&res,psqlca,sql);
    sql_query_free(&res);
    free(sql);
    return ret;
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
    BOOL APIENTRY
DllMain (
        HINSTANCE hInst     /* Library instance handle. */ ,
        DWORD reason        /* Reason this function is being called. */ ,
        LPVOID reserved     /* Not used. */ )
{
    switch (reason)
    {
        case DLL_PROCESS_ATTACH:
            break;

        case DLL_PROCESS_DETACH:
            // Disconnect from DB
            close_open_connections();
            break;

        case DLL_THREAD_ATTACH:
            break;

        case DLL_THREAD_DETACH:
            break;
    }

    /* Returns TRUE on success, FALSE on failure */
    return TRUE;
}
