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

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT void sql_debug_on (void)
{
    debug = 1;
    return;
}

DLLIMPORT void sql_debug_off (void)
{
    debug = 0;
    return;
}
/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT void sql_trace_on (void)
{
    FILE *f;
    time_t t;
    trace = 1;
    f = fopen("cbl2pg.log","a+");
    if (f != NULL) {
          t = time(NULL);
          fprintf(f,"# Begin trace %s \n", ctime(&t));
          fclose(f);
    }
    return;
}

DLLIMPORT void sql_trace_off (void)
{
    trace = 0;
    return;
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
PGconn *connection_array[MAX_CONNECTIONS];
int open_connections = 0;

DLLIMPORT int sql_connect(PGconn **conn,char *db)
{
  // db es string de conexio pic x(200), ho finalitzem amb un null
  db[199]='\0';

  if (debug)
      MessageBox(0,db,"sql_connect",MB_ICONINFORMATION);

  *conn = PQconnectdb(db);
  //*conn = PQconnectStart(db);

  if (*conn == NULL)
  {
    return -1;
  }
  while (1){
     if (PQstatus(*conn) == CONNECTION_BAD) break;
     if (PQstatus(*conn) == CONNECTION_OK) break;
  }
  if (PQstatus(*conn) == CONNECTION_BAD)
  {
    return -1;
  }
  /* Keep connection for disconnecting after */
  if (open_connections < MAX_CONNECTIONS){
	  connection_array[open_connections] = *conn;
	  open_connections++;
  }
  return 0;
}

DLLIMPORT int sql_error_text(PGconn **conn,char *text){
    strncpy(text,PQerrorMessage(*conn),70);
    return 0;
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/

DLLIMPORT int sql_disconnect(PGconn **conn)
{
	/* retire connection from conection array */
	int i = 0;
	for (i=0;i<open_connections;i++){
		if (connection_array[i] == *conn){
			connection_array[i] =(PGconn *) NULL;
			break;
		}
	}
	if (open_connections > 0){
		while((i + 1) < open_connections){
			connection_array[i] = connection_array[i+1];
			i++;
		}
		open_connections--;
	}
    PQfinish(*conn);
    return 0;
}
/***************************************************************************
 *                                                                         *
 ***************************************************************************/

static int charset_conversion = 0;
DLLIMPORT void sql_charset_conversion_on (void)
{
	charset_conversion = 1;
    return;
}

DLLIMPORT void sql_charset_conversion_off (void)
{
	charset_conversion = 0;
    return;
}
/***************************************************************************
 *                                                                         *
 ***************************************************************************/
char _conversion_iso885915_to_cp850[89][2] =	{
		{160,255},
		{161,173},
		{162,189},
		{163,156},
		{165,190},
		{167,245},
		{169,184},
		{170,166},
		{171,174},
		{172,170},
		{173,240},
		{174,169},
		{175,238},
		{176,248},
		{177,241},
		{178,253},
		{179,252},
		{181,230},
		{182,244},
		{183,250},
		{185,251},
		{186,167},
		{187,175},
		{191,168},
		{192,183},
		{193,181},
		{194,182},
		{195,199},
		{196,142},
		{197,143},
		{198,146},
		{199,128},
		{200,212},
		{201,144},
		{202,210},
		{203,211},
		{204,222},
		{205,214},
		{206,215},
		{207,216},
		{208,209},
		{209,165},
		{210,227},
		{211,224},
		{212,226},
		{213,229},
		{214,153},
		{215,158},
		{216,157},
		{217,235},
		{218,233},
		{219,234},
		{220,154},
		{221,237},
		{222,232},
		{223,225},
		{224,133},
		{225,160},
		{226,131},
		{227,198},
		{228,132},
		{229,134},
		{230,145},
		{231,135},
		{232,138},
		{233,130},
		{234,136},
		{235,137},
		{236,141},
		{237,161},
		{238,140},
		{239,139},
		{240,208},
		{241,164},
		{242,149},
		{243,162},
		{244,147},
		{245,228},
		{246,148},
		{247,246},
		{248,155},
		{249,151},
		{250,163},
		{251,150},
		{252,129},
		{253,236},
		{254,231},
		{255,152},
		{0,0}
};

void iso885915_to_cp850(char *text,int len){
	if (charset_conversion == 0){
		return ;
	}
	int i = 0, c = 0;
	for(c=0;c<len;c++){
		for(i=0;;i++){
			if (_conversion_iso885915_to_cp850[i][0] == 0){
				break;
			}
			if (_conversion_iso885915_to_cp850[i][0] == text[c]){
				text[c] = _conversion_iso885915_to_cp850[i][1];
				break;
			}
		}
	}
}
void cp850_to_iso885915(char *text,int len){
	if (charset_conversion == 0){
		return ;
	}
	int i = 0, c = 0;
	for(c=0;c<len;c++){
		for(i=0;;i++){
			if (_conversion_iso885915_to_cp850[i][0] == 0){
				break;
			}
			if (_conversion_iso885915_to_cp850[i][1] == text[c]){
				text[c] = _conversion_iso885915_to_cp850[i][0];
				break;
			}
		}
	}
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT int sql_create_field_table(PGconn **conn,struct format_camp *format){
    char sql[100];
    char table[30];
    char tmps[10];
    int columns,col;
    int oid,mod,size;
    int digits,decimals;
    //int i;

    PGresult *res;
    if (conn == NULL){
        MessageBox (0, "PGconn == NULL", "sql_create_field_table", MB_ICONINFORMATION);
        return -1 ;
    }
    if (*conn == NULL){
        MessageBox (0, "PGconn == NULL", "sql_create_field_table", MB_ICONINFORMATION);
        return -1 ;
    }
    //MessageBox (0, "1", "1", MB_ICONINFORMATION);
	memcpy ( table, format[0].nom, 27 );
	table[27] = 0;
	//MessageBox (0, table,"table", MB_ICONINFORMATION);
	snprintf(sql,100,"select * from %s where 1=0", table);
    //MessageBox (0, sql, "sql", MB_ICONINFORMATION);

	PQsetnonblocking(*conn,1);
	//MessageBox (0, "SS", "XXXX", MB_ICONINFORMATION);
    if (PQstatus(*conn) != CONNECTION_OK){
 	  MessageBox (0, "BAD CONNECTION", "sql_create_field_table", MB_ICONINFORMATION);
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
           MessageBox (0, PQresultErrorMessage(res), "ERROR : sql_create_field_table", MB_ICONINFORMATION);
           PQclear(res);
           return -1;
    	   break;
    }

    //MessageBox (0, "QUERY OK", "QUERY OK", MB_ICONINFORMATION);
    columns = PQnfields(res);
    if (columns < 1)
        return -1;

    for(col=0;col<columns;col++){
    	   memcpy(format[col+1].nom, PQfname(res,col),strlen(PQfname(res,col)));
    	   oid=PQftype(res,col);
    	   mod=PQfmod(res,col);
    	   size=PQfsize(res,col);
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
    	            //MessageBox (0, tmp2, "Hi", MB_ICONINFORMATION);

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
    	            MessageBox (0,"====", "Unknown field type", MB_ICONINFORMATION);
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

DLLIMPORT int sql_query(PGconn **conn,PGresult **res,struct sqlca *psqlca,char *sql)
{
   struct sqlca ca;
   FILE *f=NULL;

   if (conn == NULL){
       MessageBox (0, "PGconn == NULL", "sql_query", MB_ICONINFORMATION);
       return -1 ;
   }
   if (*conn == NULL){
       MessageBox (0, "PGconn == NULL", "sql_query", MB_ICONINFORMATION);
       return -1 ;
   }
   PQsetnonblocking(*conn,1);

   if (PQstatus(*conn) != CONNECTION_OK){
      MessageBox (0, "BAD CONNECTION", "sql_query", MB_ICONINFORMATION);
      PQreset(*conn);
   }
   *res=PQexec(*conn,sql);

   if(debug || trace) {
      int i=strlen(sql) - 1;
      while(sql[i]==' ') i--;
      sql[i+1]='\0';
      if (debug) MessageBox(0,sql,"sql_query",MB_ICONINFORMATION);
      f = fopen("cbl2pg.log","a+");
      if (f != NULL) {
            fwrite(sql,strlen(sql),1,f);
            fwrite("\n",1,1,f);
      }
   }

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
   if(debug || trace){
      if (debug && (ca.sqlcode < 0))
            MessageBox (0, PQresultErrorMessage(*res), "ERROR : sql_query", MB_ICONINFORMATION);
      if (f != NULL) {
            fwrite(PQresultErrorMessage(*res),strlen(PQresultErrorMessage(*res)),1,f);
            fwrite("\n",1,1,f);
            fclose(f);
      }
   }
   ca.sqlerrd[0]=0;
   ca.sqlerrd[1]=PQoidValue(*res);
   ca.sqlerrd[2]=strtol(PQcmdTuples(*res),NULL, 0);
   ca.sqlerrd[3]=PQntuples(*res);
   ca.sqlwarn[0]=ca.sqlwarn[1]=' ';
   memcpy(psqlca,&ca,sizeof(struct sqlca));
   return ca.sqlcode;
}
/***************************************************************************
 *                                                                         *
 ***************************************************************************/

DLLIMPORT int sql_query_free(PGresult **res){
    if (*res){
	    PQclear(*res);
	    *res=NULL;
    }

    return 0;
}
/***************************************************************************
 *                                                                         *
 ***************************************************************************/
int format_item(PGresult *res,int lin,int col,char *result){

   int oid,mod,size,len;
   long int inum;
   int i;
   long long int lnum;
//   char *pint;
//   short *pshort;
//   char str[200];
   int digits,decimals;
   char *c;
   double num;
   char tmp[21];
   //char tmp2[200];


   oid=PQftype(res,col);
   mod=PQfmod(res,col);
   size=PQfsize(res,col);
   len=PQgetlength(res,lin,col);
   c=PQgetvalue(res,lin,col);

   //  snprintf(tmp2,200,"oid: %d size: %d len: %d,lin:%d",oid,size,len,lin);
   //  MessageBox (0, tmp2, c, MB_ICONINFORMATION);

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
      case 21: // int2 s9(4) sts

            inum = strtol (c, NULL, 0);
            snprintf(tmp,21,"%020ld",inum);

            if (inum < 0) result[4]='-';
            else          result[4]='+';

            if (inum < 0) inum = inum * -1;

            memcpy(result,tmp + 16 ,4);

             // MessageBox (0, tmp, "int2", MB_ICONINFORMATION);
            //return (digits + 1);
            /*  pint=(char *)&inum;
            result[0]=pint[1];
            result[1]=pint[0];*/
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


            snprintf(tmp,21,"%020lld",lnum);

            //MessageBox (0, tmp, "Hi", MB_ICONINFORMATION);

            if (lnum < 0) result[18]='-';
            else          result[18]='+';

            if (lnum < 0) inum = lnum * -1;

            memcpy(result,tmp + 2,18);

            return 19;
            break;
      case 1042: // char (x)
            //snprintf(tmp2,200,"Col:%d, oid:%d, mod:%d, size:%d , len:%d, valor<%s>",
            //   col,oid,mod,size,len,c);
            //MessageBox (0, tmp2, "Hi", MB_ICONINFORMATION);
            //

            digits = mod - 4;
            if (strlen(c) == 0) return digits;
            memcpy(result,c,digits);
            iso885915_to_cp850(result,digits);
            //snprintf(tmp,20,"Len ; %d,val<%s>",len,c);
            //MessageBox (0, tmp, "char(x)", MB_ICONINFORMATION);
            return digits;
            break;
      case 1043: // varchar (x)
            //snprintf(tmp2,200,"Col:%d, oid:%d, mod:%d, size:%d , len:%d, valor<%s>",
            //   col,oid,mod,size,len,c);
            //MessageBox (0, tmp2, "Hi", MB_ICONINFORMATION);

            digits = mod - 4;
            if (strlen(c) == 0) return digits;
            memset(result,' ',digits);
            memcpy(result,c,strlen(c));
            iso885915_to_cp850(result,digits);
            //snprintf(tmp,20,"Len ; %d,val<%s>",len,c);
            //MessageBox (0, tmp, "char(x)", MB_ICONINFORMATION);
            return digits;
            break;
      case 25: // text
            digits = strlen(c);
            if (strlen(c) == 0) return digits;
            memcpy(result,c,digits);
            iso885915_to_cp850(result,digits);
            //snprintf(tmp,20,"Len ; %d,val<%s>",len,c);
            //MessageBox (0, tmp, "char(x)", MB_ICONINFORMATION);
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
              memcpy(result +  4,c     ,4); // aaaa
              memcpy(result +  2,c +  5,2); // mm
              memcpy(result     ,c +  8,2); // dd
              memcpy(result +  8,c + 11,2); // hh
              memcpy(result + 10,c + 14,2); // mm
              memcpy(result + 12,c + 17,2); // ss
              memcpy(result + 14,c + 20,6); // mmmmmm
            }
            return 20;

      default: // altres, es copies les primeres 30 posicions del resultat
            memcpy(result,c,min(30,strlen(c)));
            snprintf(tmp,20,"%d,val<%s>",oid,c);
            MessageBox (0, tmp, "Tipo de camp desconegut", MB_ICONINFORMATION);
            return min(30,strlen(c));
            break;
   }


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
//    MessageBox (0, colname,"prova", MB_ICONINFORMATION);
    col=PQfnumber(*res,colname);
    if (col<0) return -1;
    return format_item(*res,*lin,col,value);
}

/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT int sql_get_line(PGresult **res,int *lin,char *value){
    int columns,i,len,linea;
    char *p;
    //char tmp[100];
    p=value;
    linea=(signed int)*lin;
    //if (*lin != 0) {
    //  snprintf(tmp,20,"line:%d,%d,%d",*lin,linea,PQntuples(*res));
    //  MessageBox (0,tmp, "Camp INI", MB_ICONINFORMATION);
    //}

    columns = PQnfields(*res);
    if (columns < 0)
        return columns;
    if (*lin >= PQntuples(*res)){
   //    MessageBox (0,"molt feo", "FEO", MB_ICONINFORMATION);
       return PQntuples(*res);
        }
    for(i=0;i<columns;i++){
        //snprintf(tmp,20,"%d,total<%d>lin:%d",i,columns,*lin);
        //MessageBox (0,tmp, "Camp INI", MB_ICONINFORMATION);
        len = format_item(*res,(unsigned int)linea,i,p);
        //MessageBox (0,tmp, "Camp FIN", MB_ICONINFORMATION);
        p+=len;
    }
    return 0;

}
/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT int sql_get_info(PGresult **res,int *lin,int *col,char *value){
   char *c,str[100];
   int oid,size,mod,len;
   oid=PQftype(*res,*col);
   mod=PQfmod(*res,*col);
   size=PQfsize(*res,*col);
   len=PQgetlength(*res,*lin,*col);
   switch(oid){
   case 1700:
         sprintf(str,"col:%d , numeric , len,dec:%d,%d ",
               *col, ((mod - 4) & 0xffff0000) >> 16, ((mod - 4) & 0x0000ffff)) ;
         break;
   default:
        sprintf(str,"col:%d , oid:%d , mod:%d , size:%d , len:%d",
               *col,oid,mod,size,len);
        break;
   }
   c=PQgetvalue(*res,*lin,*col);


   strncpy(value,c,80);
   return 0;
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
int formatejar_camp(char *sql,char*pdata,struct format_camp *pformat,char *valors){
   char camp_editat[1000],tmp[20],nom[21];
   int len,dec;
   strncpy(tmp,pformat->len,3);
   tmp[3]=0;
   len = strtol(tmp,NULL,10);

   strncpy(tmp,pformat->dec,2);
   tmp[2]=0;
   dec = strtol(tmp,NULL,10);
   if (debug) {
    memmove(nom,pformat->nom,20);
    nom[20]=0;
    //sprintf(tmp,"nom=%s,tipo=%c,len=%d,dec=%d",nom,pformat->tipo,len,dec);
    //MessageBox (0, tmp, "Hi", MB_ICONINFORMATION);
    strcat(valors,nom);
    strcat(valors,"=");
   }
   switch(pformat->tipo){
           case 'C':
        	   	     cp850_to_iso885915(pdata,len);
                     strcat(sql,"'");
                     PQescapeString(camp_editat,pdata,len);
                     int ll;
                     ll = strlen(camp_editat);
                     ll--;
                     while(ll>0)
                     {
                    	 if (camp_editat[ll] == ' ')
                    	 {
                    		 camp_editat[ll]=0;
                    		 ll--;
                    	 }else{
                    		 ll=0;
                    	 }
                     }
                     strcat(sql,camp_editat);
                     strcat(sql,"'");
                     if (debug) {
                      //MessageBox (0, camp_editat, nom, MB_ICONINFORMATION);
                      strcat(valors,"<");
                      strcat(valors,camp_editat);
                      strcat(valors,">");
                     }
                     break;
           case 'D':
                     if (strncmp(pdata,"00000000",8)==0) {
                         strcat(sql,"'0001-01-01'");
                         if (debug ) strcat(valors,"<0001-01-01>");
                         break;
                     }
                     strcat(sql,"'");
                     strncat(sql,pdata+4,4);
                     strcat(sql,"-");
                     strncat(sql,pdata+2,2);
                     strcat(sql,"-");
                     strncat(sql,pdata,2);
                     strcat(sql,"'");
                     if (debug) {
                      strcat(valors,"<");
                      strncat(valors,pdata+4,4);
                      strcat(valors,"-");
                      strncat(valors,pdata+2,2);
                      strcat(valors,"-");
                      strncat(valors,pdata,2);
                      strcat(valors,">");
                     }
                     break;
           case 'N':
                     camp_editat[0]=pdata[len - 1];
                     camp_editat[1]=0;
                     strncat(camp_editat,pdata,len - dec - 1);
                     camp_editat[len - dec]=0;
                     if ( dec > 0 ) {
                       strcat(camp_editat,".");
                       strncat(camp_editat,pdata + len - dec - 1,dec);
                     }
                     strcat(sql,"'");
                     strcat(sql,camp_editat);
                     strcat(sql,"'");
                     if (debug) {
                         //MessageBox (0, camp_editat, nom, MB_ICONINFORMATION);
                         strcat(valors,"<");
                         strcat(valors,camp_editat);
                         strcat(valors,">");
                     }
                     break;
           case 'T':
                     strcat(sql,"'");
                     strncat(sql,pdata,2);
                     strcat(sql,":");
                     strncat(sql,pdata+2,2);
                     strcat(sql,":");
                     strncat(sql,pdata+4,2);
                     strcat(sql,"'");
                     if (debug) {
                      strcat(valors,"<");
                      strncat(valors,pdata,2);
                      strcat(valors,":");
                      strncat(valors,pdata+2,2);
                      strcat(valors,":");
                      strncat(valors,pdata+4,2);
                      strcat(valors,">");
                     }
                     break;
    }
    if(debug){
        strcat(valors,"\n");
    }
    return len;
}
/***************************************************************************
 *                                                                         *
 ***************************************************************************/

int asignar_camp(char *sql,char *pdata,struct format_camp *pformat){
    char tmp[50];
    char valors[8000];
    int len;
    int long_nom;

    strncpy(tmp,pformat->len,3);
    tmp[3]=0;
    len = strtol(tmp,NULL,10);

//        sprintf(tmp,"len:%d,dec:%d",len,dec);
//        MessageBox (0, tmp, "Hi", MB_ICONINFORMATION);
    long_nom=strcspn(pformat->nom," ");

    strncat(sql,pformat->nom,long_nom);
    strcat(sql,"=");
    formatejar_camp(sql,pdata,pformat,valors);
    return len;
}
/***************************************************************************
 *                                                                         *
 ***************************************************************************/

DLLIMPORT int sql_make_update(PGconn **conn,struct sqlca *psqlca,char *data,struct format_camp *format){
    char sql[8000];
    char tmp[20];
    char *pdata;
    PGresult *res;
    struct format_camp *pformat;
    int camps,i,long_nom;

    // busquem quants camps hi ha (el primer es el nom de la taula)
    camps = strlen((char *)format) / sizeof(struct format_camp) - 1;

    sql[0]=0;

//    format->nom[strcspn(format->nom," ")]=0;
  //  sprintf(sql,"update %s set \0",format->nom);
    strcpy(sql,"update ");
    long_nom=strcspn(format->nom," ");
    strncat(sql,format->nom,long_nom);
    strcat(sql," set ");

    pformat=format;
    pdata=data;
/*    if (debug){
       sprintf(tmp,"%p\0",pformat);
       MessageBox (0, tmp, "Debug", MB_ICONINFORMATION);
       sprintf(tmp,"%p\0",pdata);
       MessageBox (0, tmp, "Debug", MB_ICONINFORMATION);
     }*/


    for(i=0;i< camps;i++){
        pformat++;
        if ((pformat->tipo != ' ') && (pformat->key==' '))  {
          asignar_camp(sql,pdata,pformat);
          strcat(sql," , ");
        }
        strncpy(tmp,pformat->len,3);
        tmp[3]=0;
        pdata+=strtol(tmp,NULL,10);

    }
    sql[strlen(sql) - 3]=0;
//    strcat(sql,"\0");


//    if (debug)
  //     MessageBox (0, sql, "Debug3", MB_ICONINFORMATION);

    strcat(sql," where ");
    pformat=format;
    pdata=data;
    for(i=0;i< camps;i++){
        pformat++;
        if (pformat->key=='K' || pformat->key=='k'){
          asignar_camp(sql,pdata,pformat);
          strcat(sql," and ");
        }
        strncpy(tmp,pformat->len,3);
        tmp[3]=0;
        pdata+=strtol(tmp,NULL,10);
    }
    sql[strlen(sql) - 5]=0;

    i = sql_query(conn,&res,psqlca,sql);
    sql_query_free(&res);
    return i;
}
/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT int sql_make_insert(PGconn **conn,struct sqlca *psqlca,char *data,struct format_camp *format){
    char sql[10000];
    char valors[10000];
    char tmp[20];
    char *pdata;
    PGresult *res;
    struct format_camp *pformat;
    int camps,i,long_nom;

    // busquem quants camps hi ha (el primer es el nom de la taula)
    camps = strlen((char *)format) / sizeof(struct format_camp) - 1;

    if (debug) valors[0]=0;

    //format->nom[strcspn(format->nom," ")]=0;
    //sprintf(sql,"insert into %s (",format->nom);
    //sql[0]=0;
    strcpy(sql,"insert into ");
    long_nom=strcspn(format->nom," ");
    strncat(sql,format->nom,long_nom);
    strcat(sql," (");

    /*if (debug){
       MessageBox (0, sql, "Debug", MB_ICONINFORMATION);
//       MessageBox (0, sql, "Debug", MB_ICONINFORMATION);
    }*/


    pformat=format;
    for(i=0;i< camps;i++){
        pformat++;
        if ((pformat->key != 'k') && (pformat->tipo != ' ')) {
          long_nom=strcspn(pformat->nom," ");
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
          formatejar_camp(sql,pdata,pformat,valors);
          strcat(sql,",");
        }
        strncpy(tmp,pformat->len,3);
        tmp[3]=0;
        pdata+=strtol(tmp,NULL,10);
    }
    sql[strlen(sql) - 1]=0;
    strcat(sql,")");
    if (debug){
       MessageBox (0, valors, "Debug", MB_ICONINFORMATION);
//       MessageBox (0, sql, "Debug", MB_ICONINFORMATION);
    }
    i = sql_query(conn,&res,psqlca,sql);
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

    if (debug)
       MessageBox (0, sql, "Debug", MB_ICONINFORMATION);

    i = sql_query(conn,&res,psqlca,sql);
    sql_query_free(&res);
    return i;

}
/***************************************************************************
 *                                                                         *
 ***************************************************************************/
DLLIMPORT int sql_exec_file(PGconn **conn,struct sqlca *psqlca,char *filename){
    FILE *f;
    char buffer[1000];
    char *sql;
    int sql_size = 1000,size,ret;
    PGresult *res;
    if (debug) MessageBox (0, filename, "Fitxer", MB_ICONINFORMATION);

    f = fopen(filename,"r");
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

    if (debug)  MessageBox (0, sql, "SQL", MB_ICONINFORMATION);
    ret = sql_query(conn,&res,psqlca,sql);
    //MessageBox (0, "sqlquery OK", "Hi2", MB_ICONINFORMATION);
    sql_query_free(&res);
    //MessageBox (0, "sql_query_Free OK", "Hi", MB_ICONINFORMATION);
    free(sql);
    return ret;
 }

void close_open_connections(){
    FILE *f;
	while(open_connections > 0){
      if(debug || trace) {
			  if (debug) MessageBox(0,"Close one connection","close_open_connections",MB_ICONINFORMATION);
			  f = fopen("cbl2pg.log","a+");
			  if (f != NULL) {
		            fprintf(f,"# Close connection %d \n", open_connections);
			  }
	    }
		sql_disconnect(&connection_array[open_connections - 1]);
	}
}



//   MessageBox (0, str, "Hi", MB_ICONINFORMATION);
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
