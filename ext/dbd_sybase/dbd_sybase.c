/*
 Sample Sybase driver for Rainer Perl's DBI-for-Ruby

 Based on (and requires) "FreeTDS" http://www.freetds.org/
 
 Version : 0.0.3
 Author  : Rainer Perl (rainer.perl@sprytech.com)
 Homepage: http://www.sprytech.com/~rainer.perl/ruby/

 Copyright (c) 2001 Rainer Perl

 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License
 as published by the Free Software Foundation; either version 2
 of the License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

// #include <stdio.h> // (used that for debugging)
#include "ruby.h"
#include <tds.h>

typedef struct sybresult
{
 TDSSOCKET *tds;
 char *sqlstring;
} SYBRESULT;

// DBHandle
static VALUE dh_init(int argc, VALUE* argv, VALUE self);
static VALUE dh_do(VALUE self, VALUE sqlstring);
static VALUE dh_do_store(VALUE self, VALUE sqlstring);
static VALUE dh_quote(VALUE self, VALUE string2quote);
static VALUE dh_disconnect(VALUE self);

// ResultHandle
VALUE rh_new(VALUE tds_socket, VALUE sqlstring);
static VALUE rh_init(VALUE self);
static VALUE rh_fetchrow(VALUE self);
static VALUE rh_fetch_fields(VALUE self);
static VALUE rh_finish(VALUE self);
static void rh_free(void *sybres);

// Internal_TDSLogin
VALUE tdsl_new(VALUE db_host, VALUE db_user, VALUE db_pass);
static VALUE tdsl_init(VALUE self, VALUE db_host, VALUE db_user, VALUE db_pass);
static void tdsl_free(void *login);

// Internal_TDSSocket
VALUE tdss_new(VALUE db_host, VALUE db_user, VALUE db_pass);
static VALUE tdss_init(VALUE self);

// Misc
void value_as_string(char *target, int targetlen, TDSSOCKET *tds, int col_idx);
static void process_results(TDSSOCKET *tds);

VALUE mDBI,
      mSybase,
      cDBHandle,
      cResultHandle,
      cTDSLogin,
      cTDSSocket;


static VALUE dh_init(int argc, VALUE* argv, VALUE self)
{
 VALUE db_user,
       db_pass,
       db_args,
       db_name,
       db_host,
       tds_socket;

 TDSSOCKET *tds;
        
 /* Let's fill our variables... */
       
 rb_scan_args(argc, argv, "2*", &db_user, &db_pass, &db_args);
 
 db_args = rb_ary_shift(db_args);

 if(db_args != Qnil)
 {
  db_args = rb_str_split(db_args, ":");
  db_name = rb_ary_entry(db_args, 0);
  db_host = rb_ary_entry(db_args, 1);
 }
 
 if(db_host == Qnil) { db_host = rb_str_new2("localhost"); }
  
 /* Get a TDSSOCKET */ 
 tds_socket = tdss_new(db_host, db_user, db_pass);
 rb_iv_set(self, "@tds_socket", tds_socket);
 Data_Get_Struct(tds_socket, TDSSOCKET, tds);

 /* If the user submited a database-name, change to it */ 
 if(db_name != Qnil)
 {
  if(tds_submit_query(tds,STR2CSTR(rb_str_concat(rb_str_new2("USE "), db_name))) != TDS_SUCCEED)
  {
   rb_raise(rb_eRuntimeError, "SQL-USE failed (1)");
  }
  else
  {
   process_results(tds);
  } 
 }
   
 return self;
} // dh_init

static VALUE dh_do(VALUE self, VALUE sqlstring)
{
 TDSSOCKET *tds;

 Data_Get_Struct(rb_iv_get(self, "@tds_socket"), TDSSOCKET, tds);

 if(tds_submit_query(tds,STR2CSTR(sqlstring)) != TDS_SUCCEED)
 {
  rb_raise(rb_eRuntimeError, "SQL-query failed (1)");
 }
  else
 {
  process_results(tds);
 }

 return Qnil;
} // dh_do

static VALUE dh_do_store(VALUE self, VALUE sqlstring)
{
 return rh_new(rb_iv_get(self, "@tds_socket"), sqlstring);
} // dh_do_store

static VALUE dh_quote(VALUE self, VALUE string2quote)
{
 /*
  This quoting-stuff is _very_ basic and probably needs *some* work.
 */
 VALUE quotedStr = rb_funcall(string2quote, rb_intern("gsub"), 2, rb_reg_new("'", 1, 0), rb_str_new2("''"));
 
 return  rb_str_concat(rb_str_concat(rb_str_new2("'"), quotedStr), rb_str_new2("'"));
} // dh_quote

static VALUE dh_disconnect(VALUE self)
{
 TDSSOCKET *tds;

 Data_Get_Struct(rb_iv_get(self, "@tds_socket"), TDSSOCKET, tds);
 tds_free_socket(tds);
 rb_gc();

 return Qnil;
} // dh_disconnect

VALUE rh_new(VALUE tds_socket, VALUE sqlstring)
{
 VALUE oSybRes;
 SYBRESULT *sybres;

 sybres = ALLOC(SYBRESULT);
 Data_Get_Struct(tds_socket, TDSSOCKET, sybres->tds);
 sybres->sqlstring=STR2CSTR(sqlstring);

 oSybRes = Data_Wrap_Struct(cResultHandle, 0, rh_free, sybres);
 
 rb_obj_call_init(oSybRes, 0, 0);
 
 return oSybRes;
} // rh_new

static VALUE rh_init(VALUE self)
{
 SYBRESULT *sybres;
 int i,
     rc;
 VALUE aRow,
       allRows;
 char  *mystring;
 int   mystrlen;
 
 Data_Get_Struct(self, SYBRESULT, sybres);

 if(tds_submit_query(sybres->tds,sybres->sqlstring) != TDS_SUCCEED)
 {
  rb_raise(rb_eRuntimeError, "SQL-query failed (1)");
 }
  else
 {
  /*
   Based on FreeTDS's src/tds/unittest/t0005.c
  */

  allRows = rb_ary_new();
  
  while ((rc=tds_process_result_tokens(sybres->tds))==TDS_SUCCEED)
  {
   while ((rc=tds_process_row_tokens(sybres->tds))==TDS_SUCCEED)
   {
    aRow = rb_ary_new();
    for (i=0; i<sybres->tds->res_info->num_cols; i++)
    {
     /*      
      We're calling calloc for every column in every row to get the
      memory for the string-conversion.
      There are probably faster ways...
     */
     if(sybres->tds->res_info->columns[i]->column_textvalue)
     { mystrlen=(sybres->tds->res_info->columns[i]->column_textsize)+1; }
     else
     { mystrlen = 256; }
     mystring = calloc(1, mystrlen);

     if(mystring == NULL)
     {
      rb_raise(rb_eRuntimeError, "Couldn't malloc - out of memory? (1)");
     }
     value_as_string(mystring, mystrlen, sybres->tds, i);
     rb_ary_push(aRow, rb_str_new2(mystring));

     free(mystring);
    }
    rb_ary_push(allRows, aRow);
   }
   if (rc == TDS_FAIL)
   {
    rb_raise(rb_eRuntimeError, "tds_process_row_tokens() returned TDS_FAIL\n");
   }
   else if (rc != TDS_NO_MORE_ROWS)
   {
    rb_raise(rb_eRuntimeError, "tds_process_row_tokens() unexpected return\n");
   }
  }
  
  if (rc == TDS_FAIL)
  {
   rb_raise(rb_eRuntimeError, "tds_process_result_tokens() returned TDS_FAIL for SELECT\n");
   return 1;
  }
  else
   if (rc != TDS_NO_MORE_RESULTS)
   {
    rb_raise(rb_eRuntimeError, "tds_process_result_tokens() unexpected return\n");
   }
 }
 
 rb_iv_set(self, "@allRows", allRows); 
 
 return self;
} // rh_init

static VALUE rh_fetchrow(VALUE self)
{
 return rb_ary_shift(rb_iv_get(self, "@allRows"));
} // rh_fetchrow

static VALUE rh_fetch_fields(VALUE self)
{
 int i;
 SYBRESULT *sybres;
 VALUE myFields,
       myHash;

 Data_Get_Struct(self, SYBRESULT, sybres);

 myFields = rb_ary_new();

 for (i=0; i<sybres->tds->res_info->num_cols; i++)
 {
  myHash = rb_hash_new();
  rb_hash_aset(myHash, rb_str_new2("name"), rb_str_new2(sybres->tds->res_info->columns[i]->column_name));
  rb_ary_push(myFields, myHash);
 }

 return myFields;
} // rh_fetch_fields

static VALUE rh_finish(VALUE self)
{
 rb_iv_set(self, "@allRows", Qnil); 
 rb_gc();
 
 return Qnil;
} // rh_finish

static void rh_free(void *sybres)
{
 xfree(sybres);
} // rh_free

VALUE tdsl_new(VALUE db_host, VALUE db_user, VALUE db_pass)
{
 VALUE argv[3];

 TDSLOGIN *login = tds_alloc_login();
 VALUE tds_login = Data_Wrap_Struct(cTDSLogin, 0, tdsl_free, login); 

// printf("tdsl_new - H/U/P: %s/%s/%s\n", STR2CSTR(db_host), STR2CSTR(db_user), STR2CSTR(db_pass));
 
 argv[0]=db_host;
 argv[1]=db_user;
 argv[2]=db_pass;
 rb_obj_call_init(tds_login, 3, argv);
 
 return tds_login;
} // tdsl_new

static VALUE tdsl_init(VALUE self, VALUE db_host, VALUE db_user, VALUE db_pass)
{
 TDSLOGIN *login;

// printf("tdsl_init - H/U/P: %s/%s/%s\n", STR2CSTR(db_host), STR2CSTR(db_user), STR2CSTR(db_pass));
 
 Data_Get_Struct(self, TDSLOGIN, login);
 
 tds_set_server(login, STR2CSTR(db_host));
 tds_set_user(login, STR2CSTR(db_user));
 tds_set_passwd(login, STR2CSTR(db_pass));
 tds_set_app(login, "Ruby-Script");
 tds_set_host(login, "myhost");
 tds_set_library(login, "TDS-Library");
 tds_set_charset(login, "iso_1");
 tds_set_language(login, "us_english");
 tds_set_packet(login, 512); 
 
 return self;
} // tdsl_init

static void tdsl_free(void *login)
{
 tds_free_login(login);
} // tdsl_free;

VALUE tdss_new(VALUE db_host, VALUE db_user, VALUE db_pass)
{
 TDSLOGIN *login;       
 TDSSOCKET *tds;
 VALUE tds_login,
       tds_socket;
 
// printf("tdss_new - H/U/P: %s/%s/%s\n", STR2CSTR(db_host), STR2CSTR(db_user), STR2CSTR(db_pass));

 tds_login = tdsl_new(db_host, db_user, db_pass);
 Data_Get_Struct(tds_login, TDSLOGIN, login);
 /*
  Strange: if we do
  Data_Get_Struct(tdsl_new(db_host, db_user, db_pass), TDSLOGIN, login);
  instead of tds_login = ... and Data_Get_Struct(...
  then tdsl_new will be called twice. Can someone explain _why_?
 */
 
 tds = tds_connect(login);
 if (! tds)
 {
  rb_raise(rb_eRuntimeError, "Login failed (1)");
 } 

 /*
   Maybe we should add a cleanup-routine?
   
   Well, maybe not: dh_disconnect will free the struct anyway - ruby will
                    warn that this junk has been freed already if I add
                    a cleanup.
 */  
 tds_socket = Data_Wrap_Struct(cTDSSocket, 0, 0, tds); 
 
 return tds_socket;
} // tdss_new

static VALUE tdss_init(VALUE self)
{
 return self;
} // tdss_init

void value_as_string(char *target, int targetlen, TDSSOCKET *tds, int col_idx)
{
 /*
  Based on FreeTDS's src/tds/unittest/t0005.c
 */
 
 const int    type    = tds->res_info->columns[col_idx]->column_type;
 const char  *row     = tds->res_info->current_row;
 const int    offset  = tds->res_info->columns[col_idx]->column_offset;
 const void  *value   = (row+offset);
 
 switch(type)
 {
  case SYBNTEXT: SYBTEXT;
  case SYBTEXT:
   strncpy(target, tds->res_info->columns[col_idx]->column_textvalue, targetlen-1);
   break;   
  default:
   tds_convert(type, (char *)value, tds->res_info->columns[col_idx]->column_size,	SYBVARCHAR, target, targetlen);
   break;
 }
} // value_as_string

static void process_results(TDSSOCKET *tds)
{
 int rc;

 while((rc=tds_process_result_tokens(tds))==TDS_SUCCEED)
 {
  /* do nothing special, just process the tokens */
 }

 /*
 printf("type: %i, line: %i, message: %i, state: %i, level: %i, server: %s, proc: %s, sql-state: %s\nmessage: %s\n",
        tds->msg_info->priv_msg_type, tds->msg_info->line_number, tds->msg_info->msg_number,
        tds->msg_info->msg_state, tds->msg_info->msg_level, tds->msg_info->server,
        tds->msg_info->proc_name, tds->msg_info->sql_state, tds->msg_info->message);
 */
 
 if(tds->msg_info->msg_level > 0)
 {
  rb_raise(rb_eRuntimeError, "server said: %s", tds->msg_info->message);
 }
} // process_results


/*
 Main init (called by ruby)
*/
void Init_dbd_sybase()
{
 // Module DBI::DBD::Sybase
 mDBI = rb_eval_string("DBI");
 mSybase = rb_define_module_under(rb_define_module_under(mDBI, "DBD"), "Sybase");
 
 // DBHandle
 cDBHandle = rb_define_class_under(mSybase, "DBHandle", rb_cObject);
 rb_define_method(cDBHandle, "initialize", dh_init, -1);
 rb_define_method(cDBHandle, "do", dh_do, 1); 
 rb_define_method(cDBHandle, "do_store", dh_do_store, 1); 
 rb_define_method(cDBHandle, "quote", dh_quote, 1); 
 rb_define_method(cDBHandle, "disconnect", dh_disconnect, 0); 
 
 // ResultHandle
 cResultHandle = rb_define_class_under(mSybase, "ResultHandle", rb_cObject);
 rb_define_method(cResultHandle, "initialize", rh_init, 0);
 rb_define_method(cResultHandle, "fetchrow", rh_fetchrow, 0);
 rb_define_method(cResultHandle, "fetch_fields", rh_fetch_fields, 0);
 rb_define_method(cResultHandle, "finish", rh_finish, 0);
 
 // Internal_TDSLogin
 cTDSLogin = rb_define_class_under(mSybase, "Internal_TDSLogin", rb_cObject);
 rb_define_method(cTDSLogin, "initialize", tdsl_init, 3);

 // Internal_TDSSocket
 cTDSSocket = rb_define_class_under(mSybase, "Internal_TDSSocket", rb_cObject);
 rb_define_method(cTDSSocket, "initialize", tdss_init, 0);
 
 // Register the driver
 rb_eval_string("DBI.register('sybase', DBI::DBD::Sybase)");
}
