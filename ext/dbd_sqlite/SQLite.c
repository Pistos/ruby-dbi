/*
 * DBD driver for SQLite
 *
 * Copyright (c) 2001, 2002, 2003 Michael Neumann <mneumann@ntecs.de>
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions 
 * are met:
 * 1. Redistributions of source code must retain the above copyright 
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright 
 *    notice, this list of conditions and the following disclaimer in the 
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * 
 * $Id: SQLite.c,v 1.1.1.1 2006/01/04 02:03:18 francis Exp $
 */


/* TODO:
 *
 * - use more C-functions (C++ strings) to improve speed)
 * - use IDs instead of each time rb_iv_get etc.. 
 * - check correct use of exception classes 
 * - warnings: should use rb_warn ? 
 * - get column_info using "pragma table_info(table_name)" and
 *   use it to return the appropriate Ruby type.
 */

#include <sqlite.h>
#include "ruby.h"

#define USED_DBD_VERSION "0.1"

static VALUE mDBD, mSQLite;
static VALUE cDriver, cDatabase, cStatement;
static VALUE cBaseDriver, cBaseDatabase, cBaseStatement;
static VALUE cTimestamp;
static VALUE eOperationalError, eDatabaseError, eInterfaceError;
static VALUE eNotSupportedError;
static VALUE TYPE_CONV_MAP, CONVERTER, CONVERTER_PROC;
static ID id_to_time, id_utc, id_strftime;

#define SQL_FETCH_NEXT     1
#define SQL_FETCH_PRIOR    2
#define SQL_FETCH_FIRST    3
#define SQL_FETCH_LAST     4
#define SQL_FETCH_ABSOLUTE 5 
#define SQL_FETCH_RELATIVE 6 

struct sDatabase {
  struct sqlite *conn;
  int autocommit;
  int full_column_names; 
};

struct sStatement {
  VALUE conn, statement;
  char **result;
  int nrow, ncolumn, row_index, nrpc;
};

struct sTable {
  char **result;
  int nrow, ncolumn;
};

static void
rubydbi_sqlite_check_sql(VALUE sql)
{
  if (RTEST(rb_funcall(sql, rb_intern("include?"), 1, INT2FIX(0)))) {
    rb_raise(eDatabaseError, "Bad SQL, it contains NULL(\\0) character.");
  }
}

static VALUE
Driver_initialize(VALUE self)
{
  VALUE dbd_version = rb_str_new2(USED_DBD_VERSION);

  rb_call_super(1, &dbd_version);
   
  return Qnil;
}

static void database_free(void *p) {
  struct sDatabase *db = (struct sDatabase*) p;

  if (db->conn) {
    sqlite_close(db->conn);
    db->conn = NULL;
  }

  free(p);
}

static VALUE
Driver_connect(VALUE self, VALUE dbname, VALUE user, VALUE auth, VALUE attr)
{
  char *errmsg;
  struct sDatabase *db;
  VALUE database, errstr, h_ac, h_fn; 
  int state;


  Check_Type(dbname, T_STRING);
  Check_Type(attr, T_HASH);

  database = Data_Make_Struct(cDatabase, struct sDatabase, 0, database_free, db);

  db->autocommit = 0;  /* off */
  db->full_column_names = 0; /* off */

  h_ac = rb_hash_aref(attr, rb_str_new2("AutoCommit"));
  if (RTEST(h_ac)) {
    db->autocommit = 1; /* on */
  } 

  h_fn = rb_hash_aref(attr, rb_str_new2("sqlite_full_column_names"));
  if (RTEST(h_fn)) {
    db->full_column_names = 1; /* on */
  } 


  db->conn = sqlite_open(STR2CSTR(dbname), 0, &errmsg);
  if (!db->conn) {
    errstr = rb_str_new2(errmsg); 
    free(errmsg);
    rb_raise(eOperationalError, STR2CSTR(errstr));
  }

  /* AutoCommit */
  if (db->autocommit == 0) {
    state = sqlite_exec(db->conn, "BEGIN TRANSACTION", NULL, NULL, &errmsg);
    if (state != SQLITE_OK) {
      errstr = rb_str_new2(errmsg); free(errmsg);
      rb_str_cat(errstr, "(", 1); rb_str_concat(errstr, rb_str_new2(sqliteErrStr(state))); rb_str_cat(errstr, ")", 1);
      rb_raise(eDatabaseError, STR2CSTR(errstr));
    }
  }

  /* Put Full Column Names on (always) */
  state = sqlite_exec(db->conn, "PRAGMA full_column_names = ON", NULL, NULL, &errmsg);
  if (state != SQLITE_OK) {
    errstr = rb_str_new2(errmsg); free(errmsg);
    rb_str_cat(errstr, "(", 1); rb_str_concat(errstr, rb_str_new2(sqliteErrStr(state))); rb_str_cat(errstr, ")", 1);
    rb_raise(eDatabaseError, STR2CSTR(errstr));
  }

  return database;
}

static VALUE
Database_aref(VALUE self, VALUE key)
{
  struct sDatabase *db;

  Check_Type(key, T_STRING);

  if (rb_str_cmp(key, rb_str_new2("AutoCommit")) == 0) {
    Data_Get_Struct(self, struct sDatabase, db);
    if (db->autocommit == 0) return Qfalse;
    else if (db->autocommit == 1) return Qtrue;
  }
  else if (rb_str_cmp(key, rb_str_new2("sqlite_full_column_names")) == 0) {
    Data_Get_Struct(self, struct sDatabase, db);
    if (db->full_column_names == 0) return Qfalse;
    else if (db->full_column_names == 1) return Qtrue;
  }

  return Qnil;
}

static VALUE
Database_aset(VALUE self, VALUE key, VALUE value)
{
  struct sDatabase *db;
  int state;
  char *errmsg;
  VALUE errstr;

  Check_Type(key, T_STRING);

  if (rb_str_cmp(key, rb_str_new2("AutoCommit")) == 0) {
    Data_Get_Struct(self, struct sDatabase, db);
    if (RTEST(value)) {
      /* put autocommit on */
      if (db->autocommit == 0) {
        db->autocommit = 1;

        state = sqlite_exec(db->conn, "END TRANSACTION", NULL, NULL, &errmsg);
        if (state != SQLITE_OK) {
          errstr = rb_str_new2(errmsg); free(errmsg);
          rb_str_cat(errstr, "(", 1); rb_str_concat(errstr, rb_str_new2(sqliteErrStr(state))); rb_str_cat(errstr, ")", 1);
          rb_raise(eDatabaseError, STR2CSTR(errstr));
        }
      }
    } else {
      /* put autocommit off */
      if (db->autocommit == 1) {
        db->autocommit = 0;

        state = sqlite_exec(db->conn, "BEGIN TRANSACTION", NULL, NULL, &errmsg);
        if (state != SQLITE_OK) {
          errstr = rb_str_new2(errmsg); free(errmsg);
          rb_str_cat(errstr, "(", 1); rb_str_concat(errstr, rb_str_new2(sqliteErrStr(state))); rb_str_cat(errstr, ")", 1);
          rb_raise(eDatabaseError, STR2CSTR(errstr));
        }
      }
    }
  } else if (rb_str_cmp(key, rb_str_new2("sqlite_full_column_names")) == 0) {
    Data_Get_Struct(self, struct sDatabase, db);

    if (RTEST(value)) {
      /* put full_column_names on */
      if (db->full_column_names == 0) {
        db->full_column_names = 1;
      }
    } else {
      /* put full_column_names off */
      if (db->full_column_names == 1) {
        db->full_column_names = 0;
      }

    }

  }
  return Qnil;
}


static VALUE
Database_disconnect(VALUE self)
{
  struct sDatabase *db;
  Data_Get_Struct(self, struct sDatabase, db);

  if (db->conn) {
    sqlite_close(db->conn);
    db->conn = NULL;
  }

  return Qnil;
}

static VALUE
Database_ping(VALUE self)
{
  return Qtrue;
}


static VALUE
Database_commit(VALUE self)
{
  VALUE errstr;
  struct sDatabase *db;
  int state;
  char *errmsg;

  Data_Get_Struct(self, struct sDatabase, db);

  if (db->autocommit == 0) { /* Autocommit is off */

    state = sqlite_exec(db->conn, "END TRANSACTION; BEGIN TRANSACTION", NULL, NULL, &errmsg);
    if (state != SQLITE_OK) {
      errstr = rb_str_new2(errmsg); free(errmsg);
      rb_str_cat(errstr, "(", 1); rb_str_concat(errstr, rb_str_new2(sqliteErrStr(state))); rb_str_cat(errstr, ")", 1);
      rb_raise(eDatabaseError, STR2CSTR(errstr));
    }

  } else if (db->autocommit == 1) { /* Autocommit is on */
    rb_warn("Warning: Commit ineffective while AutoCommit is on"); 
  }

  return Qnil;
}

static VALUE
Database_rollback(VALUE self)
{
  VALUE errstr;
  struct sDatabase *db;
  int state;
  char *errmsg;

  Data_Get_Struct(self, struct sDatabase, db);

  if (db->autocommit == 0) { /* Autocommit is off */

    state = sqlite_exec(db->conn, "ROLLBACK TRANSACTION; BEGIN TRANSACTION", NULL, NULL, &errmsg);
    if (state != SQLITE_OK) {
      errstr = rb_str_new2(errmsg); free(errmsg);
      rb_str_cat(errstr, "(", 1); rb_str_concat(errstr, rb_str_new2(sqliteErrStr(state))); rb_str_cat(errstr, ")", 1);
      rb_raise(eDatabaseError, STR2CSTR(errstr));
    }

  } else if (db->autocommit == 1) { /* Autocommit is on */
    rb_warn("Warning: Rollback ineffective while AutoCommit is on"); 
  }

  return Qnil;
}

static VALUE
Database_do(int argc, VALUE *argv, VALUE self)
{
  /* argv[0]         = stmt
   * argv[1..argc-1] = bindvars 
   */

  VALUE prs[3], sql, errstr;
  struct sDatabase *db;
  int state;
  char *errmsg;

  Data_Get_Struct(self, struct sDatabase, db);

  /* bind params to sql */
  prs[0] = self;
  prs[1] = argv[0];
  prs[2] = rb_ary_new4(argc-1, &argv[1]); 
  sql = rb_funcall2(self, rb_intern("bind"), 3, prs);
  rubydbi_sqlite_check_sql(sql);
  
  state = sqlite_exec(db->conn, STR2CSTR(sql), NULL, NULL, &errmsg);
  if (state != SQLITE_OK) {
    errstr = rb_str_new2(errmsg); free(errmsg);
    rb_str_cat(errstr, "(", 1); rb_str_concat(errstr, rb_str_new2(sqliteErrStr(state))); rb_str_cat(errstr, ")", 1);
    rb_raise(eDatabaseError, STR2CSTR(errstr));
  }

  return Qnil;
}

static int tables_callback(void *pArg, int argc, char **argv, char **columnNames) {
  if (argv != 0 && argv[0] != 0) { 
    rb_ary_push(*(VALUE*)pArg, rb_str_new2(argv[0])); 
  }
  return 0;
}

static VALUE
Database_tables(VALUE self)
{
  VALUE errstr, arr;
  struct sDatabase *db;
  int state;
  char *errmsg;

  Data_Get_Struct(self, struct sDatabase, db);
  
  arr = rb_ary_new();

  state = sqlite_exec(db->conn, "SELECT name FROM sqlite_master WHERE type='table'", 
      &tables_callback, &arr, &errmsg); 

  if (state != SQLITE_OK) {
    errstr = rb_str_new2(errmsg); free(errmsg);
    rb_str_cat(errstr, "(", 1); rb_str_concat(errstr, rb_str_new2(sqliteErrStr(state))); rb_str_cat(errstr, ")", 1);
    rb_raise(eDatabaseError, STR2CSTR(errstr));
  }

  return arr;
}

static void statement_mark(void *p) {
  struct sStatement *sm = (struct sStatement*) p;
  
  rb_gc_mark(sm->conn);
  rb_gc_mark(sm->statement);
}

static void statement_free(void *p) {
  struct sStatement *sm = (struct sStatement*) p;

  if (sm->result) {
    sqlite_free_table(sm->result);
    sm->result = NULL;
  }

  free(p);
}

static VALUE
Database_prepare(VALUE self, VALUE stmt)
{
  VALUE statement;
  struct sStatement *sm;
  struct sDatabase *db;

  Data_Get_Struct(self, struct sDatabase, db);

  statement = Data_Make_Struct(cStatement, struct sStatement, statement_mark, statement_free, sm);
  rb_iv_set(statement, "@attr", rb_hash_new());
  rb_iv_set(statement, "@params", rb_ary_new()); 

  rb_iv_set(statement, "@col_info", Qnil);
  rb_iv_set(statement, "@rows", rb_ary_new());

  sm->conn = self; 
  sm->statement = stmt;
  sm->result = NULL;
  sm->nrow = -1;
  sm->ncolumn = -1;
  sm->nrpc = -1;

  return statement;
}

static VALUE
Statement_bind_param(VALUE self, VALUE param, VALUE value, VALUE attribs) 
{
  if (FIXNUM_P(param)) {
    rb_ary_store(rb_iv_get(self, "@params"), FIX2INT(param)-1, value);  
  } else {
    rb_raise(eInterfaceError, "Only ? parameters supported");
  }
  return Qnil;
}

static void table_free(void *p) {
  struct sTable *tb = (struct sTable*) p;

  if (tb->result) {
    sqlite_free_table(tb->result);
    tb->result = NULL;
  }

  free(p);
}

static VALUE
Database_columns(VALUE self, VALUE tablename)
{
  struct sDatabase *db;
  struct sTable *tb;
  VALUE sql_type, table, columns, hash;
  VALUE str;
  VALUE col_name, type_name;
  int state, i, j, pos, row_index;
  char *errmsg;
  
  Data_Get_Struct(self, struct sDatabase, db);
  
  /* build SQL statement */
  sql_type = rb_str_new2("PRAGMA table_info("); 
  rb_str_concat(sql_type, tablename);
  rb_str_cat(sql_type, ")", 1);
  
  table = Data_Make_Struct(rb_cObject, struct sTable, 0, table_free, tb);
  
  /* execute SQL */
  state = sqlite_get_table(db->conn, STR2CSTR(sql_type), &tb->result, &tb->nrow, &tb->ncolumn, &errmsg);
  if (state != SQLITE_OK) {
    VALUE errstr;
    errstr = rb_str_new2(errmsg); free(errmsg);
    rb_str_cat(errstr, "(", 1); rb_str_concat(errstr, rb_str_new2(sqliteErrStr(state))); 
    rb_str_cat(errstr, ")", 1);
    rb_raise(eDatabaseError, STR2CSTR(errstr));
  }

  columns = rb_ary_new();
  for (row_index=0; row_index < tb->nrow ; row_index++) {
    pos = (row_index+1)*tb->ncolumn;
    
    hash = rb_hash_new();
    rb_ary_store(columns, row_index, hash);
    if (tb->result[pos] != NULL) {
      col_name = rb_str_new2(tb->result[pos+1]);
      rb_hash_aset(hash, rb_str_new2("name"), col_name);
      
      type_name = rb_str_new2(tb->result[pos+2]);
      rb_hash_aset(hash, rb_str_new2("type_name"), type_name);

      if (tb->result[pos+3] != NULL) {
        if (strcmp(tb->result[pos+3],"0")) {
          rb_hash_aset(hash, rb_str_new2("nullable"), Qfalse);
        } else {
          rb_hash_aset(hash, rb_str_new2("nullable"), Qtrue);
        }
      }
      
      if ((tb->result[pos+4]) != NULL) {
        str = rb_str_new2(tb->result[pos+4]);
        rb_hash_aset(hash, rb_str_new2("default"), str);
      }
    }
  }
  return columns;
}

static VALUE
Statement_execute(VALUE self) 
{
  int state, i;
  char *errmsg;
  VALUE prs[3], sql, errstr, hash;
  struct sStatement *sm;
  struct sDatabase *db;


  VALUE str, sql_type, tables, table;
  VALUE col_name, tn_cn;

  int j;
  struct sTable *tb;


  Data_Get_Struct(self, struct sStatement, sm);
  Data_Get_Struct(sm->conn, struct sDatabase, db);

  /* bind params to sql */
  prs[0] = self;
  prs[1] = sm->statement;
  prs[2] = rb_iv_get(self, "@params"); 
  sql = rb_funcall2(self, rb_intern("bind"), 3, prs);
  rubydbi_sqlite_check_sql(sql);

  rb_iv_set(sm->statement, "@params", rb_ary_new()); /* @params = [] */
  sm->row_index = 0;

  /* execute sql */
  state = sqlite_get_table(db->conn, STR2CSTR(sql), &sm->result, &sm->nrow, &sm->ncolumn, &errmsg); 
  if (state != SQLITE_OK) {
    errstr = rb_str_new2(errmsg); free(errmsg);
    rb_str_cat(errstr, "(", 1); rb_str_concat(errstr, rb_str_new2(sqliteErrStr(state))); rb_str_cat(errstr, ")", 1);
    rb_raise(eDatabaseError, STR2CSTR(errstr));
  }
  sm->nrpc = sqlite_changes(db->conn);

  /* col_info */
  tables = rb_hash_new();  /* cache the table informations here */
  if (rb_iv_get(self, "@col_info") == Qnil || RARRAY(rb_iv_get(self, "@col_info"))->len == 0) {
    rb_iv_set(self, "@col_info", rb_ary_new2(sm->ncolumn));

    for (i=0; i<sm->ncolumn;i++) { /* only first column */

      /* get informations about one column */
      hash = rb_hash_new();
      rb_ary_store(rb_iv_get(self, "@col_info"), i, hash);

      if (sm->result[i] != NULL) {
        col_name = rb_str_new2(sm->result[i]);

        /* check if column_name is a real column name or not (e.g. expression) */
        str = rb_eval_string("/^[a-zA-Z_]\\w*([.][a-zA-Z_]\\w*)?$/");

        if (rb_funcall2(col_name, rb_intern("=~"), 1, &str) == Qnil) {

          rb_hash_aset(hash, rb_str_new2("name"), col_name);

        } else {

          str   = rb_str_new2(".");
          tn_cn = rb_funcall2(col_name, rb_intern("split"), 1, &str);

          rb_hash_aset(hash, rb_str_new2("full_name"), col_name);
          rb_hash_aset(hash, rb_str_new2("table_name"), rb_ary_entry(tn_cn,0));

          if (db->full_column_names == 1) { 
            rb_hash_aset(hash, rb_str_new2("name"), col_name);
          } else {
            rb_hash_aset(hash, rb_str_new2("name"), rb_ary_entry(tn_cn,1));
          }

          /* now get type informations */

          /* only if no information about that tables has been received yet */
          if (rb_hash_aref(tables, rb_ary_entry(tn_cn, 0)) == Qnil) {

            /* build SQL statement */
            sql_type = rb_str_new2("PRAGMA table_info("); 
            rb_str_concat(sql_type, rb_ary_entry(tn_cn, 0));
            rb_str_cat(sql_type, ")", 1);

            table = Data_Make_Struct(rb_cObject, struct sTable, 0, table_free, tb);

            /* execute SQL */
            state = sqlite_get_table(db->conn, STR2CSTR(sql_type), &tb->result, &tb->nrow, &tb->ncolumn, &errmsg); 
            if (state != SQLITE_OK) {
              errstr = rb_str_new2(errmsg); free(errmsg);
              rb_str_cat(errstr, "(", 1); rb_str_concat(errstr, rb_str_new2(sqliteErrStr(state))); 
              rb_str_cat(errstr, ")", 1);
              rb_raise(eDatabaseError, STR2CSTR(errstr));
            }

            rb_hash_aset(tables, rb_ary_entry(tn_cn, 0), table); 
          }

          /* find the matching column */
          table = rb_hash_aref(tables, rb_ary_entry(tn_cn, 0));
          Data_Get_Struct(table, struct sTable, tb);

#define COLUMN_NAME 1
#define COLUMN_TYPE 2  

          for (j=0; j < tb->nrow; j++) {
            if (strcmp(tb->result[(j+1)*tb->ncolumn+COLUMN_NAME], STR2CSTR(rb_ary_entry(tn_cn, 1))) == 0) {
              rb_hash_aset(hash, rb_str_new2("type"), 
                  tb->result[(j+1)*tb->ncolumn+COLUMN_TYPE] ? rb_str_new2(tb->result[(j+1)*tb->ncolumn+COLUMN_TYPE]) : Qnil);
              break;
            }
          } /* for */

        }

      } /* if (sm->result[i] != NULL) */


    }
  }


  if (db->full_column_names == 0) { 
    str = rb_str_new2(
        "col_name_occurences = Hash.new(0)                              \n"
        "                                                               \n"
        "@col_info.each do |n|                                          \n"
        "  col_name_occurences[n['name']] += 1                          \n"
        "end                                                            \n"
        "                                                               \n"
        "col_name_occurences.each do |name, anz|                        \n"
        "  if anz > 1 then                                              \n"
        "    @col_info.each do |c|                                      \n"
        "      c['name'] = c['full_name'] if c['name'] == name          \n"
        "    end                                                        \n"
        "  end                                                          \n"
        "end                                                            \n"
        );
    rb_funcall2(self, rb_intern("eval"), 1, &str); 
  }

  return Qnil; 
}

static VALUE
Statement_cancel(VALUE self)
{
  struct sStatement *sm;
  Data_Get_Struct(self, struct sStatement, sm);

  if (sm->result) {
    sqlite_free_table(sm->result);
    sm->result = NULL;
  }

  sm->nrow = -1;
  sm->nrpc = -1;
  rb_iv_set(self, "@rows", rb_ary_new()); 
  rb_iv_set(self, "@params", rb_ary_new()); 

  return Qnil;
}


static VALUE
Statement_finish(VALUE self) 
{
  struct sStatement *sm;
  Data_Get_Struct(self, struct sStatement, sm);

  if (sm->result) {
    sqlite_free_table(sm->result);
    sm->result = NULL;
  }

  rb_iv_set(self, "@rows", Qnil); 
  rb_iv_set(self, "@params", Qnil); 

  return Qnil; 
} 

static VALUE
Statement_fetch(VALUE self) 
{
  struct sStatement *sm;
  int i, pos;
  VALUE rows, col_info;
  VALUE params[4];
  Data_Get_Struct(self, struct sStatement, sm);

  rows = rb_iv_get(self, "@rows"); 
  col_info = rb_iv_get(self, "@col_info");

  if (sm->row_index < sm->nrow) {
    pos = (sm->row_index+1)*sm->ncolumn;
    for (i=0; i<sm->ncolumn;i++) {

      if (sm->result[pos+i]) {
        /* Convert type */
        params[0] = TYPE_CONV_MAP;
        params[1] = CONVERTER; 
        params[2] = rb_str_new2(sm->result[pos+i]);
        params[3] = rb_hash_aref(rb_ary_entry(col_info, i), rb_str_new2("type")); 
        rb_ary_store(rows, i, rb_funcall2(CONVERTER_PROC, rb_intern("call"), 4, params));  
      } else {
        rb_ary_store(rows, i, Qnil); 
      } 

    }
    sm->row_index += 1;
    return rows;
  } else {
    return Qnil; 
  }
}

static VALUE
Statement_fetch_scroll(VALUE self, VALUE direction, VALUE offset) 
{
  struct sStatement *sm;
  int i, pos, get_row, dir;
  VALUE rows, params[4], col_info;

  Data_Get_Struct(self, struct sStatement, sm);

  dir = NUM2INT(direction);

  switch (dir) {
    case SQL_FETCH_NEXT:        get_row = sm->row_index; break;
    case SQL_FETCH_PRIOR:       get_row = sm->row_index-1; break;
    case SQL_FETCH_FIRST:       get_row = 0; break;
    case SQL_FETCH_LAST:        get_row = sm->nrow-1; break;
    case SQL_FETCH_ABSOLUTE:    get_row = NUM2INT(offset); break;
    case SQL_FETCH_RELATIVE:    get_row = sm->row_index+NUM2INT(offset)-1; break;
    default:
      rb_raise(eNotSupportedError, "wrong direction");
  }

  if (get_row >= 0 && get_row < sm->nrow) {
    rows = rb_iv_get(self, "@rows"); 
    col_info = rb_iv_get(self, "@col_info");

    pos = (get_row+1)*sm->ncolumn;
    for (i=0; i<sm->ncolumn;i++) {

      if (sm->result[pos+i]) {
        /* Convert type */
        params[0] = TYPE_CONV_MAP;
        params[1] = CONVERTER; 
        params[2] = rb_str_new2(sm->result[pos+i]);
        params[3] = rb_hash_aref(rb_ary_entry(col_info, i), rb_str_new2("type")); 
        rb_ary_store(rows, i, rb_funcall2(CONVERTER_PROC, rb_intern("call"), 4, params));  
      } else {
        rb_ary_store(rows, i, Qnil); 
      } 
    }

    /* position pointer */
    if (dir == SQL_FETCH_PRIOR) {
      sm->row_index = get_row;
    } else {
      sm->row_index = get_row + 1;
    }
 
    return rows;
  } else {
    if (get_row < 0) sm->row_index = 0; /* at the beginning => prev return nil */
    else if (get_row >= sm->nrow) sm->row_index = sm->nrow; /* at the end => next returns nil */
    return Qnil;
  }
}




static VALUE
Statement_column_info(VALUE self) 
{
  struct sStatement *sm;
  VALUE col_info;
  Data_Get_Struct(self, struct sStatement, sm);

  col_info = rb_iv_get(self, "@col_info");

  if (col_info == Qnil) {
    return rb_ary_new();
  } else {
    return col_info;
  }
}

static VALUE
Statement_rows(VALUE self) 
{
  struct sStatement *sm;
  Data_Get_Struct(self, struct sStatement, sm);

  if (sm->nrpc != -1) {
    return INT2NUM(sm->nrpc); 
  } else {
    return Qnil;
  }
}

static VALUE
Statement_quote(VALUE self,VALUE obj) 
{
  if (TYPE(obj)==T_OBJECT && RBASIC(obj)->klass == cTimestamp) {
    VALUE time;
    time = rb_funcall(obj, id_to_time, 0);
    time = rb_funcall(time, id_utc, 0);
    return rb_funcall(time , id_strftime, 1, rb_str_new2("'%Y/%m/%d %H:%M:%S UTC'"));
  } else {
    return rb_call_super(1, &obj);
  }
}


/* Init */
void Init_SQLite() {
  mDBD              = rb_eval_string("DBI::DBD");
  cBaseDriver       = rb_eval_string("DBI::BaseDriver");
  cBaseDatabase     = rb_eval_string("DBI::BaseDatabase");
  cBaseStatement    = rb_eval_string("DBI::BaseStatement");
  eOperationalError = rb_eval_string("DBI::OperationalError"); 
  eDatabaseError    = rb_eval_string("DBI::DatabaseError"); 
  eInterfaceError   = rb_eval_string("DBI::InterfaceError"); 
  eNotSupportedError= rb_eval_string("DBI::NotSupportedError"); 
  cTimestamp        = rb_eval_string("DBI::Timestamp");
  id_to_time        = rb_intern("to_time");
  id_utc            = rb_intern("utc");
  id_strftime       = rb_intern("strftime");
  
  mSQLite = rb_define_module_under(mDBD, "SQLite");

  /* Driver */
  cDriver    = rb_define_class_under(mSQLite, "Driver", cBaseDriver);
  rb_define_method(cDriver, "initialize", Driver_initialize, 0);
  rb_define_method(cDriver, "connect", Driver_connect, 4);
  rb_enable_super(cDriver, "initialize"); 

  /* Database */
  cDatabase  = rb_define_class_under(mSQLite, "Database", cBaseDatabase);
  rb_define_method(cDatabase, "disconnect", Database_disconnect, 0);
  rb_define_method(cDatabase, "prepare",    Database_prepare, 1);
  rb_define_method(cDatabase, "ping",       Database_ping, 0);
  rb_define_method(cDatabase, "do",         Database_do, -1);
  rb_define_method(cDatabase, "tables",     Database_tables, 0);
  rb_define_method(cDatabase, "commit",     Database_commit, 0);
  rb_define_method(cDatabase, "rollback",   Database_rollback, 0);
  rb_define_method(cDatabase, "[]",         Database_aref, 1);
  rb_define_method(cDatabase, "[]=",        Database_aset, 2);
  rb_define_method(cDatabase, "columns",    Database_columns, 1);

  rb_include_module(cDatabase, rb_eval_string("DBI::SQL::BasicBind"));

  /* Statement */
  cStatement = rb_define_class_under(mSQLite, "Statement", cBaseStatement);
  rb_define_method(cStatement, "bind_param", Statement_bind_param, 3);
  rb_define_method(cStatement, "execute", Statement_execute, 0);
  rb_define_method(cStatement, "finish", Statement_finish, 0);
  rb_define_method(cStatement, "cancel", Statement_cancel, 0);
  rb_define_method(cStatement, "fetch", Statement_fetch, 0);
  rb_define_method(cStatement, "fetch_scroll", Statement_fetch_scroll, 2);
  rb_define_method(cStatement, "column_info", Statement_column_info, 0);
  rb_define_method(cStatement, "rows",    Statement_rows, 0);
  rb_define_method(cStatement, "quote",   Statement_quote, 1);
  rb_enable_super(cStatement, "quote");

  rb_include_module(cStatement, rb_eval_string("DBI::SQL::BasicBind"));
  rb_include_module(cStatement, rb_eval_string("DBI::SQL::BasicQuote"));
  

  TYPE_CONV_MAP = rb_eval_string(
      "  [                                                                          \n"
      "    [ /^INT(EGER)?$/i,            proc {|str, c| c.as_int(str) } ],          \n"
      "    [ /^(OID|ROWID|_ROWID_)$/i,   proc {|str, c| c.as_int(str) }],           \n"
      "    [ /^(FLOAT|REAL|DOUBLE)$/i,   proc {|str, c| c.as_float(str) }],         \n"
      "    [ /^DECIMAL/i,                proc {|str, c| c.as_float(str) }],         \n"
      "    [ /^(BOOL|BOOLEAN)$/i,        proc {|str, c| c.as_bool(str) }],          \n"
      "    [ /^TIME$/i,                  proc {|str, c| c.as_time(str) }],          \n"
      "    [ /^DATE$/i,                  proc {|str, c| c.as_date(str) }],          \n"
      "    [ /^TIMESTAMP$/i,             proc {|str, c| c.as_timestamp(str) }]      \n"
      "  # [ /^(VARCHAR|CHAR|TEXT)/i,    proc {|str, c| c.as_str(str).dup } ]       \n"
      "  ]                                                                          \n"
      );

  rb_define_const(cStatement, "TYPE_CONV_MAP", TYPE_CONV_MAP);

  CONVERTER = rb_eval_string("DBI::SQL::BasicQuote::Coerce.new");
  rb_define_const(cStatement, "CONVERTER", CONVERTER);

  CONVERTER_PROC = rb_eval_string(
      "proc {|tm, cv, val, typ|                                         \n"
      "  ret = val.dup                                                  \n"
      "  tm.each do |reg, pr|                                           \n"
      "    if typ =~ reg                                                \n"
      "      ret = pr.call(val, cv)                                     \n"
      "      break                                                      \n"
      "    end                                                          \n"
      "  end                                                            \n"
      "  ret                                                            \n"
      "}                                                                \n"
      );
  rb_define_const(cStatement, "CONVERTER_PROC", CONVERTER_PROC);

  
}


