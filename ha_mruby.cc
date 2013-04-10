/* Copyright (c) 2004, 2012, Oracle and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file ha_mruby.cc

  @brief
  The ha_mruby engine is a stubbed storage engine for mruby purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  /storage/mruby/ha_mruby.h.

  @details
  ha_mruby will let you create/open/delete tables, but
  nothing further (for mruby, indexes are not supported nor can data
  be stored in the table). Use this mruby as a template for
  implementing the same functionality in your own storage engine. You
  can enable the mruby storage engine in your build by doing the
  following during your build process:<br> ./configure
  --with-mruby-storage-engine

  Once this is done, MySQL will let you create tables with:<br>
  CREATE TABLE <table name> (...) ENGINE=MRUBY;

  The mruby storage engine is set up to use table locks. It
  implements an mruby "SHARE" that is inserted into a hash by table
  name. You can use this to store information of state that any
  mruby handler object will be able to see when it is using that
  table.

  Please read the object definition in ha_mruby.h before reading the rest
  of this file.

  @note
  When you create an MRUBY table, the MySQL Server creates a table .frm
  (format) file in the database directory, using the table name as the file
  name as is customary with MySQL. No other files are created. To get an idea
  of what occurs, here is an mruby select that would do a scan of an entire
  table:

  @code
  ha_mruby::store_lock
  ha_mruby::external_lock
  ha_mruby::info
  ha_mruby::rnd_init
  ha_mruby::extra
  ENUM HA_EXTRA_CACHE        Cache record in HA_rrnd()
  ha_mruby::rnd_next
  ha_mruby::rnd_next
  ha_mruby::rnd_next
  ha_mruby::rnd_next
  ha_mruby::rnd_next
  ha_mruby::rnd_next
  ha_mruby::rnd_next
  ha_mruby::rnd_next
  ha_mruby::rnd_next
  ha_mruby::extra
  ENUM HA_EXTRA_NO_CACHE     End caching of records (def)
  ha_mruby::external_lock
  ha_mruby::extra
  ENUM HA_EXTRA_RESET        Reset database to after open
  @endcode

  Here you see that the mruby storage engine has 9 rows called before
  rnd_next signals that it has reached the end of its data. Also note that
  the table in question was already opened; had it not been open, a call to
  ha_mruby::open() would also have been necessary. Calls to
  ha_mruby::extra() are hints as to what will be occuring to the request.

  A Longer Example can be found called the "Skeleton Engine" which can be 
  found on TangentOrg. It has both an engine and a full build environment
  for building a pluggable storage engine.

  Happy coding!<br>
    -Brian
*/

#define MYSQL_SERVER 1
#include "sql_priv.h"
#include "sql_base.h"
#include "ha_mruby.h"
#include "probes_mysql.h"
#include "sql_plugin.h"

#include <mruby/compile.h>
#include <mruby/proc.h>
#include <mruby/string.h>

#include <string>
#include <sstream>

struct timeline_t {
  unsigned user_id;
  unsigned status_id;
  std::string *json;
  timeline_t() {}
  timeline_t(unsigned u, unsigned s, std::string *j) : user_id(u), status_id(s), json(j) {}
};

std::vector<timeline_t> timeline;

static handler *mruby_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root);

handlerton *mruby_hton;

/* Interface to mysqld, to check system tables supported by SE */
static const char* mruby_system_database();
static bool mruby_is_supported_system_table(const char *db,
                                      const char *table_name,
                                      bool is_sql_layer_system_table);
#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key ex_key_mutex_mruby_share_mutex;

static PSI_mutex_info all_mruby_mutexes[]=
{
  { &ex_key_mutex_mruby_share_mutex, "mruby_share::mutex", 0}
};

static void init_mruby_psi_keys()
{
  const char* category= "mruby";
  int count;

  count= array_elements(all_mruby_mutexes);
  mysql_mutex_register(category, all_mruby_mutexes, count);
}
#endif

mruby_share::mruby_share()
{
  thr_lock_init(&lock);
  mysql_mutex_init(ex_key_mutex_mruby_share_mutex,
                   &mutex, MY_MUTEX_INIT_FAST);
}


static int mruby_init_func(void *p)
{
  DBUG_ENTER("mruby_init_func");

#ifdef HAVE_PSI_INTERFACE
  init_mruby_psi_keys();
#endif

  mruby_hton= (handlerton *)p;
  mruby_hton->state=                     SHOW_OPTION_YES;
  mruby_hton->create=                    mruby_create_handler;
  mruby_hton->flags=                     HTON_CAN_RECREATE;
  mruby_hton->system_database=   mruby_system_database;
  mruby_hton->is_supported_system_table= mruby_is_supported_system_table;

  DBUG_RETURN(0);
}


/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each mruby handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

mruby_share *ha_mruby::get_share(const char *table_name, TABLE *table)
{
  mruby_share *tmp_share;

  DBUG_ENTER("ha_mruby::get_share()");

  lock_shared_ha_data();

  if (!(tmp_share= static_cast<mruby_share*>(get_ha_share_ptr())))
  {
    tmp_share= new mruby_share;
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share*>(tmp_share));
  }
err:
  unlock_shared_ha_data();
  DBUG_RETURN(tmp_share);
}


static handler* mruby_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root)
{
  return new (mem_root) ha_mruby(hton, table);
}

ha_mruby::ha_mruby(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg)
{}


/**
  @brief
  If frm_error() is called then we will use this to determine
  the file extensions that exist for the storage engine. This is also
  used by the default rename_table and delete_table method in
  handler.cc.

  For engines that have two file name extentions (separate meta/index file
  and data file), the order of elements is relevant. First element of engine
  file name extentions array should be meta/index file extention. Second
  element - data file extention. This order is assumed by
  prepare_for_repair() when REPAIR TABLE ... USE_FRM is issued.

  @see
  rename_table method in handler.cc and
  delete_table method in handler.cc
*/

static const char *ha_mruby_exts[] = {
  NullS
};

const char **ha_mruby::bas_ext() const
{
  return ha_mruby_exts;
}

/*
  Following handler function provides access to
  system database specific to SE. This interface
  is optional, so every SE need not implement it.
*/
const char* ha_mruby_system_database= NULL;
const char* mruby_system_database()
{
  return ha_mruby_system_database;
}

/*
  List of all system tables specific to the SE.
  Array element would look like below,
     { "<database_name>", "<system table name>" },
  The last element MUST be,
     { (const char*)NULL, (const char*)NULL }

  This array is optional, so every SE need not implement it.
*/
static st_system_tablename ha_mruby_system_tables[]= {
  {(const char*)NULL, (const char*)NULL}
};

/**
  @brief Check if the given db.tablename is a system table for this SE.

  @param db                         Database name to check.
  @param table_name                 table name to check.
  @param is_sql_layer_system_table  if the supplied db.table_name is a SQL
                                    layer system table.

  @return
    @retval TRUE   Given db.table_name is supported system table.
    @retval FALSE  Given db.table_name is not a supported system table.
*/
static bool mruby_is_supported_system_table(const char *db,
                                              const char *table_name,
                                              bool is_sql_layer_system_table)
{
  st_system_tablename *systab;

  // Does this SE support "ALL" SQL layer system tables ?
  if (is_sql_layer_system_table)
    return false;

  // Check if this is SE layer system tables
  systab= ha_mruby_system_tables;
  while (systab && systab->db)
  {
    if (systab->db == db &&
        strcmp(systab->tablename, table_name) == 0)
      return true;
    systab++;
  }

  return false;
}


/**
  @brief
  Used for opening tables. The name will be the name of the file.

  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).

  Called from handler.cc by handler::ha_open(). The server opens all tables by
  calling ha_open() which then calls the handler specific open().

  @see
  handler::ha_open() in handler.cc
*/

int ha_mruby::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("ha_mruby::open");

  if (!(share = get_share(name, table)))
    DBUG_RETURN(1);
  thr_lock_data_init(&share->lock,&lock,NULL);

  DBUG_RETURN(0);
}


/**
  @brief
  Closes a table.

  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
  only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.

  For sql_base.cc look at close_data_tables().

  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_mruby::close(void)
{
  DBUG_ENTER("ha_mruby::close");
  DBUG_RETURN(0);
}


/**
  @brief
  write_row() inserts a row. No extra() hint is given currently if a bulk load
  is happening. buf() is a byte array of data. You can use the field
  information to extract the data from the native byte array type.

  @details
  Example of this would be:
  @code
  for (Field **field=table->field ; *field ; field++)
  {
    ...
  }
  @endcode

  See ha_tina.cc for an mruby of extracting all of the data as strings.
  ha_berekly.cc has an mruby of how to store it intact by "packing" it
  for ha_berkeley's own native storage type.

  See the note for update_row() on auto_increments. This case also applies to
  write_row().

  Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.

  @see
  item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc and sql_update.cc
*/

int ha_mruby::write_row(uchar *buf)
{
  DBUG_ENTER("ha_mruby::write_row");
  /*
    Example of a successful write_row. We don't store the data
    anywhere; they are thrown away. A real implementation will
    probably need to do something with 'buf'. We report a success
    here, to pretend that the insert was successful.
  */
  DBUG_RETURN(0);
}


/**
  @brief
  Yes, update_row() does what you expect, it updates a row. old_data will have
  the previous row record in it, while new_data will have the newest data in it.
  Keep in mind that the server can do updates based on ordering if an ORDER BY
  clause was used. Consecutive ordering is not guaranteed.

  @details
  Currently new_data will not have an updated auto_increament record. You can
  do this for mruby by doing:

  @code

  if (table->next_number_field && record == table->record[0])
    update_auto_increment();

  @endcode

  Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.

  @see
  sql_select.cc, sql_acl.cc, sql_update.cc and sql_insert.cc
*/
int ha_mruby::update_row(const uchar *old_data, uchar *new_data)
{

  DBUG_ENTER("ha_mruby::update_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  This will delete a row. buf will contain a copy of the row to be deleted.
  The server will call this right after the current row has been called (from
  either a previous rnd_nexT() or index call).

  @details
  If you keep a pointer to the last row or can access a primary key it will
  make doing the deletion quite a bit easier. Keep in mind that the server does
  not guarantee consecutive deletions. ORDER BY clauses can be used.

  Called in sql_acl.cc and sql_udf.cc to manage internal table
  information.  Called in sql_delete.cc, sql_insert.cc, and
  sql_select.cc. In sql_select it is used for removing duplicates
  while in insert it is used for REPLACE calls.

  @see
  sql_acl.cc, sql_udf.cc, sql_delete.cc, sql_insert.cc and sql_select.cc
*/

int ha_mruby::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_mruby::delete_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Positions an index cursor to the index specified in the handle. Fetches the
  row if available. If the key value is null, begin at the first key of the
  index.
*/

int ha_mruby::index_read_map(uchar *buf, const uchar *key,
                               key_part_map keypart_map __attribute__((unused)),
                               enum ha_rkey_function find_flag
                               __attribute__((unused)))
{
  int rc;
  DBUG_ENTER("ha_mruby::index_read");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  Used to read forward through the index.
*/

int ha_mruby::index_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_mruby::index_next");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  Used to read backwards through the index.
*/

int ha_mruby::index_prev(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_mruby::index_prev");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  index_first() asks for the first key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_mruby::index_first(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_mruby::index_first");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  index_last() asks for the last key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_mruby::index_last(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_mruby::index_last");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the mruby in the introduction at the top of this file to see when
  rnd_init() is called.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_mruby::rnd_init(bool scan)
{
  DBUG_ENTER("ha_mruby::rnd_init");
  share->n_max = timeline.size();
  share->n_cur = 0;
  DBUG_RETURN(0);
}

int ha_mruby::rnd_end()
{
  DBUG_ENTER("ha_mruby::rnd_end");
  DBUG_RETURN(0);
}


/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_mruby::rnd_next(uchar *buf)
{
  int rc;
  my_bitmap_map *org_bitmap;
  bool read_all;
  int n_max = share->n_max;
  int n_cur = share->n_cur;
  timeline_t tl;
  int n;
  struct mrb_parser_state *p;
  mrb_state *mrb;

  DBUG_ENTER("ha_mruby::rnd_next");

  if (n_cur >= n_max)
    goto end_of_timeline;

  tl = timeline[n_cur];
  share->n_cur++;

  mrb = mrb_open();
  p = mrb_parse_string(mrb, table->s->comment.str, NULL);
  if (!p) {
    goto parse_error; //return mrb_undef_value();
  }
  else if (!p->tree || p->nerr) {
    if (p->capture_errors) {
      char buf[256];

      n = snprintf(buf, sizeof(buf), "line %d: %s\n",
      p->error_buffer[0].lineno, p->error_buffer[0].message);
      mrb->exc = mrb_obj_ptr(mrb_exc_new(mrb, E_SYNTAX_ERROR, buf, n));
      mrb_parser_free(p);
      goto parse_error; //return mrb_undef_value();
    }
    else {
      static const char msg[] = "syntax error";
      mrb->exc = mrb_obj_ptr(mrb_exc_new(mrb, E_SYNTAX_ERROR, msg, sizeof(msg) - 1));
      mrb_parser_free(p);
      goto parse_error; //return mrb_undef_value();
    }
  }

  n = mrb_generate_code(mrb, p);

  mrb_parser_free(p);

  //int ai;
  //ai = mrb_gc_arena_save(mrb);
  mrb_run(mrb, mrb_proc_new(mrb, mrb->irep[n]), mrb_top_self(mrb));
  //mrb_gc_arena_restore(mrb, ai);

  if (mrb->exc) {
    mrb_value obj = mrb_funcall(mrb, mrb_obj_value(mrb->exc), "inspect", 0);
    if (mrb_type(obj) == MRB_TT_STRING) {
      struct RString *str = mrb_str_ptr(obj);
      DBUG_PRINT("info", ("ha_mruby::rnd_next():error: %s", str->ptr));
    }
  }

  /* We must read all columns in case a table is opened for update */
  read_all = !bitmap_is_clear_all(table->write_set);
  /* Avoid asserts in ::store() for columns that are not going to be updated */
  org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);

  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);

  for (Field **field = table->field ; *field ; field++) {
    if (read_all || bitmap_is_set(table->read_set, (*field)->field_index)) {
      mrb_value fld = mrb_str_new_cstr(mrb, (*field)->field_name);
      mrb_value json = mrb_str_new_cstr(mrb, tl.json->c_str());
      mrb_value json_value = mrb_funcall(mrb, mrb_top_self(mrb), "json_get", 2, json, fld);
      if (strcmp((*field)->field_name, "id") == 0) {
        if (mrb_float_p(json_value)) {
          (*field)->set_notnull();
          (*field)->store((int)mrb_float(json_value), TRUE);
        } else {
          (*field)->set_null();
        }
      } else if (strcmp((*field)->field_name, "data") == 0) {
        if (mrb_string_p(json_value)) {
          struct RString *data = mrb_str_ptr(json_value);
          (*field)->set_notnull();
          (*field)->store(STRING_WITH_LEN(data->ptr), &my_charset_utf8_bin);
        } else {
          (*field)->set_null();
        }
      } else if (strcmp((*field)->field_name, "user_id") == 0) {
        (*field)->set_notnull();
        (*field)->store(tl.user_id, TRUE);
      } else if (strcmp((*field)->field_name, "status_id") == 0) {
        (*field)->set_notnull();
        (*field)->store(tl.status_id, TRUE);
      } else {
        (*field)->set_null();
      }
    }
  }

  dbug_tmp_restore_column_map(table->write_set, org_bitmap);

  MYSQL_READ_ROW_DONE(0);
  DBUG_RETURN(0);

end_of_timeline:
parse_error:
  rc = HA_ERR_END_OF_FILE;
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
  @code
  my_store_ptr(ref, ref_length, current_position);
  @endcode

  @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.

  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

  @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void ha_mruby::position(const uchar *record)
{
  DBUG_ENTER("ha_mruby::position");
  DBUG_VOID_RETURN;
}


/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.

  @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_mruby::rnd_pos(uchar *buf, uchar *pos)
{
  int rc;
  DBUG_ENTER("ha_mruby::rnd_pos");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.

  @details
  Currently this table handler doesn't implement most of the fields really needed.
  SHOW also makes use of this data.

  You will probably want to have the following in your code:
  @code
  if (records < 2)
    records = 2;
  @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.

  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_table.cc, sql_union.cc, and sql_update.cc.

  @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc, sql_delete.cc,
  sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_table.cc,
  sql_union.cc and sql_update.cc
*/
int ha_mruby::info(uint flag)
{
  DBUG_ENTER("ha_mruby::info");
  DBUG_RETURN(0);
}


/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.

    @see
  ha_innodb.cc
*/
int ha_mruby::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("ha_mruby::extra");
  DBUG_RETURN(0);
}


/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases where
  the optimizer realizes that all rows will be removed as a result of an SQL statement.

  @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_select_lex_unit::exec().

  @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_select_lex_unit::exec() in sql_union.cc.
*/
int ha_mruby::delete_all_rows()
{
  DBUG_ENTER("ha_mruby::delete_all_rows");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Used for handler specific truncate table.  The table is locked in
  exclusive mode and handler is responsible for reseting the auto-
  increment counter.

  @details
  Called from Truncate_statement::handler_truncate.
  Not used if the handlerton supports HTON_CAN_RECREATE, unless this
  engine can be used as a partition. In this case, it is invoked when
  a particular partition is to be truncated.

  @see
  Truncate_statement in sql_truncate.cc
  Remarks in handler::truncate.
*/
int ha_mruby::truncate()
{
  DBUG_ENTER("ha_mruby::truncate");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to understand
  this.

  @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().

  @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
*/
int ha_mruby::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_mruby::external_lock");
  DBUG_RETURN(0);
}


/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for mruby, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

  @see
  get_lock_data() in lock.cc
*/
THR_LOCK_DATA **ha_mruby::store_lock(THD *thd,
                                       THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type=lock_type;
  *to++= &lock;
  return to;
}


/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.

  @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.

  @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_mruby::delete_table(const char *name)
{
  DBUG_ENTER("ha_mruby::delete_table");
  /* This is not implemented but we want someone to be able that it works. */
  DBUG_RETURN(0);
}


/**
  @brief
  Renames a table from one name to another via an alter table call.

  @details
  If you do not implement this, the default rename_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from sql_table.cc by mysql_rename_table().

  @see
  mysql_rename_table() in sql_table.cc
*/
int ha_mruby::rename_table(const char * from, const char * to)
{
  DBUG_ENTER("ha_mruby::rename_table ");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.

  @details
  end_key may be empty, in which case determine if start_key matches any rows.

  Called from opt_range.cc by check_quick_keys().

  @see
  check_quick_keys() in opt_range.cc
*/
ha_rows ha_mruby::records_in_range(uint inx, key_range *min_key,
                                     key_range *max_key)
{
  DBUG_ENTER("ha_mruby::records_in_range");
  DBUG_RETURN(10);                         // low number to force index usage
}


/**
  @brief
  create() is called to create a database. The variable name will have the name
  of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @see
  ha_create_table() in handle.cc
*/

int ha_mruby::create(const char *name, TABLE *table_arg,
                       HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("ha_mruby::create");
  /*
    This is not implemented but we want someone to be able to see that it
    works.
  */
  DBUG_RETURN(0);
}


struct st_mysql_storage_engine mruby_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static ulong srv_enum_var= 0;
static ulong srv_ulong_var= 0;

const char *enum_var_names[]=
{
  "e1", "e2", NullS
};

TYPELIB enum_var_typelib=
{
  array_elements(enum_var_names) - 1, "enum_var_typelib",
  enum_var_names, NULL
};

static MYSQL_SYSVAR_ENUM(
  enum_var,                       // name
  srv_enum_var,                   // varname
  PLUGIN_VAR_RQCMDARG,            // opt
  "Sample ENUM system variable.", // comment
  NULL,                           // check
  NULL,                           // update
  0,                              // def
  &enum_var_typelib);             // typelib

static MYSQL_SYSVAR_ULONG(
  ulong_var,
  srv_ulong_var,
  PLUGIN_VAR_RQCMDARG,
  "0..1000",
  NULL,
  NULL,
  8,
  0,
  1000,
  0);

static struct st_mysql_sys_var* mruby_system_variables[]= {
  MYSQL_SYSVAR(enum_var),
  MYSQL_SYSVAR(ulong_var),
  NULL
};

// this is an mruby of SHOW_FUNC and of my_snprintf() service
static int show_func_mruby(MYSQL_THD thd, struct st_mysql_show_var *var,
                             char *buf)
{
  var->type= SHOW_CHAR;
  var->value= buf; // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  my_snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
              "enum_var is %lu, ulong_var is %lu, %.6b", // %b is MySQL extension
              srv_enum_var, srv_ulong_var, "really");
  return 0;
}

static struct st_mysql_show_var func_status[]=
{
  {"mruby_func_mruby",  (char *)show_func_mruby, SHOW_FUNC},
  {0,0,SHOW_UNDEF}
};

mysql_declare_plugin(mruby)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &mruby_storage_engine,
  "MRUBY",
  "Ryuta Kamizono",
  "mruby storage engine",
  PLUGIN_LICENSE_GPL,
  mruby_init_func,                            /* Plugin Init */
  NULL,                                         /* Plugin Deinit */
  0x0001 /* 0.1 */,
  func_status,                                  /* status variables */
  mruby_system_variables,                     /* system variables */
  NULL,                                         /* config options */
  0,                                            /* flags */
}
mysql_declare_plugin_end;

extern "C" {
my_bool mysqlcasual_init(UDF_INIT* initid, UDF_ARGS* args, char* message);
void mysqlcasual_deinit(UDF_INIT* initid);
char* mysqlcasual(UDF_INIT* initid, UDF_ARGS* args, char* result, unsigned long* length, char* is_null, char* error);
}

static Field *get_field(TABLE *table, const char *name) {
  Field **f;
  for (f = table->field; *f != NULL; f++) {
    if (strcmp((*f)->field_name, name) == 0) {
      return *f;
    }
  }
  return NULL;
}

static KEY *index_init(TABLE *table, const char *name, bool sorted) {
  for (uint keynr = 0; keynr < table->s->keys; keynr++) {
    if (strcmp(table->s->key_info[keynr].name, name) == 0) {
      if (table->file->ha_index_init(keynr, sorted) != 0) {
        return NULL;
      }
      return table->s->key_info + keynr;
    }
  }
  return NULL;
}

my_bool mysqlcasual_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
  initid->ptr = (char*)(void*)new std::string();
  initid->const_item = 1;
  return 0;
}

void mysqlcasual_deinit(UDF_INIT* initid)
{
  delete (std::string*)(void*)initid->ptr;
}

char* mysqlcasual(UDF_INIT* initid, UDF_ARGS* args, char* result, unsigned long* length, char* is_null, char* error)
{
  std::string* out = (std::string*)(void*)initid->ptr;
  std::ostringstream ss;

  TABLE_LIST *table_list;
  THD *thd = current_thd;
  uint flags = (
    MYSQL_OPEN_IGNORE_GLOBAL_READ_LOCK |
    MYSQL_LOCK_IGNORE_GLOBAL_READ_ONLY |
    MYSQL_OPEN_IGNORE_FLUSH |
    MYSQL_LOCK_IGNORE_TIMEOUT |
    MYSQL_OPEN_GET_NEW_TABLE
  );

  close_thread_tables(thd);
  table_list = (TABLE_LIST*) thd->alloc(sizeof(TABLE_LIST));
  table_list->init_one_table(STRING_WITH_LEN("test"), STRING_WITH_LEN("timeline"), "timeline", TL_READ);

  TABLE *table = open_n_lock_single_table(thd, table_list, table_list->lock_type, flags);

  Field *status_id, *user_id, *json;
  status_id = get_field(table, "status_id");
  user_id   = get_field(table, "user_id");
  json      = get_field(table, "json");
  //table->clear_column_bitmaps();
  //bitmap_set_bit(table->read_set, status_id->field_index);
  //bitmap_set_bit(table->read_set, user_id->field_index);
  //bitmap_set_bit(table->read_set, json->field_index);
  table->use_all_columns();

  KEY *key = index_init(table, "PRIMARY", true);

  uchar *keybuff = (uchar*)alloca(key->key_length);
  memset(keybuff, 0, key->key_length);
  key_part_map keypart_map = make_keypart_map(0);

  table->file->ha_index_read_map(table->record[0], keybuff, keypart_map, HA_READ_KEY_OR_NEXT);

  timeline.clear();
  do {
    unsigned uid = user_id->val_int();
    unsigned sid = status_id->val_int();
    char str_buffer[1024];
    String str(str_buffer, sizeof(str_buffer), &my_charset_utf8_bin);
    json->val_str(&str);
    str.copy();
    timeline.push_back(timeline_t(uid, sid, new std::string(str.ptr())));
    /*
    ss << uid << ":" << sid; ss << ":";
    ss << str.ptr() << std::endl;
    */
  } while (table->file->ha_index_next(table->record[0]) == 0);

  table->file->ha_index_end();
  thd->locked_tables_list.unlock_locked_tables(thd);

  ss << "#mysqlcasual";
  *out = ss.str();
  *length = out->size();

  if (*length > 0) {
    return &(*out)[0];
  }

  *is_null = 1;
  return NULL;
}
