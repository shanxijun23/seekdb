/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ob_resolver.h"
#include "share/ob_server_struct.h"
#include "common/mysqlclient/ob_mysql_proxy.h"
#include "common/mysqlclient/ob_mysql_result.h"
#include "lib/utility/utility.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "lib/utility/ob_smart_call.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "sql/resolver/cmd/ob_alter_system_resolver.h"
#include "sql/resolver/dml/ob_insert_resolver.h"
#include "sql/resolver/dml/ob_update_resolver.h"
#include "sql/resolver/dml/ob_delete_resolver.h"
#include "sql/resolver/tcl/ob_start_trans_resolver.h"
#include "sql/resolver/tcl/ob_end_trans_resolver.h"
#include "sql/resolver/prepare/ob_prepare_resolver.h"
#include "sql/resolver/prepare/ob_execute_resolver.h"
#include "sql/resolver/prepare/ob_deallocate_resolver.h"
#include "sql/resolver/ddl/ob_create_table_resolver.h"
#include "sql/resolver/ddl/ob_rename_table_resolver.h"
#include "sql/resolver/ddl/ob_truncate_table_resolver.h"
#include "sql/resolver/ddl/ob_fork_table_resolver.h"
#include "sql/resolver/ddl/ob_fork_database_resolver.h"
#include "sql/resolver/ddl/ob_create_table_like_resolver.h"
#include "sql/resolver/ddl/ob_alter_table_resolver.h"
#include "sql/resolver/ddl/ob_drop_table_resolver.h"
#include "sql/resolver/ddl/ob_create_index_resolver.h"
#include "sql/resolver/ddl/ob_drop_index_resolver.h"
#include "sql/resolver/ddl/ob_create_database_resolver.h"
#include "sql/resolver/ddl/ob_alter_database_resolver.h"
#include "sql/resolver/ddl/ob_use_database_resolver.h"
#include "sql/resolver/ddl/ob_drop_database_resolver.h"
#include "sql/resolver/ddl/ob_create_view_resolver.h"
#include "sql/resolver/ddl/ob_explain_resolver.h"
#include "sql/resolver/ddl/ob_create_outline_resolver.h"
#include "sql/resolver/ddl/ob_alter_outline_resolver.h"
#include "sql/resolver/ddl/ob_drop_outline_resolver.h"
#include "sql/resolver/ddl/ob_drop_routine_resolver.h"
#include "sql/resolver/ddl/ob_trigger_resolver.h"
#include "sql/resolver/ddl/ob_optimize_resolver.h"
#include "ddl/ob_drop_routine_resolver.h"
#include "ddl/ob_alter_routine_resolver.h"
#include "sql/resolver/ddl/ob_create_package_resolver.h"
#include "sql/resolver/ddl/ob_drop_package_resolver.h"
#include "sql/resolver/ddl/ob_recyclebin_restore_resolver.h"
#include "sql/resolver/ddl/ob_purge_resolver.h"
#include "sql/resolver/ddl/ob_analyze_stmt_resolver.h"
#include "sql/resolver/ddl/ob_purge_resolver.h"
#include "sql/resolver/ddl/ob_set_comment_resolver.h"
#include "sql/resolver/ddl/ob_lock_table_resolver.h"
#include "sql/resolver/dml/ob_insert_resolver.h"
#include "sql/resolver/dml/ob_update_resolver.h"
#include "sql/resolver/dml/ob_delete_resolver.h"
#include "sql/resolver/dcl/ob_create_user_resolver.h"
#include "sql/resolver/dcl/ob_drop_user_resolver.h"
#include "sql/resolver/dcl/ob_rename_user_resolver.h"
#include "sql/resolver/dcl/ob_set_password_resolver.h"
#include "sql/resolver/dcl/ob_lock_user_resolver.h"
#include "sql/resolver/dcl/ob_grant_resolver.h"
#include "sql/resolver/dcl/ob_revoke_resolver.h"
#include "sql/resolver/dcl/ob_create_role_resolver.h"
#include "sql/resolver/dcl/ob_drop_role_resolver.h"
#include "sql/resolver/dcl/ob_alter_user_role_resolver.h"
#include "sql/resolver/tcl/ob_start_trans_resolver.h"
#include "sql/resolver/tcl/ob_end_trans_resolver.h"
#include "tcl/ob_savepoint_resolver.h"
#include "sql/resolver/cmd/ob_variable_set_resolver.h"
#include "sql/resolver/cmd/ob_show_resolver.h"
#include "sql/resolver/cmd/ob_diff_table_resolver.h"
#include "sql/resolver/cmd/ob_merge_table_resolver.h"
#include "sql/resolver/cmd/ob_kill_resolver.h"
#include "sql/resolver/cmd/ob_set_names_resolver.h"
#include "sql/resolver/cmd/ob_set_transaction_resolver.h"
#include "sql/resolver/cmd/ob_empty_query_resolver.h"
#include "sql/resolver/cmd/ob_anonymous_block_resolver.h"
#include "sql/resolver/cmd/ob_call_procedure_resolver.h"
#include "sql/resolver/cmd/ob_load_data_resolver.h"
#include "sql/resolver/prepare/ob_execute_resolver.h"
#include "sql/resolver/prepare/ob_deallocate_resolver.h"
#include "sql/resolver/ddl/ob_purge_resolver.h"
#include "sql/resolver/ddl/ob_set_comment_resolver.h"
#include "sql/resolver/expr/ob_raw_expr_wrap_enum_set.h"
#include "sql/resolver/cmd/ob_get_diagnostics_resolver.h"
#include "sql/resolver/dcl/ob_alter_role_resolver.h"
#include "sql/pl/ob_pl_package.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
ObResolver::ObResolver(ObResolverParams &params)
    : params_(params)
{

}

ObResolver::~ObResolver()
{
}

template <typename ResolverType>
int ObResolver::stmt_resolver_func(ObResolverParams &params, const ParseNode &parse_tree, ObStmt *&stmt)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ResolverType, stmt_resolver, params) {
    if (OB_FAIL(SMART_CALL(stmt_resolver.resolve(parse_tree)))) {
    }
    stmt = stmt_resolver.get_basic_stmt();
  }
  return ret;
}

template <typename SelectResolverType>
int ObResolver::select_stmt_resolver_func(ObResolverParams &params, const ParseNode &parse_tree, ObStmt *&stmt)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(SelectResolverType, stmt_resolver, params) {
    stmt_resolver.set_calc_found_rows(true);
    stmt_resolver.set_has_top_limit(true);
    if (OB_FAIL(SMART_CALL(stmt_resolver.resolve(parse_tree)))) {
    }
    stmt = stmt_resolver.get_basic_stmt();
  }
  return ret;
}

namespace {

static int ai_strict_check_column(const uint64_t table_id, const uint64_t column_id)
{
  int ret = OB_SUCCESS;
  ObSqlString meta_sql;
  if (OB_FAIL(meta_sql.append_fmt("select database_name, table_name, column_name "
                                  "from oceanbase.__all_ai_column where table_id = %lu and column_id = %lu",
                                  table_id, column_id))) {
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_ISNULL(GCTX.sql_proxy_)) {
        ret = OB_NOT_INIT;
      } else if (OB_FAIL(GCTX.sql_proxy_->read(result, meta_sql.ptr()))) {
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        sqlclient::ObMySQLResult *mysql_result = result.get_result();
        int next_ret = mysql_result->next();
        if (OB_ITER_END == next_ret) {
          LOG_WARN("ai strict read column is not ai column", K(table_id), K(column_id));
        } else if (OB_FAIL(next_ret)) {
          ret = next_ret;
        } else {
          ObString db_name;
          ObString table_name;
          ObString col_name;
          if (OB_FAIL(mysql_result->get_varchar(static_cast<int64_t>(0), db_name))) {
          } else if (OB_FAIL(mysql_result->get_varchar(static_cast<int64_t>(1), table_name))) {
          } else if (OB_FAIL(mysql_result->get_varchar(static_cast<int64_t>(2), col_name))) {
          } else {
            ObSqlString null_sql;
            if (OB_FAIL(null_sql.append_fmt("select 1 from `%.*s`.`%.*s` where `%.*s` is null limit 1",
                                        static_cast<int>(db_name.length()), db_name.ptr(),
                                        static_cast<int>(table_name.length()), table_name.ptr(),
                                        static_cast<int>(col_name.length()), col_name.ptr()))) {
            } else {
              SMART_VAR(ObMySQLProxy::MySQLResult, null_result) {
                if (OB_FAIL(GCTX.sql_proxy_->read(null_result, null_sql.ptr()))) {
                } else if (OB_ISNULL(null_result.get_result())) {
                  ret = OB_ERR_UNEXPECTED;
                } else {
                  sqlclient::ObMySQLResult *null_mysql_result = null_result.get_result();
                  int row_ret = null_mysql_result->next();
                  if (OB_ITER_END == row_ret) {
                    // all rows of this AI column are ready
                  } else if (OB_FAIL(row_ret)) {
                    ret = row_ret;
                  } else {
                    LOG_WARN("ai strict read found unfinished row", K(table_id), K(column_id),
                             K(db_name), K(table_name), K(col_name));
                    ret = OB_ERR_AI_COLUMN_UNFINISHED;
                    LOG_USER_ERROR(OB_ERR_AI_COLUMN_UNFINISHED, table_id, column_id);
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

static bool ai_strict_visited(const ObIArray<const ObDMLStmt *> &visited, const ObDMLStmt *stmt)
{
  bool found = false;
  for (int64_t i = 0; !found && i < visited.count(); ++i) {
    if (visited.at(i) == stmt) {
      found = true;
    }
  }
  return found;
}

static int ai_strict_collect_column_ids(const ObDMLStmt &dml_stmt, ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  const ObIArray<ColumnItem> &column_items = dml_stmt.get_column_items();
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
    const ColumnItem &item = column_items.at(i);
    const ObColumnRefRawExpr *ref_expr = item.get_expr();
    uint64_t column_id = (NULL != ref_expr) ? ref_expr->get_column_id() : item.column_id_;
    if (OB_INVALID_ID == column_id) {
      continue;
    }
    bool duplicated = false;
    for (int64_t k = 0; !duplicated && k < column_ids.count(); ++k) {
      if (column_ids.at(k) == column_id) {
        duplicated = true;
      }
    }
    if (!duplicated && OB_FAIL(column_ids.push_back(column_id))) {
    }
  }
  return ret;
}

static bool ai_strict_column_referenced(const ObIArray<uint64_t> &column_ids, const uint64_t column_id)
{
  bool found = false;
  for (int64_t i = 0; !found && i < column_ids.count(); ++i) {
    if (column_ids.at(i) == column_id) {
      found = true;
    }
  }
  return found;
}

static int ai_strict_has_unfinished_row(const ObString &db_name,
                                        const ObString &table_name,
                                        const ObString &column_name,
                                        bool &unfinished)
{
  int ret = OB_SUCCESS;
  unfinished = false;
  ObSqlString null_sql;
  if (OB_FAIL(null_sql.append_fmt("select 1 from `%.*s`.`%.*s` where `%.*s` is null limit 1",
                                  static_cast<int>(db_name.length()), db_name.ptr(),
                                  static_cast<int>(table_name.length()), table_name.ptr(),
                                  static_cast<int>(column_name.length()), column_name.ptr()))) {
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, null_result) {
      if (OB_ISNULL(GCTX.sql_proxy_)) {
        ret = OB_NOT_INIT;
      } else if (OB_FAIL(GCTX.sql_proxy_->read(null_result, null_sql.ptr()))) {
      } else if (OB_ISNULL(null_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        sqlclient::ObMySQLResult *null_mysql_result = null_result.get_result();
        int row_ret = null_mysql_result->next();
        if (OB_ITER_END == row_ret) {
          // all rows of this AI column are ready
        } else if (OB_FAIL(row_ret)) {
          ret = row_ret;
        } else {
          unfinished = true;
        }
      }
    }
  }
  return ret;
}

static int ai_strict_check_stmt(ObDMLStmt &dml_stmt, ObIArray<const ObDMLStmt *> &visited)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 16> referenced_ids;
  if (OB_FAIL(ai_strict_collect_column_ids(dml_stmt, referenced_ids))) {
  } else {
    const ObIArray<TableItem *> &table_items = dml_stmt.get_table_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
      const TableItem *table_item = table_items.at(i);
      if (OB_ISNULL(table_item) || !table_item->is_basic_table()) {
        continue;
      }
      uint64_t table_id = table_item->get_base_table_item().ref_id_;
      if (OB_INVALID_ID == table_id) {
        table_id = table_item->table_id_;
      }
      if (OB_INVALID_ID == table_id) {
        continue;
      }
      ObSqlString meta_sql;
      if (OB_FAIL(meta_sql.append_fmt("select column_id, database_name, table_name, column_name "
                                      "from oceanbase.__all_ai_column where table_id = %lu", table_id))) {
      } else {
        SMART_VAR(ObMySQLProxy::MySQLResult, meta_result) {
          if (OB_ISNULL(GCTX.sql_proxy_)) {
            ret = OB_NOT_INIT;
          } else if (OB_FAIL(GCTX.sql_proxy_->read(meta_result, meta_sql.ptr()))) {
          } else if (OB_ISNULL(meta_result.get_result())) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            sqlclient::ObMySQLResult *meta_mysql_result = meta_result.get_result();
            int next_ret = OB_SUCCESS;
            while (OB_SUCC(ret)) {
              next_ret = meta_mysql_result->next();
              if (OB_ITER_END == next_ret) {
                break;
              } else if (OB_FAIL(next_ret)) {
                ret = next_ret;
                break;
              } else {
                int64_t column_id_int = 0;
                ObString db_name;
                ObString table_name;
                ObString column_name;
                bool unfinished = false;
                if (OB_FAIL(meta_mysql_result->get_int(static_cast<int64_t>(0), column_id_int))) {
                } else if (!ai_strict_column_referenced(referenced_ids,
                                                        static_cast<uint64_t>(column_id_int))) {
                  // this AI column is not referenced by the statement
                } else if (OB_FAIL(meta_mysql_result->get_varchar(static_cast<int64_t>(1), db_name))) {
                } else if (OB_FAIL(meta_mysql_result->get_varchar(static_cast<int64_t>(2), table_name))) {
                } else if (OB_FAIL(meta_mysql_result->get_varchar(static_cast<int64_t>(3), column_name))) {
                } else if (OB_FAIL(ai_strict_has_unfinished_row(db_name, table_name, column_name,
                                                                unfinished))) {
                } else if (unfinished) {
                  LOG_WARN("ai column read is rejected by strict read mode", K(table_id),
                           K(column_id_int));
                  ret = OB_ERR_AI_COLUMN_UNFINISHED;
                  LOG_USER_ERROR(OB_ERR_AI_COLUMN_UNFINISHED, table_id,
                                 static_cast<uint64_t>(column_id_int));
                }
              }
            }
          }
        }
      }
    }
  }
  const ObIArray<ObQueryRefRawExpr *> &subquery_exprs = dml_stmt.get_subquery_exprs();
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs.count(); ++i) {
    ObDMLStmt *ref_stmt = NULL;
    if (OB_ISNULL(ref_stmt = subquery_exprs.at(i)->get_ref_stmt())) {
    } else if (ai_strict_visited(visited, ref_stmt)) {
    } else if (OB_FAIL(visited.push_back(ref_stmt))) {
    } else if (OB_FAIL(ai_strict_check_stmt(*ref_stmt, visited))) {
    }
  }
  return ret;
}

static int check_ai_column_strict_read(ObResolverParams &params, ObStmt &stmt)
{
  int ret = OB_SUCCESS;
  bool strict_read = false;
  stmt::StmtType stmt_type = stmt.get_stmt_type();
  if (stmt::T_SELECT != stmt_type && stmt::T_UPDATE != stmt_type && stmt::T_DELETE != stmt_type) {
  } else if (OB_ISNULL(params.session_info_)) {
  } else if (OB_FAIL(params.session_info_->get_sys_variable(
                 share::SYS_VAR_AI_COLUMN_STRICT_READ, strict_read))) {
    LOG_WARN("failed to get ai column strict read variable", K(ret));
    ret = OB_SUCCESS;
  } else if (!strict_read) {
  } else {
    ObDMLStmt &dml_stmt = static_cast<ObDMLStmt &>(stmt);
    ObSEArray<const ObDMLStmt *, 16> visited;
    if (OB_FAIL(visited.push_back(&dml_stmt))) {
    } else if (OB_FAIL(ai_strict_check_stmt(dml_stmt, visited))) {
    }
  }
  return ret;
}

}  // namespace

int ObResolver::resolve(IsPrepared if_prepared, const ParseNode &parse_tree, ObStmt *&stmt)
{
#define REGISTER_STMT_RESOLVER(name)                                   \
  do {                                                                 \
    ret = stmt_resolver_func<Ob##name##Resolver>(params_, *real_parse_tree, stmt); \
  } while (0)

#define REGISTER_SELECT_STMT_RESOLVER(name)                                   \
  do {                                                                        \
    ret = select_stmt_resolver_func<Ob##name##Resolver>(params_, parse_tree, stmt); \
  } while (0)
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_resolve);
  int ret = OB_SUCCESS;
  const ParseNode *real_parse_tree = NULL;
  int64_t questionmark_count = 0;
  UNUSED(if_prepared);
  if (OB_ISNULL(params_.allocator_)
      || OB_ISNULL(params_.schema_checker_)
      || OB_ISNULL(params_.session_info_)
      || OB_ISNULL(params_.expr_factory_)
      || OB_ISNULL(params_.query_ctx_)) {
    ret = OB_NOT_INIT;
  } else {
    real_parse_tree = &parse_tree;
  }

  if (OB_SUCC(ret)) {
    params_.resolver_scope_stmt_type_ = parse_tree.type_;
    params_.session_info_->set_stmt_type(
      ObResolverUtils::get_stmt_type_by_item_type(parse_tree.type_));
    ObExecContext *exec_ctx = params_.session_info_->get_cur_exec_ctx();
    if (exec_ctx != nullptr && exec_ctx->get_sql_ctx() != nullptr) {
      //save the current stmt type on my context,
      //some other modules may need to get the current SQL stmt type through its context
      ObSqlCtx *sql_ctx = exec_ctx->get_sql_ctx();
      sql_ctx->stmt_type_ = ObResolverUtils::get_stmt_type_by_item_type(parse_tree.type_);
    }

    switch (real_parse_tree->type_) {
      case T_CREATE_TABLE: {
        REGISTER_STMT_RESOLVER(CreateTable);
        break;
      }
      case T_ALTER_TABLE: {
        REGISTER_STMT_RESOLVER(AlterTable);
        break;
      }
      case T_CREATE_INDEX: {
        REGISTER_STMT_RESOLVER(CreateIndex);
        break;
      }
      case T_CREATE_VIEW: {
        REGISTER_STMT_RESOLVER(CreateView);
        break;
      }
      case T_ALTER_VIEW: {
#if 0
        ObAlterViewResolver stmt_resolver(params_);
        if (OB_FAIL(stmt_resolver.resolve(*node))) {
        }

        stmt = stmt_resolver.get_basic_stmt();
#endif
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support to alter view");
	      LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter view");
        break;
      }
      case T_DROP_VIEW:
      case T_DROP_TABLE: {
        REGISTER_STMT_RESOLVER(DropTable);
        break;
      }
      case T_DROP_INDEX: {
        REGISTER_STMT_RESOLVER(DropIndex);
        break;
      }
      case T_CREATE_TABLE_LIKE: {
        REGISTER_STMT_RESOLVER(CreateTableLike);
        break;
      }
      case T_FORK_TABLE: {
        REGISTER_STMT_RESOLVER(ForkTable);
        break;
      }
      case T_FORK_DATABASE: {
        REGISTER_STMT_RESOLVER(ForkDatabase);
        break;
      }
      case T_DIFF_TABLE: {
        REGISTER_STMT_RESOLVER(DiffTable);
        break;
      }
      case T_MERGE_TABLE: {
        REGISTER_STMT_RESOLVER(MergeTable);
        break;
      }
      case T_SELECT: {
        REGISTER_SELECT_STMT_RESOLVER(Select);
        break;
      }
      case T_INSERT: {
        REGISTER_STMT_RESOLVER(Insert);
        break;
      }
      case T_UPDATE: {
        REGISTER_STMT_RESOLVER(Update);
        break;
      }
      case T_DELETE: {
        REGISTER_STMT_RESOLVER(Delete);
        break;
      }
      case T_BEGIN: {
        REGISTER_STMT_RESOLVER(StartTrans);
        break;
      }
      case T_ROLLBACK:
        //fall through
      case T_COMMIT: {
        REGISTER_STMT_RESOLVER(EndTrans);
        break;
      }
      case T_FREEZE: {
        REGISTER_STMT_RESOLVER(Freeze);
        break;
      }
      case T_FLUSH_CACHE: {
        REGISTER_STMT_RESOLVER(FlushCache);
        break;
      }
      case T_FLUSH_KVCACHE: {
        REGISTER_STMT_RESOLVER(FlushKVCache);
        break;
      }
      case T_FLUSH_ILOGCACHE: {
        REGISTER_STMT_RESOLVER(FlushIlogCache);
        break;
      }
      case T_FLUSH_DAG_WARNINGS: {
        REGISTER_STMT_RESOLVER(FlushDagWarnings);
        break;
      }
      case T_MERGE_CONTROL: {
        REGISTER_STMT_RESOLVER(AdminMerge);
        break;
      }
      case T_CANCEL_TASK: {
        REGISTER_STMT_RESOLVER(CancelTask);
        break;
      }
      case T_REFRESH_MEMORY_STAT: {
        REGISTER_STMT_RESOLVER(RefreshMemStat);
        break;
      }
      case T_REFRESH_IO_CALIBRATION: {
        REGISTER_STMT_RESOLVER(RefreshIOCalibration);
        break;
      }
      case T_SWITCHOVER_TO_STANDBY:
      case T_SWITCHOVER_TO_PRIMARY:
      case T_ACTIVATE_STANDBY: {
        REGISTER_STMT_RESOLVER(SwitchRole);
        break;
      }
      case T_ALTER_SYSTEM_SET_PARAMETER: {
        REGISTER_STMT_RESOLVER(SetConfig);
        break;
      }
      case T_ALTER_SYSTEM_SETTP: {
        REGISTER_STMT_RESOLVER(SetTP);
        break;
      }
      case T_CLEAR_MERGE_ERROR: {
        REGISTER_STMT_RESOLVER(ClearMergeError);
        break;
      }
      case T_CREATE_DATABASE: {
        REGISTER_STMT_RESOLVER(CreateDatabase);
        break;
      }
      case T_USE_DATABASE: {
        REGISTER_STMT_RESOLVER(UseDatabase);
        break;
      }
      case T_ALTER_DATABASE: {
        REGISTER_STMT_RESOLVER(AlterDatabase);
        break;
      }
      case T_DROP_DATABASE: {
        REGISTER_STMT_RESOLVER(DropDatabase);
        break;
      }
      case T_RENAME_TABLE: {
        REGISTER_STMT_RESOLVER(RenameTable);
        break;
      }
      case T_TRUNCATE_TABLE: {
        REGISTER_STMT_RESOLVER(TruncateTable);
        break;
      }
      case T_RECYCLEBIN_RESTORE_TABLE: {
        REGISTER_STMT_RESOLVER(RecyclebinRestoreTable);
        break;
      }
      case T_RECYCLEBIN_RESTORE_DATABASE: {
        REGISTER_STMT_RESOLVER(RecyclebinRestoreDatabase);
        break;
      }
      case T_PURGE_TABLE: {
        REGISTER_STMT_RESOLVER(PurgeTable);
        break;
      }
      case T_PURGE_INDEX: {
        REGISTER_STMT_RESOLVER(PurgeIndex);
        break;
      }
      case T_PURGE_DATABASE: {
        REGISTER_STMT_RESOLVER(PurgeDatabase);
        break;
      }
      case T_PURGE_RECYCLEBIN: {
        REGISTER_STMT_RESOLVER(PurgeRecycleBin);
        break;
      }
      case T_OPTIMIZE_TABLE: {
        REGISTER_STMT_RESOLVER(OptimizeTable);
        break;
      }
      case T_PREPARE: {
        if (params_.is_prepare_protocol_) {
          ret = OB_ERR_PARSE_SQL;
        } else {
          REGISTER_STMT_RESOLVER(Prepare);
        }
        break;
      }
      case T_EXECUTE: {
        if (params_.is_prepare_protocol_) {
          ret = OB_ERR_PARSE_SQL;
        } else {
          REGISTER_STMT_RESOLVER(Execute);
        }
        break;
      }
      case T_DEALLOCATE: {
        if (params_.is_prepare_protocol_) {
          ret = OB_ERR_PARSE_SQL;
        } else {
          REGISTER_STMT_RESOLVER(Deallocate);
        }
        break;
      }
      case T_VARIABLE_SET: {
        REGISTER_STMT_RESOLVER(VariableSet);
        break;
      }
      case T_TRANSACTION: {
        REGISTER_STMT_RESOLVER(SetTransaction);
        break;
      }
      case T_SHOW_TABLES:
      case T_SHOW_DATABASES:
      case T_SHOW_VARIABLES:
      case T_SHOW_COLUMNS:
      case T_SHOW_SCHEMA:
      case T_SHOW_CREATE_DATABASE:
      case T_SHOW_CREATE_TABLE:
      case T_SHOW_CREATE_VIEW:
      case T_SHOW_CREATE_PROCEDURE:
      case T_SHOW_CREATE_FUNCTION:
      case T_SHOW_TABLE_STATUS:
      case T_SHOW_PARAMETERS:
      case T_SHOW_INDEXES:
      case T_SHOW_PROCESSLIST:
      case T_SHOW_WARNINGS:
      case T_SHOW_ERRORS:
      case T_SHOW_TRACE:
      case T_SHOW_ENGINES:
      case T_SHOW_PROFILE:
      case T_SHOW_ENGINE:
      case T_SHOW_OPEN_TABLES:
      case T_SHOW_PRIVILEGES:
      case T_SHOW_CHARSET:
      case T_SHOW_COLLATION:
      case T_SHOW_GRANTS:
      case T_SHOW_RECYCLEBIN:
      case T_SHOW_PROCEDURE_STATUS:
      case T_SHOW_FUNCTION_STATUS:
      case T_SHOW_TRIGGERS:
      case T_SHOW_STATUS:
      case T_SHOW_CREATE_TRIGGER:
      case T_SHOW_CHECK_TABLE:
      case T_SHOW_CREATE_USER: {
        REGISTER_STMT_RESOLVER(Show);
        break;
      }
      case T_CREATE_USER: {
        REGISTER_STMT_RESOLVER(CreateUser);
        break;
      }
      case T_DROP_USER: {
        REGISTER_STMT_RESOLVER(DropUser);
        break;
      }
      case T_RENAME_USER: {
        REGISTER_STMT_RESOLVER(RenameUser);
        break;
      }
      case T_SET_PASSWORD: {
        REGISTER_STMT_RESOLVER(SetPassword);
        break;
      }
      case T_LOCK_USER: {
        REGISTER_STMT_RESOLVER(LockUser);
        break;
      }
      case T_ALTER_USER_DEFAULT_ROLE:
      case T_SET_ROLE: {
        REGISTER_STMT_RESOLVER(AlterUserRole);
        break;
      }

      case T_GRANT_ROLE:
      case T_SYSTEM_GRANT:
      case T_GRANT: {
        REGISTER_STMT_RESOLVER(Grant);
        break;
      }
      case T_SYSTEM_REVOKE:
      case T_REVOKE:
      case T_REVOKE_ALL: {
        REGISTER_STMT_RESOLVER(Revoke);
        break;
      }
      case T_EXPLAIN: {
        REGISTER_STMT_RESOLVER(Explain);
        break;
      }
      case T_ALTER_SYSTEM_SET: {
        REGISTER_STMT_RESOLVER(AlterSystemSet);
        break;
      }
      case T_ALTER_SYSTEM_KILL: {
        REGISTER_STMT_RESOLVER(Kill);
        break;
      }
      case T_ALTER_SESSION_SET: {
        REGISTER_STMT_RESOLVER(AlterSessionSet);
        break;
      }
      case T_SET_NAMES:
        // fall through
      case T_SET_CHARSET: {
        REGISTER_STMT_RESOLVER(SetNames);
        break;
      }
      case T_KILL: {
        REGISTER_STMT_RESOLVER(Kill);
        break;
      }
      case T_EMPTY_QUERY: {
        REGISTER_STMT_RESOLVER(EmptyQuery);
        break;
      }
      case T_FLUSH_PRIVILEGES: {
        REGISTER_STMT_RESOLVER(EmptyQuery);
        break;
      }
      case T_LOCK_TABLE: {
        REGISTER_STMT_RESOLVER(LockTable);
        break;
      }
      case T_CREATE_OUTLINE: {
        REGISTER_STMT_RESOLVER(CreateOutline);
        break;
      }
      case T_ALTER_OUTLINE: {
        REGISTER_STMT_RESOLVER(AlterOutline);
        break;
      }
      case T_DROP_OUTLINE: {
        REGISTER_STMT_RESOLVER(DropOutline);
        break;
      }
      case T_SP_CREATE: {
        REGISTER_STMT_RESOLVER(CreateProcedure);
        break;
      }
      case T_SP_ALTER: {
        REGISTER_STMT_RESOLVER(AlterProcedure);
        break;
      }
      case T_SP_CALL_STMT: {
        REGISTER_STMT_RESOLVER(CallProcedure);
        break;
      }
      case T_SF_CREATE: {
        REGISTER_STMT_RESOLVER(CreateFunction);
        break;
      }
      case T_SF_ALTER: {
        REGISTER_STMT_RESOLVER(AlterFunction);
        break;
      }
      case T_SP_DROP: {
        REGISTER_STMT_RESOLVER(DropProcedure);
        break;
      }
      case T_SF_DROP: {
        REGISTER_STMT_RESOLVER(DropFunction);
        break;
      }
      case T_SP_ANONYMOUS_BLOCK: {
        REGISTER_STMT_RESOLVER(AnonymousBlock);
        break;
      }
      case T_PACKAGE_CREATE: {
        REGISTER_STMT_RESOLVER(CreatePackage);
        break;
      }
      case T_PACKAGE_CREATE_BODY: {
        REGISTER_STMT_RESOLVER(CreatePackageBody);
        break;
      }
      case T_PACKAGE_DROP: {
        REGISTER_STMT_RESOLVER(DropPackage);
        break;
      }
      case T_MYSQL_UPDATE_HISTOGRAM:
      case T_MYSQL_DROP_HISTOGRAM:
      case T_MYSQL_ANALYZE: {
        REGISTER_STMT_RESOLVER(AnalyzeStmt);
        break;
      }
      case T_LOAD_DATA_URL:
      case T_LOAD_DATA: {
        REGISTER_STMT_RESOLVER(LoadData);
        break;
      }
      case T_SET_TABLE_COMMENT:
      case T_SET_COLUMN_COMMENT: {
        REGISTER_STMT_RESOLVER(SetComment);
        break;
      }
      case T_CREATE_ROLE: {
        REGISTER_STMT_RESOLVER(CreateRole);
        break;
      }
      case T_DROP_ROLE: {
        REGISTER_STMT_RESOLVER(DropRole);
        break;
      }
      case T_ALTER_ROLE: {
        REGISTER_STMT_RESOLVER(AlterRole);
        break;
      }
      case T_CREATE_SAVEPOINT:
      case T_ROLLBACK_SAVEPOINT:
      case T_RELEASE_SAVEPOINT: {
        REGISTER_STMT_RESOLVER(SavePoint);
        break;
      }
      case T_TG_CREATE: {
        REGISTER_STMT_RESOLVER(Trigger);
        break;
      }
      case T_TG_DROP: {
        REGISTER_STMT_RESOLVER(Trigger);
        break;
      }
      case T_TG_ALTER: {
        REGISTER_STMT_RESOLVER(Trigger);
        break;
      }
      case T_DIAGNOSTICS: {
        REGISTER_STMT_RESOLVER(GetDiagnostics);
        break;
      }
      case T_ALTER_SYSTEM_RESET_PARAMETER: {
        REGISTER_STMT_RESOLVER(ResetConfig);
        break;
      }
      case T_ALTER_SYSTEM_RESET: {
        REGISTER_STMT_RESOLVER(AlterSystemReset);
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        const char *type_name = get_type_name(parse_tree.type_);
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "statement type");
        break;
      }
    }  // end switch
    if (OB_SUCC(ret) && stmt->is_dml_write_stmt()) {
      // todo yanli:check leader-follower database
    }
    
    if (OB_SUCC(ret) && stmt->is_dml_stmt() && !params_.session_info_->is_varparams_sql_prepare()) {
      ObDMLStmt *dml_stmt = static_cast<ObDMLStmt*>(stmt);
      ObRawExprWrapEnumSet enum_set_wrapper(*params_.expr_factory_, params_.session_info_);
      if (OB_FAIL(enum_set_wrapper.wrap_enum_set(*dml_stmt))) {
      }
    }

    if (OB_SUCC(ret) && stmt->is_dml_stmt() && !stmt->is_explain_stmt()) {
      if (OB_FAIL(check_ai_column_strict_read(params_, *stmt))) {
      }
    }

    if (OB_SUCC(ret) && stmt->is_dml_stmt() && !stmt->is_explain_stmt()) {
      if (OB_FAIL(params_.query_ctx_->query_hint_.init_query_hint(params_.allocator_,
                                                                  params_.session_info_,
                                                                  static_cast<ObDMLStmt*>(stmt)))) {
      } else if (OB_FAIL(params_.query_ctx_->query_hint_.check_and_set_params_from_hint(params_,
                                                         *static_cast<ObDMLStmt*>(stmt)))) {
      }
    }

    if (OB_SUCC(ret) && stmt->is_dml_stmt() && !stmt->is_explain_stmt()) {
      bool is_contain_inner_table = false;
      bool is_contain_select_for_update = false;
      ObDMLStmt *dml_stmt = static_cast<ObDMLStmt*>(stmt);
      if (OB_FAIL(dml_stmt->check_if_contain_inner_table(is_contain_inner_table))) {
      } else if (OB_FAIL(dml_stmt->check_if_contain_select_for_update(
                  is_contain_select_for_update))) {
      } else {
        params_.query_ctx_->is_contain_inner_table_ = is_contain_inner_table;
        params_.query_ctx_->is_contain_select_for_update_ = is_contain_select_for_update;
        params_.query_ctx_->has_dml_write_stmt_ = dml_stmt->is_dml_write_stmt();
      }

    }
    if (OB_SUCC(ret)) {
      stmt::StmtType stmt_type = stmt->get_stmt_type();
      if (ObStmt::is_ddl_stmt(stmt_type, stmt->has_global_variable()) || ObStmt::is_dcl_stmt(stmt_type)) {
        ObDDLStmt *ddl_stmt = static_cast<ObDDLStmt*>(stmt);
        obcall::ObDDLArg &ddl_arg = ddl_stmt->get_ddl_arg();

        if (OB_ISNULL(params_.query_ctx_)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          ddl_arg.ddl_stmt_str_ = params_.query_ctx_->get_sql_stmt();
        }
      }
    }
  }  // end if
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
