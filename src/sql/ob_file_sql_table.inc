/*
 * File SQL: inline derived-table builder.
 *
 * Every file table function is expanded into a derived table
 *   (select <row1> union all select <row2> ...)
 * so the native SQL engine performs all filtering / aggregation / JOIN work
 * without creating any temporary tables (and without touching the DDL path).
 */
#ifndef OCEANBASE_SQL_OB_FILE_SQL_TABLE_INC_
#define OCEANBASE_SQL_OB_FILE_SQL_TABLE_INC_

namespace oceanbase
{
namespace sql
{
namespace file_sql
{

static int file_sql_append_sql_string(ObSqlString &sql, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("'"))) {
  } else {
    const char *p = value.ptr();
    for (int64_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
      const unsigned char c = static_cast<unsigned char>(p[i]);
      switch (c) {
        case '\'': ret = sql.append("\\'"); break;
        case '\\': ret = sql.append("\\\\"); break;
        case '\n': ret = sql.append("\\n"); break;
        case '\r': ret = sql.append("\\r"); break;
        case '\t': ret = sql.append("\\t"); break;
        case '\0': ret = sql.append("\\0"); break;
        case '\032': ret = sql.append("\\Z"); break;
        default:
          if (c < 0x20) {
            ret = sql.append_fmt("\\%c", static_cast<char>(c + 64));
          } else {
            ret = sql.append_fmt("%c", static_cast<char>(c));
          }
          break;
      }
    }
    if (OB_SUCC(ret)) {
      ret = sql.append("'");
    }
  }
  return ret;
}

static int file_sql_append_sql_ident(ObSqlString &sql, const ObString &name)
{
  return sql.append_fmt("`%.*s`", name.length(), name.ptr());
}

class FileSqlTableBuilder
{
public:
  FileSqlTableBuilder()
    : schema_(), sql_(), rows_(0), inited_(false), created_(false) {}

  void set_schema(const FileSchema &schema)
  {
    schema_ = schema;
    inited_ = true;
  }
  const FileSchema &get_schema() const { return schema_; }
  // The "table name" is actually the SQL text of the derived table.
  const char *table_name() const { return sql_.ptr(); }
  const char *sql_text() const { return sql_.ptr(); }

  int create_table()
  {
    int ret = OB_SUCCESS;
    if (!inited_) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(sql_.append("(select "))) {
    } else {
      created_ = true;
    }
    return ret;
  }

  int add_row(const ObIArray<ObString> &values, const ObIArray<bool> &is_nulls)
  {
    int ret = OB_SUCCESS;
    if (!created_) {
      ret = OB_NOT_INIT;
    } else if (values.count() != schema_.columns_.count()
               || is_nulls.count() != values.count()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      if (rows_ > 0 && OB_FAIL(sql_.append(" union all select "))) {
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < schema_.columns_.count(); ++i) {
        const FileColumn &col = schema_.columns_.at(i);
        const ObString &value = values.at(i);
        if (i > 0 && OB_FAIL(sql_.append(", "))) {
          break;
        }
        if (is_nulls.at(i) || FILE_COL_NULL == col.type_) {
          if (OB_FAIL(sql_.append("null"))) {
          }
        } else {
          switch (col.type_) {
            case FILE_COL_BIGINT:
            case FILE_COL_BOOLEAN:
              if (OB_FAIL(sql_.append_fmt("cast(%.*s as signed)",
                                          value.length(), value.ptr()))) {
              }
              break;
            case FILE_COL_DOUBLE:
              if (OB_FAIL(sql_.append_fmt("cast(%.*s as double)",
                                          value.length(), value.ptr()))) {
              }
              break;
            case FILE_COL_DATE:
              if (OB_FAIL(sql_.append("cast("))) {
              } else if (OB_FAIL(file_sql_append_sql_string(sql_, value))) {
              } else if (OB_FAIL(sql_.append(" as date)"))) {
              }
              break;
            case FILE_COL_DATETIME:
              if (OB_FAIL(sql_.append("cast("))) {
              } else if (OB_FAIL(file_sql_append_sql_string(sql_, value))) {
              } else if (OB_FAIL(sql_.append(" as datetime)"))) {
              }
              break;
            case FILE_COL_VARCHAR:
            default:
              if (OB_FAIL(file_sql_append_sql_string(sql_, value))) {
              }
              break;
          }
        }
        if (OB_SUCC(ret) && 0 == rows_) {
          if (OB_FAIL(sql_.append(" as "))) {
          } else if (OB_FAIL(file_sql_append_sql_ident(sql_, col.name_))) {
          }
        }
      }
      if (OB_SUCC(ret)) {
        ++rows_;
        // protect against extremely large inline statements: 8MB of SQL text
        if (sql_.length() > 8 * 1024 * 1024) {
          ret = file_sql_raise_not_supported("file too large for an inline file scan");
        }
      }
    }
    return ret;
  }

  int finish()
  {
    int ret = OB_SUCCESS;
    if (!created_) {
      ret = OB_NOT_INIT;
    } else if (rows_ <= 0) {
      if (schema_.columns_.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(sql_.append("null as "))) {
      } else if (OB_FAIL(file_sql_append_sql_ident(sql_, schema_.columns_.at(0).name_))) {
      }
      for (int64_t i = 1; OB_SUCC(ret) && i < schema_.columns_.count(); ++i) {
        if (OB_FAIL(sql_.append(", null as "))) {
        } else if (OB_FAIL(file_sql_append_sql_ident(sql_, schema_.columns_.at(i).name_))) {
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(sql_.append(" where 1 = 0"))) {
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(sql_.append(")"))) {
    }
    return ret;
  }

private:
  FileSchema schema_;
  ObSqlString sql_;
  int64_t rows_;
  bool inited_;
  bool created_;
};

} // namespace file_sql
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_FILE_SQL_TABLE_INC_
