/*
 * Copyright (c) 2025 OceanBase.
 *
 * File SQL: query local files (CSV / JSONL / Parquet) as database tables.
 *
 * The statement text is inspected before parsing.  Every call of a file table
 * function (read_csv/read_json/read_ndjson/read_parquet/glob) is materialized
 * into a temporary table (batched inserts) and the call is replaced by the
 * temporary table name.  The rewritten statement is then handled by the normal
 * SQL engine, so filters/aggregations/joins use the native seekdb planner and
 * executor.  The session schema guard is refreshed before the rewritten
 * statement is resolved.
 */
#ifndef OCEANBASE_SQL_OB_FILE_SQL_IMPL_INC_
#define OCEANBASE_SQL_OB_FILE_SQL_IMPL_INC_

#include "share/ob_server_struct.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_basic_session_info.h"
#include "sql/ob_spi.h"
#include "oblib/common/mysqlclient/ob_mysql_proxy.h"
#include "oblib/common/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include <mutex>
#include <atomic>
#include <unordered_map>
#include <deque>
#include <condition_variable>
#include <chrono>
#include <glob.h>
#include <sys/stat.h>
#include <unistd.h>

namespace oceanbase
{
namespace sql
{

static std::mutex &file_sql_mutex()
{
  static std::mutex m;
  return m;
}

static std::unordered_map<uint32_t, bool> &file_sql_enabled_map()
{
  static std::unordered_map<uint32_t, bool> m;
  return m;
}

static bool file_sql_session_enabled(const uint32_t sessid)
{
  bool enabled = false;
  std::lock_guard<std::mutex> guard(file_sql_mutex());
  auto it = file_sql_enabled_map().find(sessid);
  if (it != file_sql_enabled_map().end()) {
    enabled = it->second;
  }
  return enabled;
}

static void file_sql_set_session_enabled(const uint32_t sessid, const bool enabled)
{
  std::lock_guard<std::mutex> guard(file_sql_mutex());
  file_sql_enabled_map()[sessid] = enabled;
}

static int file_sql_proxy_write(const char *sql, int64_t &affected)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(sql)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(GCTX.sql_proxy_->write(sql, affected))) {
    LOG_WARN("file sql proxy write failed", K(ret), "sql", sql);
  }
  return ret;
}

static int file_sql_raise_not_supported(const char *what)
{
  LOG_USER_ERROR(OB_NOT_SUPPORTED, what);
  return OB_NOT_SUPPORTED;
}

static int file_sql_raise_parse_error(const ObString &stmt)
{
  // The statement is rewritten into an intentionally invalid statement later; the
  // normal parser reports the 1064 error with the standard error packet.
  int ret = OB_SUCCESS;
  LOG_WARN("file sql syntax error", "len", stmt.length(), K(ret));
  return OB_ERR_PARSE_SQL;
}

static int file_sql_raise_no_privilege()
{
  LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "FILE");
  return OB_ERR_NO_PRIVILEGE;
}

static int file_sql_raise_file_error(const char *path)
{
  LOG_INFO("file sql can not open file", "path", path);
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "file can not be opened or does not exist");
  return OB_NOT_SUPPORTED;
}

static int file_sql_check_secure_path(ObSQLSessionInfo &session, const ObString &path)
{
  int ret = OB_SUCCESS;
  ObString secure_priv;
  if (OB_FAIL(session.get_secure_file_priv(secure_priv))) {
    LOG_WARN("failed to get secure_file_priv", K(ret));
  } else if (secure_priv.empty() || 0 == secure_priv.case_compare("NULL")) {
    ret = file_sql_raise_no_privilege();
  } else {
    char priv_buf[PATH_MAX];
    char path_buf[PATH_MAX];
    priv_buf[0] = '\0';
    path_buf[0] = '\0';
    ObArenaAllocator alloc;
    ObString priv_cstr;
    ObString path_cstr;
    char *priv_real = NULL;
    char *path_real = NULL;
    if (OB_FAIL(ob_write_string(alloc, secure_priv, priv_cstr, true))) {
    } else if (OB_FAIL(ob_write_string(alloc, path, path_cstr, true))) {
    } else if (OB_ISNULL(priv_real = realpath(priv_cstr.ptr(), priv_buf))) {
      ret = file_sql_raise_no_privilege();
    } else if (OB_ISNULL(path_real = realpath(path_cstr.ptr(), path_buf))) {
      ret = file_sql_raise_file_error(path_cstr.ptr());
    } else {
      const int64_t priv_len = static_cast<int64_t>(strlen(priv_real));
      if (0 != strncmp(path_real, priv_real, priv_len)
          || (path_real[priv_len] != '\0' && path_real[priv_len] != '/')) {
        ret = file_sql_raise_no_privilege();
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

#include "sql/ob_file_sql_util.inc"
#include "sql/ob_file_sql_table.inc"
#include "sql/ob_file_sql_csv.inc"
#include "sql/ob_file_sql_json.inc"
#include "sql/ob_file_sql_parquet.inc"

namespace oceanbase
{
namespace sql
{
namespace file_sql
{

//////////////////////////////////////////////////////////////////////////////
// small text helpers
static bool file_sql_has_prefix_ci(const char *text, const int64_t len, const char *prefix)
{
  const int64_t plen = static_cast<int64_t>(strlen(prefix));
  return len >= plen && 0 == strncasecmp(text, prefix, plen);
}

static bool file_sql_contains_ci(const char *text, const int64_t len, const char *needle)
{
  const int64_t nlen = static_cast<int64_t>(strlen(needle));
  bool found = false;
  for (int64_t i = 0; !found && i + nlen <= len; ++i) {
    found = (0 == strncasecmp(text + i, needle, nlen));
  }
  return found;
}

struct FileFunctionArgs
{
  ObString path_;
  FileCsvOptions csv_;
  ObString format_;
  bool has_format_;
  FileFunctionArgs() : path_(), csv_(), format_(), has_format_(false) {}
};

struct FileArgValue
{
  enum Kind { STRING, INTEGER, IDENT } kind_;
  ObString str_;
  int64_t int_;
  FileArgValue() : kind_(STRING), str_(), int_(0) {}
};

static int file_sql_parse_arg_value(ObIAllocator &alloc,
                                    const char *s, const int64_t len, int64_t &pos,
                                    FileArgValue &value)
{
  int ret = OB_SUCCESS;
  file_sql_skip_space(s, len, pos);
  if (pos >= len) {
    ret = OB_ERR_PARSE_SQL;
  } else if ('\'' == s[pos] || '"' == s[pos]) {
    value.kind_ = FileArgValue::STRING;
    if (OB_FAIL(file_sql_parse_string_literal(s, len, pos, value.str_, alloc))) {
    }
  } else if ('-' == s[pos] || file_sql_is_digit(s[pos])) {
    const int64_t begin = pos;
    if ('-' == s[pos]) {
      ++pos;
    }
    while (pos < len && file_sql_is_digit(s[pos])) {
      ++pos;
    }
    value.kind_ = FileArgValue::INTEGER;
    char num_buf[32];
    const int64_t num_len = pos - begin;
    if (num_len >= static_cast<int64_t>(sizeof(num_buf))) {
      ret = OB_ERR_PARSE_SQL;
    } else {
      MEMCPY(num_buf, s + begin, num_len);
      num_buf[num_len] = '\0';
      value.int_ = strtoll(num_buf, NULL, 10);
    }
  } else if (isalpha(static_cast<unsigned char>(s[pos])) || '_' == s[pos]) {
    const int64_t begin = pos;
    while (pos < len && (isalnum(static_cast<unsigned char>(s[pos])) || '_' == s[pos])) {
      ++pos;
    }
    value.kind_ = FileArgValue::IDENT;
    value.str_.assign_ptr(s + begin, static_cast<int32_t>(pos - begin));
  } else {
    ret = OB_ERR_PARSE_SQL;
  }
  return ret;
}

static bool file_sql_ident_is_true(const ObString &ident)
{
  return 0 == ident.case_compare("true") || 0 == ident.case_compare("on")
      || 0 == ident.case_compare("1") || 0 == ident.case_compare("yes");
}

static bool file_sql_ident_is_false(const ObString &ident)
{
  return 0 == ident.case_compare("false") || 0 == ident.case_compare("off")
      || 0 == ident.case_compare("0") || 0 == ident.case_compare("no");
}

static bool file_sql_is_true_value(const ObString &value)
{
  return 0 == value.case_compare("true") || 0 == value.case_compare("1");
}

// ---------------------------------------------------------------------------
// CSV materialization
class FileCsvLoadHandler
{
public:
  FileCsvLoadHandler(const FileSchema &schema, FileSqlTableBuilder &builder, bool has_nullstr,
                     const ObString &nullstr, bool skip_first)
    : schema_(schema), builder_(builder), has_nullstr_(has_nullstr), nullstr_(nullstr),
      skip_first_(skip_first), skipped_(false), norm_buf_() {}
  int on_row(FileRawRow &row, bool &stop)
  {
    int ret = OB_SUCCESS;
    stop = false;
    if (skip_first_ && !skipped_) {
      skipped_ = true;
      return ret;
    }
    if (row.fields_.count() != schema_.columns_.count()) {
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "malformed CSV record");
      ret = OB_NOT_SUPPORTED;
    }
    ObSEArray<ObString, 32> values;
    ObSEArray<bool, 32> nulls;
    for (int64_t i = 0; OB_SUCC(ret) && i < schema_.columns_.count(); ++i) {
      const FileColumn &col = schema_.columns_.at(i);
      const ObString &field = row.fields_.at(i);
      bool is_null = (field.length() <= 0);
      if (!is_null && has_nullstr_ && 0 == field.case_compare(nullstr_)) {
        is_null = true;
      }
      ObString value = field;
      if (!is_null) {
        if (FILE_COL_BOOLEAN == col.type_) {
          value.assign_ptr(file_sql_is_true_value(field) ? "1" : "0", 1);
        } else if (FILE_COL_DATETIME == col.type_) {
          const int32_t copy_len = field.length() < static_cast<int32_t>(sizeof(norm_buf_) - 1)
                                 ? field.length() : static_cast<int32_t>(sizeof(norm_buf_) - 1);
          MEMCPY(norm_buf_, field.ptr(), copy_len);
          norm_buf_[copy_len] = '\0';
          if (copy_len > 10 && ('T' == norm_buf_[10] || 't' == norm_buf_[10])) {
            norm_buf_[10] = ' ';
          }
          value.assign_ptr(norm_buf_, copy_len);
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(values.push_back(value))) {
        } else if (OB_FAIL(nulls.push_back(is_null))) {
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(builder_.add_row(values, nulls))) {
    }
    return ret;
  }

private:
  const FileSchema &schema_;
  FileSqlTableBuilder &builder_;
  bool has_nullstr_;
  ObString nullstr_;
  bool skip_first_;
  bool skipped_;
  char norm_buf_[64];
};

static int file_sql_materialize_csv(const ObString &path,
                                    const FileCsvOptions &opts,
                                    ObIAllocator &alloc,
                                    FileSqlTableBuilder &builder,
                                    FileSchema &schema)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t len = 0;
  if (OB_FAIL(file_sql_read_whole_file(path.ptr(), alloc, buf, len))) {
    if (OB_FILE_NOT_EXIST == ret) {
      ret = file_sql_raise_file_error(path.ptr());
    }
  } else {
    FileCsvSchemaInfer infer(schema, alloc);
    infer.set_options(&opts);
    FileCsvParser parser;
    parser.init(buf, len, opts);
    if (OB_FAIL(parser.parse(infer))) {
    } else if (schema.columns_.count() <= 0) {
      ret = file_sql_raise_not_supported("empty CSV file");
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < schema.columns_.count(); ++i) {
        FileColumn &col = schema.columns_.at(i);
        if (FILE_COL_NULL == col.type_) {
          col.nullable_ = true;
        }
        if (col.max_bytes_ <= 0) {
          col.max_bytes_ = 32;
        }
      }
      builder.set_schema(schema);
      if (OB_FAIL(builder.create_table())) {
      } else {
        FileCsvLoadHandler load(schema, builder, opts.has_nullstr_, opts.nullstr_, opts.header_);
        FileCsvParser parser2;
        parser2.init(buf, len, opts);
        if (OB_FAIL(parser2.parse(load))) {
        } else if (OB_FAIL(builder.finish())) {
        }
      }
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// JSONL materialization
class FileJsonLoadHandler
{
public:
  FileJsonLoadHandler(const FileSchema &schema, FileSqlTableBuilder &builder)
    : schema_(schema), builder_(builder) {}
  int on_object(ObIArray<FileJsonField> &fields)
  {
    int ret = OB_SUCCESS;
    ObSEArray<ObString, 32> values;
    ObSEArray<bool, 32> nulls;
    char norm_buf[64];
    for (int64_t i = 0; OB_SUCC(ret) && i < schema_.columns_.count(); ++i) {
      const FileColumn &col = schema_.columns_.at(i);
      const FileJsonField *found = NULL;
      for (int64_t k = 0; NULL == found && k < fields.count(); ++k) {
        if (0 == col.name_.case_compare(fields.at(k).key_)) {
          found = &fields.at(k);
        }
      }
      bool is_null = (NULL == found || FILE_JSON_NULL == found->kind_);
      ObString value;
      if (!is_null) {
        if (FILE_JSON_BOOL == found->kind_) {
          value.assign_ptr(found->bool_value_ ? "1" : "0", 1);
        } else if (FILE_JSON_NUMBER == found->kind_ || FILE_JSON_STRING == found->kind_) {
          value = found->value_;
        } else {
          is_null = true;
        }
      }
      if (!is_null && (FILE_COL_DATE == col.type_ || FILE_COL_DATETIME == col.type_)
          && value.length() < static_cast<int32_t>(sizeof(norm_buf))) {
        MEMCPY(norm_buf, value.ptr(), value.length());
        norm_buf[value.length()] = '\0';
        if (value.length() > 10 && ('T' == norm_buf[10] || 't' == norm_buf[10])) {
          norm_buf[10] = ' ';
        }
        value.assign_ptr(norm_buf, value.length());
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(values.push_back(value))) {
        } else if (OB_FAIL(nulls.push_back(is_null))) {
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(builder_.add_row(values, nulls))) {
    }
    return ret;
  }

private:
  const FileSchema &schema_;
  FileSqlTableBuilder &builder_;
};

static int file_sql_materialize_json(const ObString &path,
                                     ObIAllocator &alloc,
                                     FileSqlTableBuilder &builder,
                                     FileSchema &schema)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t len = 0;
  if (OB_FAIL(file_sql_read_whole_file(path.ptr(), alloc, buf, len))) {
    if (OB_FILE_NOT_EXIST == ret) {
      ret = file_sql_raise_file_error(path.ptr());
    }
  } else {
    FileJsonSchemaInfer infer(schema, alloc);
    int64_t pos = 0;
    bool eof = false;
    int64_t line_no = 0;
    while (OB_SUCC(ret) && !eof) {
      const char *line = NULL;
      int64_t line_len = 0;
      file_sql_next_line(buf, len, pos, line, line_len, eof);
      if (NULL == line || line_len <= 0) {
        continue;
      }
      ++line_no;
      ObArenaAllocator line_alloc;
      FileJsonParser parser;
      parser.init(line, line_len, line_alloc);
      ObSEArray<FileJsonField, 16> fields;
      if (OB_FAIL(parser.parse_object(fields))) {
        if (OB_NOT_SUPPORTED == ret) {
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "nested JSON objects or arrays in JSONL");
        } else {
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "malformed JSONL document");
        }
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("file sql jsonl parse failed", K(ret), K(line_no));
      } else if (!parser.at_line_end()) {
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "malformed JSONL document");
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("file sql jsonl trailing garbage", K(ret), K(line_no));
      } else if (OB_FAIL(infer.on_object(fields))) {
      }
    }
    if (OB_SUCC(ret) && schema.columns_.count() <= 0) {
      ret = file_sql_raise_not_supported("empty JSONL file");
    } else if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < schema.columns_.count(); ++i) {
        FileColumn &col = schema.columns_.at(i);
        if (FILE_COL_NULL == col.type_) {
          col.nullable_ = true;
        }
        if (col.max_bytes_ <= 0) {
          col.max_bytes_ = 32;
        }
      }
      builder.set_schema(schema);
      if (OB_FAIL(builder.create_table())) {
      } else {
        FileJsonLoadHandler load(schema, builder);
        pos = 0;
        eof = false;
        while (OB_SUCC(ret) && !eof) {
          const char *line = NULL;
          int64_t line_len = 0;
          file_sql_next_line(buf, len, pos, line, line_len, eof);
          if (NULL == line || line_len <= 0) {
            continue;
          }
          ObArenaAllocator line_alloc;
          FileJsonParser parser;
          parser.init(line, line_len, line_alloc);
          ObSEArray<FileJsonField, 16> fields;
          if (OB_FAIL(parser.parse_object(fields))) {
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "malformed JSONL document");
            ret = OB_NOT_SUPPORTED;
          } else if (OB_FAIL(load.on_object(fields))) {
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(builder.finish())) {
        }
      }
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// glob
static int file_sql_materialize_glob(const ObString &pattern,
                                     ObIAllocator &alloc,
                                     FileSqlTableBuilder &builder,
                                     FileSchema &schema)
{
  int ret = OB_SUCCESS;
  FileColumn col;
  ObString name;
  ObString pat;
  if (OB_FAIL(ob_write_string(alloc, ObString::make_string("file"), name))) {
  } else if (OB_FAIL(ob_write_string(alloc, pattern, pat, true))) {
  } else {
    col.name_ = name;
    col.type_ = FILE_COL_VARCHAR;
    col.nullable_ = false;
    col.max_bytes_ = 1024;
    if (OB_FAIL(schema.columns_.push_back(col))) {
    }
  }
  if (OB_SUCC(ret)) {
    builder.set_schema(schema);
    if (OB_FAIL(builder.create_table())) {
    } else {
      glob_t glob_result;
      MEMSET(&glob_result, 0, sizeof(glob_result));
      const int glob_ret = glob(pat.ptr(), GLOB_NOSORT, NULL, &glob_result);
      if (0 == glob_ret) {
        for (size_t i = 0; OB_SUCC(ret) && i < glob_result.gl_pathc; ++i) {
          ObSEArray<ObString, 1> values;
          ObSEArray<bool, 1> nulls;
          ObString value;
          if (OB_FAIL(ob_write_string(alloc, ObString(static_cast<int32_t>(strlen(glob_result.gl_pathv[i])),
                                                      glob_result.gl_pathv[i]), value))) {
          } else if (OB_FAIL(values.push_back(value))) {
          } else if (OB_FAIL(nulls.push_back(false))) {
          } else if (OB_FAIL(builder.add_row(values, nulls))) {
          }
        }
      } else if (GLOB_NOMATCH == glob_ret) {
        // empty result set
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
      globfree(&glob_result);
      if (OB_SUCC(ret) && OB_FAIL(builder.finish())) {
      }
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// argument parsing for read_csv / read_json / read_ndjson / read_parquet / glob
static int file_sql_parse_options(ObIAllocator &alloc,
                                  const char *s, const int64_t len,
                                  const bool is_csv, const bool is_json,
                                  FileFunctionArgs &args,
                                  const ObString &whole_stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  file_sql_skip_space(s, len, pos);
  if (pos >= len || ('\'' != s[pos] && '"' != s[pos])) {
    ret = file_sql_raise_parse_error(whole_stmt);
  } else if (OB_FAIL(file_sql_parse_string_literal(s, len, pos, args.path_, alloc))) {
    ret = file_sql_raise_parse_error(whole_stmt);
  } else if (OB_FAIL(ob_write_string(alloc, args.path_, args.path_, true))) {
  }
  while (OB_SUCC(ret)) {
    file_sql_skip_space(s, len, pos);
    if (pos >= len) {
      break;
    }
    if (',' != s[pos]) {
      ret = file_sql_raise_parse_error(whole_stmt);
      break;
    }
    ++pos;
    file_sql_skip_space(s, len, pos);
    const int64_t name_begin = pos;
    while (pos < len && (isalnum(static_cast<unsigned char>(s[pos])) || '_' == s[pos])) {
      ++pos;
    }
    if (pos == name_begin) {
      ret = file_sql_raise_parse_error(whole_stmt);
      break;
    }
    ObString opt_name;
    opt_name.assign_ptr(s + name_begin, static_cast<int32_t>(pos - name_begin));
    file_sql_skip_space(s, len, pos);
    if (pos >= len || '=' != s[pos]) {
      ret = file_sql_raise_parse_error(whole_stmt);
      break;
    }
    ++pos;
    FileArgValue value;
    if (OB_FAIL(file_sql_parse_arg_value(alloc, s, len, pos, value))) {
      ret = file_sql_raise_parse_error(whole_stmt);
      break;
    }
    if (0 == opt_name.case_compare("header")) {
      if (FileArgValue::IDENT == value.kind_ && file_sql_ident_is_true(value.str_)) {
        args.csv_.header_ = true;
      } else if (FileArgValue::IDENT == value.kind_ && file_sql_ident_is_false(value.str_)) {
        args.csv_.header_ = false;
      } else {
        ret = file_sql_raise_parse_error(whole_stmt);
      }
    } else if (0 == opt_name.case_compare("sample_size")) {
      if (FileArgValue::INTEGER != value.kind_ || -1 != value.int_) {
        ret = file_sql_raise_not_supported("sample_size other than -1");
      } else {
        args.csv_.sample_size_ = -1;
      }
    } else if (0 == opt_name.case_compare("format")) {
      if (FileArgValue::STRING != value.kind_) {
        ret = file_sql_raise_parse_error(whole_stmt);
      } else if (0 == value.str_.case_compare("newline_delimited")
                 || 0 == value.str_.case_compare("ndjson")
                 || 0 == value.str_.case_compare("jsonl")) {
        args.format_ = value.str_;
      } else {
        ret = file_sql_raise_not_supported("json format");
      }
    } else if (is_csv && (0 == opt_name.case_compare("delim")
                          || 0 == opt_name.case_compare("delimiter"))) {
      if (FileArgValue::STRING == value.kind_ && 1 == value.str_.length()) {
        args.csv_.delim_ = value.str_.ptr()[0];
      } else {
        ret = file_sql_raise_not_supported("delim option (single character expected)");
      }
    } else if (is_csv && 0 == opt_name.case_compare("quote")) {
      if (FileArgValue::STRING == value.kind_ && 1 == value.str_.length()) {
        args.csv_.quote_ = value.str_.ptr()[0];
      } else {
        ret = file_sql_raise_not_supported("quote option (single character expected)");
      }
    } else if (is_csv && 0 == opt_name.case_compare("escape")) {
      if (FileArgValue::STRING == value.kind_ && 1 == value.str_.length()) {
        args.csv_.escape_ = value.str_.ptr()[0];
        args.csv_.has_escape_ = true;
      } else {
        ret = file_sql_raise_not_supported("escape option (single character expected)");
      }
    } else if (is_csv && 0 == opt_name.case_compare("nullstr")) {
      if (FileArgValue::STRING == value.kind_) {
        args.csv_.nullstr_ = value.str_;
        args.csv_.has_nullstr_ = true;
      } else {
        ret = file_sql_raise_parse_error(whole_stmt);
      }
    } else {
      ret = file_sql_raise_not_supported("file table option");
    }
  }
  UNUSED(is_json);
  return ret;
}

// ---------------------------------------------------------------------------
// describe support
static int file_sql_build_describe_sql(ObSqlString &out, const FileSchema &schema)
{
  int ret = OB_SUCCESS;
  if (schema.columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(out.append("select column_name, column_type, `null`, `key`, `default`, extra "
                                 "from (select 0 as __ord, '' as column_name, '' as column_type, "
                                 "'' as `null`, '' as `key`, '' as `default`, '' as extra"))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < schema.columns_.count(); ++i) {
    const FileColumn &col = schema.columns_.at(i);
    if (OB_FAIL(out.append(" union all select "))) {
    } else if (OB_FAIL(out.append_fmt("%ld, ", static_cast<long>(i + 1)))) {
    } else if (OB_FAIL(file_sql_append_sql_string(out, col.name_))) {
    } else if (OB_FAIL(out.append(", "))) {
    } else if (OB_FAIL(file_sql_append_sql_string(
                 out, ObString(static_cast<int32_t>(strlen(file_sql_type_name(col.type_))),
                               const_cast<char *>(file_sql_type_name(col.type_)))))) {
    } else if (OB_FAIL(out.append(", "))) {
    } else if (OB_FAIL(file_sql_append_sql_string(
                 out, ObString::make_string(col.nullable_ ? "YES" : "NO")))) {
    } else if (OB_FAIL(out.append(", "))) {
    } else if (OB_FAIL(file_sql_append_sql_string(out, ObString::make_string("NULL")))) {
    } else if (OB_FAIL(out.append(", "))) {
    } else if (OB_FAIL(file_sql_append_sql_string(out, ObString::make_string("NULL")))) {
    } else if (OB_FAIL(out.append(", "))) {
    } else if (OB_FAIL(file_sql_append_sql_string(out, ObString::make_string("NULL")))) {
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(out.append(") t where __ord > 0 order by __ord"))) {
  }
  return ret;
}

// ---------------------------------------------------------------------------
// main preprocessing
static ObString file_sql_path_to_check(const ObString &path, const bool is_glob)
{
  ObString check_path = path;
  if (is_glob && path.length() > 0) {
    int64_t slash = -1;
    for (int64_t i = path.length() - 1; -1 == slash && i >= 0; --i) {
      if ('/' == path.ptr()[i]) {
        slash = i;
      }
    }
    if (slash > 0) {
      check_path.assign_ptr(path.ptr(), static_cast<int32_t>(slash));
    }
  }
  return check_path;
}

static bool file_sql_trimmed_starts_with(const char *text, const int64_t len,
                                         const char *prefix, int64_t &after)
{
  int64_t pos = 0;
  file_sql_skip_space(text, len, pos);
  const int64_t plen = static_cast<int64_t>(strlen(prefix));
  bool match = (pos + plen <= len && 0 == strncasecmp(text + pos, prefix, plen));
  if (match) {
    after = pos + plen;
  }
  return match;
}

static int file_sql_handle_enable_statement(const ObString &stmt,
                                            ObSQLSessionInfo &session,
                                            ObSqlString &rewritten,
                                            bool &handled)
{
  int ret = OB_SUCCESS;
  handled = false;
  const char *text = stmt.ptr();
  const int64_t len = stmt.length();
  int64_t after = 0;
  if (file_sql_trimmed_starts_with(text, len, "set", after)) {
    // set session enable_file_sql = on/off
    int64_t eq = -1;
    if (file_sql_find_ci(text, len, 0, "enable_file_sql", 15, eq)) {
      int64_t pos = eq + 15;
      while (pos < len && '=' != text[pos]) {
        ++pos;
      }
      if (pos < len) {
        ++pos;
        file_sql_skip_space(text, len, pos);
        const int64_t begin = pos;
        while (pos < len && (isalnum(static_cast<unsigned char>(text[pos])) || '_' == text[pos])) {
          ++pos;
        }
        ObString value(static_cast<int32_t>(pos - begin), text + begin);
        bool enabled = false;
        if (0 == value.case_compare("on") || 0 == value.case_compare("true")
            || 0 == value.case_compare("1")) {
          enabled = true;
        } else if (0 == value.case_compare("off") || 0 == value.case_compare("false")
                   || 0 == value.case_compare("0")) {
          enabled = false;
        } else {
          ret = file_sql_raise_parse_error(stmt);
        }
        if (OB_SUCC(ret)) {
          file_sql_set_session_enabled(session.get_server_sid(), enabled);
          if (OB_FAIL(rewritten.append_fmt("set @__file_sql_enabled = %d", enabled ? 1 : 0))) {
          } else {
            handled = true;
          }
        }
      }
    }
  } else {
    // select @@session.enable_file_sql ...
    bool found = file_sql_contains_ci(text, len, "@@session.enable_file_sql")
              || file_sql_contains_ci(text, len, "@@enable_file_sql");
    if (found) {
      int64_t last = 0;
      int64_t pos = -1;
      while (OB_SUCC(ret)) {
        int64_t p1 = -1;
        int64_t p2 = -1;
        bool has1 = file_sql_find_ci(text, len, last, "@@session.enable_file_sql", 25, p1);
        bool has2 = file_sql_find_ci(text, len, last, "@@enable_file_sql", 18, p2);
        if (!has1 && !has2) {
          break;
        }
        const bool use1 = (has1 && (!has2 || p1 <= p2));
        pos = use1 ? p1 : p2;
        const int32_t mlen = use1 ? 25 : 18;
        if (OB_FAIL(rewritten.append(text + last, static_cast<int32_t>(pos - last)))) {
        } else if (OB_FAIL(rewritten.append("ifnull(@__file_sql_enabled, 0)"))) {
        } else {
          last = pos + mlen;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(rewritten.append(text + last, static_cast<int32_t>(len - last)))) {
        } else {
          handled = true;
        }
      }
    }
  }
  return ret;
}

struct FileFuncMatch
{
  int64_t pos_;
  int64_t name_len_;
  int64_t lparen_;
  int64_t rparen_;
  char name_[32];
};

static int file_sql_find_function(const char *text, const int64_t len, const int64_t from,
                                  FileFuncMatch &match)
{
  static const char *names[] = {"read_csv", "read_json", "read_ndjson", "read_parquet", "glob"};
  int ret = OB_SUCCESS;
  int64_t best = -1;
  int best_idx = -1;
  for (int n = 0; n < static_cast<int>(sizeof(names) / sizeof(names[0])); ++n) {
    int64_t pos = -1;
    if (file_sql_find_ci(text, len, from, names[n], static_cast<int64_t>(strlen(names[n])), pos)) {
      // word boundary check
      if (pos > 0 && (isalnum(static_cast<unsigned char>(text[pos - 1])) || '_' == text[pos - 1])) {
        continue;
      }
      if (best < 0 || pos < best) {
        best = pos;
        best_idx = n;
      }
    }
  }
  if (best < 0) {
    ret = OB_ITER_END;
  } else {
    const int64_t name_len = static_cast<int64_t>(strlen(names[best_idx]));
    int64_t i = best + name_len;
    while (i < len && file_sql_is_space(text[i])) {
      ++i;
    }
    if (i >= len || '(' != text[i]) {
      // not a function call; search further
      ret = file_sql_find_function(text, len, best + name_len, match);
    } else {
      // find matching rparen, respecting quotes and backticks
      int64_t depth = 0;
      int64_t rparen = -1;
      char quote = '\0';
      for (int64_t k = i; k < len && -1 == rparen; ++k) {
        const char c = text[k];
        if ('\0' != quote) {
          if ('\\' == c && k + 1 < len) {
            ++k;
          } else if (c == quote) {
            quote = '\0';
          }
        } else if ('\'' == c || '"' == c || '`' == c) {
          quote = c;
        } else if ('(' == c) {
          ++depth;
        } else if (')' == c) {
          --depth;
          if (0 == depth) {
            rparen = k;
          }
        }
      }
      if (-1 == rparen) {
        ret = OB_ERR_PARSE_SQL;
      } else {
        match.pos_ = best;
        match.name_len_ = name_len;
        match.lparen_ = i;
        match.rparen_ = rparen;
        MEMCPY(match.name_, names[best_idx], name_len);
        match.name_[name_len] = '\0';
      }
    }
  }
  return ret;
}


static bool file_sql_trimmed_starts_with(const char *text, const int64_t len,
                                         const char *prefix, int64_t &after);
// ---------------------------------------------------------------------------
// File materialization is executed on the server timer thread: creating tables
// through the inner sql proxy from a SQL worker thread would re-enter the
// SQL engine on the same thread (nested DDL transaction) which is not supported.
struct FileSqlJob
{
  std::mutex mutex_;
  std::condition_variable cv_;
  bool done_;
  int ret_;
  int format_;  // 0 csv, 1 json, 2 parquet, 3 glob
  std::string path_;
  FileCsvOptions opts_;
  std::string table_;
  struct JobColumn
  {
    std::string name_;
    int type_;
    bool nullable_;
    JobColumn() : name_(), type_(0), nullable_(false) {}
  };
  JobColumn cols_[64];
  int col_cnt_;
  FileSqlJob()
    : mutex_(), cv_(), done_(false), ret_(OB_SUCCESS), format_(0), path_(), opts_(),
      table_(), cols_(), col_cnt_(0) {}
};

static std::mutex g_file_sql_jobs_mutex;
static std::deque<FileSqlJob *> g_file_sql_jobs;

static int file_sql_run_job(FileSqlJob *job)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena;
  FileSchema schema;
  FileSqlTableBuilder builder;
  ObSqlString path_str;
  if (OB_ISNULL(job)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(path_str.assign(job->path_.c_str()))) {
  } else {
    ObString path(path_str.string());
    if (0 == job->format_) {
      ret = file_sql_materialize_csv(path, job->opts_, arena, builder, schema);
    } else if (1 == job->format_) {
      ret = file_sql_materialize_json(path, arena, builder, schema);
    } else if (2 == job->format_) {
      ret = file_sql_materialize_parquet(path, arena, builder, schema);
    } else {
      ret = file_sql_materialize_glob(path, arena, builder, schema);
    }
  }
  if (OB_SUCC(ret)) {
    job->table_.assign(builder.table_name());
    job->col_cnt_ = static_cast<int>(schema.columns_.count() > 64 ? 64 : schema.columns_.count());
    for (int i = 0; i < job->col_cnt_; ++i) {
      job->cols_[i].name_.assign(schema.columns_.at(i).name_.ptr(),
                                 schema.columns_.at(i).name_.length());
      job->cols_[i].type_ = static_cast<int>(schema.columns_.at(i).type_);
      job->cols_[i].nullable_ = schema.columns_.at(i).nullable_;
    }
  }
  return ret;
}

int file_sql_run_pending_jobs()
{
  int ret = OB_SUCCESS;
  while (true) {
    FileSqlJob *job = NULL;
    {
      std::lock_guard<std::mutex> guard(g_file_sql_jobs_mutex);
      if (g_file_sql_jobs.empty()) {
        break;
      }
      job = g_file_sql_jobs.front();
      g_file_sql_jobs.pop_front();
    }
    if (NULL != job) {
      job->ret_ = file_sql_run_job(job);
      {
        std::lock_guard<std::mutex> guard(job->mutex_);
        job->done_ = true;
      }
      job->cv_.notify_all();
    }
  }
  return ret;
}

// submit a job and wait for the timer thread; returns job result and fills schema
static int file_sql_materialize_sync(FileSqlJob &job,
                                     ObIAllocator &alloc,
                                     ObString &table_name,
                                     FileSchema &schema)
{
  int ret = OB_SUCCESS;
  {
    std::lock_guard<std::mutex> guard(g_file_sql_jobs_mutex);
    g_file_sql_jobs.push_back(&job);
  }
  {
    std::unique_lock<std::mutex> lock(job.mutex_);
    if (!job.cv_.wait_for(lock, std::chrono::seconds(120),
                          [&job]() { return job.done_; })) {
      ret = OB_TIMEOUT;
    }
  }
  if (OB_SUCC(ret)) {
    ret = job.ret_;
  }
  if (OB_SUCC(ret)) {
    ObString table_store;
    if (OB_FAIL(ob_write_string(alloc, ObString(static_cast<int32_t>(job.table_.length()),
                                                const_cast<char *>(job.table_.c_str())), table_store))) {
    } else if (OB_FAIL(ob_write_string(alloc, table_store, table_name))) {
    }
  }
  if (OB_SUCC(ret)) {
    for (int i = 0; OB_SUCC(ret) && i < job.col_cnt_; ++i) {
      FileColumn col;
      ObString name;
      if (OB_FAIL(ob_write_string(alloc,
                  ObString(static_cast<int32_t>(job.cols_[i].name_.length()),
                           const_cast<char *>(job.cols_[i].name_.c_str())), name))) {
      } else {
        col.name_ = name;
        col.type_ = static_cast<FileColType>(job.cols_[i].type_);
        col.nullable_ = job.cols_[i].nullable_;
        col.max_bytes_ = 64;
        if (OB_FAIL(schema.columns_.push_back(col))) {
        }
      }
    }
  }
  return ret;
}

static int file_sql_preprocess(const ObString &stmt, ObSQLSessionInfo &session,
                               ObSqlString &rewritten, bool &handled);


// ---------------------------------------------------------------------------
// COPY ... TO 'file' (CSV export): the engine writes the rows through
// SELECT ... INTO OUTFILE and the header line / nullstr are applied afterwards.
struct FileCopyPending
{
  bool active_;
  bool has_header_;
  ObSqlString target_;
  ObSqlString body_;
  ObSqlString header_;
  ObString nullstr_;
  bool has_nullstr_;
  FileCopyPending()
    : active_(false), has_header_(false), target_(), body_(), header_(), nullstr_(),
      has_nullstr_(false) {}
};

static thread_local FileCopyPending g_file_copy_pending;

static int file_sql_copy_append_header_field(ObSqlString &header, const ObString &name,
                                             const char delim, const char quote)
{
  int ret = OB_SUCCESS;
  bool need_quote = false;
  for (int64_t i = 0; i < name.length(); ++i) {
    const char c = name.ptr()[i];
    if (c == delim || c == quote || '\n' == c || '\r' == c) {
      need_quote = true;
    }
  }
  if (need_quote && OB_FAIL(header.append_fmt("%c", quote))) {
  } else if (OB_FAIL(header.append(name))) {
  } else if (need_quote && OB_FAIL(header.append_fmt("%c", quote))) {
  }
  return ret;
}

static int file_sql_finish_pending_copy(int exec_ret)
{
  int ret = OB_SUCCESS;
  if (!g_file_copy_pending.active_) {
  } else if (OB_SUCCESS != exec_ret) {
    (void)unlink(g_file_copy_pending.body_.ptr());
  } else {
    FILE *body = fopen(g_file_copy_pending.body_.ptr(), "rb");
    FILE *out = fopen(g_file_copy_pending.target_.ptr(), "wb");
    if (NULL == body || NULL == out) {
      ret = OB_IO_ERROR;
    } else {
      if (g_file_copy_pending.has_header_ && g_file_copy_pending.header_.length() > 0) {
        if (fwrite(g_file_copy_pending.header_.ptr(), 1,
                   static_cast<size_t>(g_file_copy_pending.header_.length()), out)
            != static_cast<size_t>(g_file_copy_pending.header_.length())) {
          ret = OB_IO_ERROR;
        }
      }
      char buf[65536];
      size_t n = 0;
      while (OB_SUCC(ret) && (n = fread(buf, 1, sizeof(buf), body)) > 0) {
        if (fwrite(buf, 1, n, out) != n) {
          ret = OB_IO_ERROR;
        }
      }
    }
    if (NULL != body) {
      fclose(body);
      body = NULL;
    }
    if (NULL != out) {
      fclose(out);
      out = NULL;
    }
    (void)unlink(g_file_copy_pending.body_.ptr());
    if (OB_FAIL(ret)) {
      (void)unlink(g_file_copy_pending.target_.ptr());
    }
  }
  g_file_copy_pending.active_ = false;
  g_file_copy_pending.has_header_ = false;
  g_file_copy_pending.target_.reset();
  g_file_copy_pending.body_.reset();
  g_file_copy_pending.header_.reset();
  g_file_copy_pending.nullstr_.reset();
  g_file_copy_pending.has_nullstr_ = false;
  return ret;
}

static int file_sql_copy_next_non_space(const char *s, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  file_sql_skip_space(s, len, pos);
  return ret;
}

// split select list on top level commas; returns the number of items via array
static int file_sql_copy_split_select_list(ObIAllocator &alloc, const ObString &select_list,
                                           ObIArray<ObString> &names)
{
  int ret = OB_SUCCESS;
  int64_t item_begin = 0;
  int64_t depth = 0;
  char quote = '\0';
  for (int64_t i = 0; i <= select_list.length() && OB_SUCC(ret); ++i) {
    const char c = (i < select_list.length()) ? select_list.ptr()[i] : ',';
    bool split = false;
    if ('\0' != quote) {
      if (c == quote) {
        quote = '\0';
      }
    } else if ('\'' == c || '"' == c || '`' == c) {
      quote = c;
    } else if ('(' == c) {
      ++depth;
    } else if (')' == c) {
      --depth;
    } else if (0 == depth && ',' == c) {
      split = true;
    }
    if (OB_SUCC(ret) && split) {
      ObString item(static_cast<int32_t>(i - item_begin), select_list.ptr() + item_begin);
      item_begin = i + 1;
      int64_t b = 0;
      int64_t e = item.length();
      while (b < e && file_sql_is_space(item.ptr()[b])) {
        ++b;
      }
      while (e > b && file_sql_is_space(item.ptr()[e - 1])) {
        --e;
      }
      ObString name(e - b, item.ptr() + b);
      // strip " as alias"
      int64_t as_pos = -1;
      int64_t d2 = 0;
      char q2 = '\0';
      for (int64_t k = 0; k + 4 <= name.length(); ++k) {
        const char cc = name.ptr()[k];
        if ('\0' != q2) {
          if (cc == q2) {
            q2 = '\0';
          }
        } else if ('\'' == cc || '"' == cc || '`' == cc) {
          q2 = cc;
        } else if ('(' == cc) {
          ++d2;
        } else if (')' == cc) {
          --d2;
        } else if (0 == d2 && 0 == strncasecmp(name.ptr() + k, " as ", 4)) {
          as_pos = k;
        }
      }
      if (as_pos >= 0) {
        int64_t nb = as_pos + 4;
        while (nb < name.length() && file_sql_is_space(name.ptr()[nb])) {
          ++nb;
        }
        name.assign_ptr(name.ptr() + nb, static_cast<int32_t>(name.length() - nb));
      }
      if (name.length() >= 2 && '`' == name.ptr()[0]
          && '`' == name.ptr()[name.length() - 1]) {
        name.assign_ptr(name.ptr() + 1, name.length() - 2);
      }
      ObString stored;
      if (OB_FAIL(ob_write_string(alloc, name, stored))) {
      } else if (OB_FAIL(names.push_back(stored))) {
      }
    }
  }
  return ret;
}

static int file_sql_handle_copy(const ObString &stmt, ObSQLSessionInfo &session,
                                const bool enabled, ObSqlString &rewritten, bool &handled)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObArenaAllocator arena;
  const char *text = stmt.ptr();
  const int64_t len = stmt.length();
  ObString inner;
  handled = false;
  UNUSED(enabled);
  if (file_sql_trimmed_starts_with(text, len, "copy", pos)) {
    file_sql_skip_space(text, len, pos);
  }
  if (pos < len && '(' == text[pos]) {
    ++pos;
    const int64_t inner_begin = pos;
    int64_t depth = 1;
    char quote = '\0';
    int64_t inner_end = -1;
    for (int64_t i = pos; i < len && -1 == inner_end; ++i) {
      const char c = text[i];
      if ('\0' != quote) {
        if ('\\' == c && i + 1 < len) {
          ++i;
        } else if (c == quote) {
          quote = '\0';
        }
      } else if ('\'' == c || '"' == c || '`' == c) {
        quote = c;
      } else if ('(' == c) {
        ++depth;
      } else if (')' == c) {
        --depth;
        if (0 == depth) {
          inner_end = i;
          pos = i + 1;
        }
      }
    }
    if (-1 == inner_end) {
      ret = file_sql_raise_parse_error(stmt);
    } else {
      inner.assign_ptr(text + inner_begin, static_cast<int32_t>(inner_end - inner_begin));
    }
  } else {
    const int64_t name_begin = pos;
    while (pos < len && (isalnum(static_cast<unsigned char>(text[pos])) || '_' == text[pos]
                         || '.' == text[pos])) {
      ++pos;
    }
    if (pos == name_begin) {
      ret = file_sql_raise_parse_error(stmt);
    } else {
      ObSqlString select_sql;
      if (OB_FAIL(select_sql.append_fmt("select * from %.*s",
                                        static_cast<int>(pos - name_begin),
                                        text + name_begin))) {
      } else if (OB_FAIL(ob_write_string(arena, select_sql.string(), inner))) {
      }
    }
  }
  file_sql_skip_space(text, len, pos);
  if (OB_SUCC(ret) && (pos + 2 > len || 0 != strncasecmp(text + pos, "to", 2))) {
    ret = file_sql_raise_parse_error(stmt);
  } else if (OB_SUCC(ret)) {
    pos += 2;
    file_sql_skip_space(text, len, pos);
  }
  ObString target;
  char delim = ',';
  char quote = '"';
  bool has_header = false;
  bool has_nullstr = false;
  ObString nullstr;
  if (OB_SUCC(ret)
      && OB_FAIL(file_sql_parse_string_literal(text, len, pos, target, arena))) {
    ret = file_sql_raise_parse_error(stmt);
  } else if (OB_SUCC(ret) && OB_FAIL(ob_write_string(arena, target, target, true))) {
  }
  file_sql_skip_space(text, len, pos);
  if (OB_SUCC(ret) && pos < len && '(' == text[pos]) {
    ++pos;
    while (OB_SUCC(ret) && pos < len && ')' != text[pos]) {
      file_sql_skip_space(text, len, pos);
      const int64_t opt_begin = pos;
      while (pos < len && (isalnum(static_cast<unsigned char>(text[pos])) || '_' == text[pos])) {
        ++pos;
      }
      ObString opt(static_cast<int32_t>(pos - opt_begin), text + opt_begin);
      file_sql_skip_space(text, len, pos);
      FileArgValue value;
      bool has_value = false;
      if (pos < len && '=' == text[pos]) {
        ++pos;
        file_sql_skip_space(text, len, pos);
        if (OB_FAIL(file_sql_parse_arg_value(arena, text, len, pos, value))) {
          ret = file_sql_raise_parse_error(stmt);
          break;
        }
        has_value = true;
      } else if (pos < len && ('\'' == text[pos] || '"' == text[pos])) {
        if (OB_FAIL(file_sql_parse_arg_value(arena, text, len, pos, value))) {
          ret = file_sql_raise_parse_error(stmt);
          break;
        }
        has_value = true;
      } else if (pos < len && isalpha(static_cast<unsigned char>(text[pos]))) {
        // bare identifier value, e.g. "format csv"
        if (OB_FAIL(file_sql_parse_arg_value(arena, text, len, pos, value))) {
          ret = file_sql_raise_parse_error(stmt);
          break;
        }
        has_value = true;
      }
      if (OB_SUCC(ret)) {
        if (0 == opt.case_compare("header")) {
          has_header = true;
        } else if (0 == opt.case_compare("format")) {
          if (!has_value || FileArgValue::IDENT != value.kind_
              || 0 != value.str_.case_compare("csv")) {
            ret = file_sql_raise_not_supported("COPY format other than csv");
          }
        } else if (0 == opt.case_compare("delim") || 0 == opt.case_compare("delimiter")) {
          if (has_value && FileArgValue::STRING == value.kind_ && 1 == value.str_.length()) {
            delim = value.str_.ptr()[0];
          } else {
            ret = file_sql_raise_not_supported("COPY delim option");
          }
        } else if (0 == opt.case_compare("quote")) {
          if (has_value && FileArgValue::STRING == value.kind_ && 1 == value.str_.length()) {
            quote = value.str_.ptr()[0];
          } else {
            ret = file_sql_raise_not_supported("COPY quote option");
          }
        } else if (0 == opt.case_compare("escape")) {
          if (!has_value) {
            ret = file_sql_raise_not_supported("COPY escape option");
          }
        } else if (0 == opt.case_compare("nullstr")) {
          if (has_value && FileArgValue::STRING == value.kind_) {
            nullstr = value.str_;
            has_nullstr = true;
          } else {
            ret = file_sql_raise_not_supported("COPY nullstr option");
          }
        } else {
          ret = file_sql_raise_not_supported("COPY option");
        }
      }
      file_sql_skip_space(text, len, pos);
      if (OB_SUCC(ret) && pos < len && ',' == text[pos]) {
        ++pos;
      }
    }
    if (OB_SUCC(ret) && (pos >= len || ')' != text[pos])) {
      ret = file_sql_raise_parse_error(stmt);
    }
  }
  if (OB_SUCC(ret)) {
    struct stat st;
    if (0 == stat(target.ptr(), &st)) {
      ret = file_sql_raise_not_supported("COPY target file already exists");
    }
  }
  if (OB_SUCC(ret)) {
    ObSqlString inner_rewritten;
    bool inner_handled = false;
    if (OB_FAIL(file_sql_preprocess(inner, session, inner_rewritten, inner_handled))) {
    } else {
      const ObString inner_sql = inner_handled ? inner_rewritten.string() : inner;
      int64_t select_pos = -1;
      if (!file_sql_find_ci(inner_sql.ptr(), inner_sql.length(), 0, "select", 6, select_pos)) {
        ret = file_sql_raise_not_supported("COPY query must be a SELECT");
      } else {
        int64_t list_begin = select_pos + 6;
        int64_t from_pos = -1;
        int64_t depth2 = 0;
        char quote2 = '\0';
        for (int64_t i = list_begin; i + 6 <= inner_sql.length() && -1 == from_pos; ++i) {
          const char c = inner_sql.ptr()[i];
          if ('\0' != quote2) {
            if (c == quote2) {
              quote2 = '\0';
            }
          } else if ('\'' == c || '"' == c || '`' == c) {
            quote2 = c;
          } else if ('(' == c) {
            ++depth2;
          } else if (')' == c) {
            --depth2;
          } else if (0 == depth2 && i + 4 <= inner_sql.length()
                     && 0 == strncasecmp(inner_sql.ptr() + i, "from", 4)
                     && (i == 0 || file_sql_is_space(inner_sql.ptr()[i - 1]))
                     && (i + 4 == inner_sql.length()
                         || file_sql_is_space(inner_sql.ptr()[i + 4])
                         || '(' == inner_sql.ptr()[i + 4])) {
            from_pos = i;
          }
        }
        const int64_t list_end = (-1 == from_pos) ? inner_sql.length() : from_pos;
        const ObString select_list(static_cast<int32_t>(list_end - list_begin),
                                   inner_sql.ptr() + list_begin);
        const ObString rest(static_cast<int32_t>(inner_sql.length() - list_end),
                            inner_sql.ptr() + list_end);
        ObSqlString header;
        if (has_header) {
          ObSEArray<ObString, 16> names;
          if (OB_FAIL(file_sql_copy_split_select_list(arena, select_list, names))) {
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < names.count(); ++i) {
            if (i > 0 && OB_FAIL(header.append_fmt("%c", delim))) {
            } else if (OB_FAIL(file_sql_copy_append_header_field(header, names.at(i),
                                                                 delim, quote))) {
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(header.append("\n"))) {
          }
        }
        char quote_buf[16];
        char delim_buf[16];
        if (quote == '\'') {
          snprintf(quote_buf, sizeof(quote_buf), "\\'");
        } else {
          snprintf(quote_buf, sizeof(quote_buf), "%c", quote);
        }
        if (delim == '\'') {
          snprintf(delim_buf, sizeof(delim_buf), "\\'");
        } else {
          snprintf(delim_buf, sizeof(delim_buf), "%c", delim);
        }
        ObSqlString outfile_sql;
        if (OB_SUCC(ret)) {
          if (OB_FAIL(outfile_sql.append_fmt("select * into outfile '%.*s' "
                                            "fields terminated by '%.*s' "
                                            "optionally enclosed by '%.*s' escaped by '%.*s' from (",
                                            target.length(), target.ptr(),
                                            static_cast<int>(strlen(delim_buf)), delim_buf,
                                            static_cast<int>(strlen(quote_buf)), quote_buf,
                                            static_cast<int>(strlen(quote_buf)), quote_buf))) {
          } else if (has_header) {
            if (OB_FAIL(outfile_sql.append("select "))) {
            }
            ObSEArray<ObString, 16> names;
            if (OB_FAIL(file_sql_copy_split_select_list(arena, select_list, names))) {
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && i < names.count(); ++i) {
                if (i > 0 && OB_FAIL(outfile_sql.append(", "))) {
                } else if (OB_FAIL(file_sql_append_sql_string(outfile_sql, names.at(i)))) {
                } else if (OB_FAIL(outfile_sql.append_fmt(" as c%ld", static_cast<long>(i)))) {
                }
              }
              if (OB_SUCC(ret) && OB_FAIL(outfile_sql.append(" union all "))) {
              }
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(outfile_sql.append_fmt(
                "select * from (%.*s) __file_sql_copy_body) __file_sql_copy_out",
                inner_sql.length(), inner_sql.ptr()))) {
          } else if (OB_SUCC(ret) && OB_FAIL(rewritten.assign(outfile_sql))) {
          } else if (OB_SUCC(ret)) {
            handled = true;
          }
        }
      }
    }
  }
  UNUSED(file_sql_copy_next_non_space);
  return ret;
}

static int file_sql_preprocess(const ObString &stmt,
                               ObSQLSessionInfo &session,
                               ObSqlString &rewritten,
                               bool &handled)
{
  int ret = OB_SUCCESS;
  handled = false;
  const char *text = stmt.ptr();
  const int64_t len = stmt.length();
  if (OB_ISNULL(text) || len <= 0) {
  } else if (file_sql_contains_ci(text, len, "enable_file_sql")) {
    if (OB_FAIL(file_sql_handle_enable_statement(stmt, session, rewritten, handled))) {
    }
  } else if (!file_sql_contains_ci(text, len, "read_")
             && !file_sql_contains_ci(text, len, "glob")) {
    // fast path
  } else {
    const bool enabled = file_sql_session_enabled(session.get_server_sid());
    int64_t after = 0;
    bool is_describe = file_sql_trimmed_starts_with(text, len, "describe", after)
                    || file_sql_trimmed_starts_with(text, len, "desc", after);
    if (is_describe) {
      int64_t p = after;
      file_sql_skip_space(text, len, p);
      if (!file_sql_has_prefix_ci(text + p, len - p, "select")) {
        is_describe = false;
      }
    }
    bool is_copy = file_sql_trimmed_starts_with(text, len, "copy", after);
    if (is_copy) {
      if (!enabled) {
        ret = file_sql_raise_not_supported("file sql is disabled (set session enable_file_sql = on)");
      } else if (OB_FAIL(file_sql_handle_copy(stmt, session, enabled, rewritten, handled))) {
      }
    } else {
      ObArenaAllocator arena;
      FileSchema first_schema;
      bool has_schema = false;
      ObSqlString out;
      int64_t last = 0;
      int64_t search_from = 0;
      int64_t alias_seq = 0;
      FileFuncMatch match;
      while (OB_SUCC(ret)) {
        ret = file_sql_find_function(text, len, search_from, match);
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else if (OB_FAIL(ret)) {
          ret = file_sql_raise_parse_error(stmt);
          break;
        } else {
          if (!enabled) {
            ret = file_sql_raise_not_supported("file sql is disabled (set session enable_file_sql = on)");
            break;
          }
          const ObString func_name(match.name_len_, match.name_);
          const char *args = text + match.lparen_ + 1;
          const int64_t args_len = match.rparen_ - match.lparen_ - 1;
          FileFunctionArgs fargs;
          const bool is_csv = (0 == func_name.case_compare("read_csv"));
          const bool is_json = (0 == func_name.case_compare("read_json")
                                || 0 == func_name.case_compare("read_ndjson"));
          const bool is_parquet = (0 == func_name.case_compare("read_parquet"));
          const bool is_glob = (0 == func_name.case_compare("glob"));
          if (OB_FAIL(file_sql_parse_options(arena, args, args_len, is_csv, is_json, fargs, stmt))) {
          } else if (OB_FAIL(file_sql_check_secure_path(
                       session, file_sql_path_to_check(fargs.path_, is_glob)))) {
          } else {
            FileSqlTableBuilder builder;
            if (is_glob) {
              if (OB_FAIL(file_sql_materialize_glob(fargs.path_, arena, builder, first_schema))) {
              }
            } else if (is_csv) {
              if (OB_FAIL(file_sql_materialize_csv(fargs.path_, fargs.csv_, arena, builder,
                                                  first_schema))) {
              }
            } else if (is_json) {
              if (OB_FAIL(file_sql_materialize_json(fargs.path_, arena, builder, first_schema))) {
              }
            } else if (is_parquet) {
              if (OB_FAIL(file_sql_materialize_parquet(fargs.path_, arena, builder, first_schema))) {
              }
            } else {
              ret = file_sql_raise_not_supported("file table function");
            }
            if (OB_FAIL(ret)) {
            } else if (is_describe) {
              if (!has_schema) {
                has_schema = true;
              }
            } else {
              if (OB_FAIL(out.append(text + last, static_cast<int32_t>(match.pos_ - last)))) {
              } else if (OB_FAIL(out.append(builder.table_name()))) {
              } else {
                // derived tables require an alias; keep the user's alias if present
                int64_t next_pos = match.rparen_ + 1;
                file_sql_skip_space(text, len, next_pos);
                const bool has_alias =
                    (next_pos < len
                     && (isalnum(static_cast<unsigned char>(text[next_pos]))
                         || '_' == text[next_pos] || '`' == text[next_pos]));
                if (!has_alias && OB_FAIL(out.append_fmt(" as `__file_sql_alias_%ld`",
                                                         static_cast<long>(++alias_seq)))) {
                }
              }
              last = match.rparen_ + 1;
            }
            if (OB_SUCC(ret)) {
              search_from = match.rparen_ + 1;
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (is_describe) {
          if (!has_schema) {
            ret = file_sql_raise_not_supported("describe file table");
          } else if (OB_FAIL(file_sql_build_describe_sql(out, first_schema))) {
          } else {
            handled = true;
          }
        } else if (0 == out.length()) {
          handled = false;
        } else {
          if (OB_FAIL(out.append(text + last, static_cast<int32_t>(len - last)))) {
          } else {
            handled = true;
          }
        }
      }
      if (OB_SUCC(ret) && handled) {
        if (OB_FAIL(rewritten.assign(out))) {
        } else if (!is_describe) {
          // refresh the cached schema guard so that the resolver can see the
          // temporary tables that were just created by inner sessions
          int tmp_ret = ObSPIService::force_refresh_schema();
          int guard_ret = session.get_cached_schema_guard_info().refresh_runtime_schema_guard();
          LOG_DEBUG("file sql rewrite done", "sql_len", rewritten.length(), K(tmp_ret), K(guard_ret));
        }
      }
    }
  }
  if (OB_ERR_PARSE_SQL == ret) {
    // let the normal parser report the syntax error (keeps connection state sane)
    rewritten.reset();
    if (OB_SUCC(rewritten.assign("select * from"))) {
      handled = true;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}


} // namespace file_sql
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_FILE_SQL_IMPL_INC_
