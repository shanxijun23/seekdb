/*
 * File SQL: common helpers (column types, value classification, materialization).
 */
#ifndef OCEANBASE_SQL_OB_FILE_SQL_UTIL_INC_
#define OCEANBASE_SQL_OB_FILE_SQL_UTIL_INC_

#include <ctype.h>
#include <stdlib.h>
#include <limits.h>

namespace oceanbase
{
namespace sql
{
namespace file_sql
{

enum FileColType
{
  FILE_COL_NULL = 0,
  FILE_COL_BIGINT = 1,
  FILE_COL_DOUBLE = 2,
  FILE_COL_BOOLEAN = 3,
  FILE_COL_VARCHAR = 4,
  FILE_COL_DATE = 5,
  FILE_COL_DATETIME = 6,
};

struct FileColumn
{
  ObString name_;
  FileColType type_;
  bool nullable_;
  int64_t max_bytes_;
  FileColumn()
    : name_(), type_(FILE_COL_NULL), nullable_(false), max_bytes_(0) {}
  void reset()
  {
    name_.reset();
    type_ = FILE_COL_NULL;
    nullable_ = false;
    max_bytes_ = 0;
  }
};

struct FileSchema
{
  ObSEArray<FileColumn, 16> columns_;
  bool inited_;
  FileSchema() : columns_(), inited_(false) {}
};

static int file_sql_ascii_lower(const char c)
{
  return (c >= 'A' && c <= 'Z') ? (static_cast<int>(c - 'A') + 'a') : c;
}

static bool file_sql_is_space(const char c)
{
  return ' ' == c || '\t' == c || '\n' == c || '\r' == c || '\f' == c || '\v' == c;
}

static bool file_sql_skip_space(const char *s, const int64_t len, int64_t &pos)
{
  while (pos < len && file_sql_is_space(s[pos])) {
    ++pos;
  }
  return pos <= len;
}

static bool file_sql_find_ci(const char *text, const int64_t text_len, const int64_t from,
                             const char *pat, const int64_t pat_len, int64_t &pos)
{
  bool found = false;
  for (int64_t i = from; !found && i + pat_len <= text_len; ++i) {
    bool match = true;
    for (int64_t j = 0; match && j < pat_len; ++j) {
      if (file_sql_ascii_lower(text[i + j]) != file_sql_ascii_lower(pat[j])) {
        match = false;
      }
    }
    if (match) {
      pos = i;
      found = true;
    }
  }
  return found;
}

static int file_sql_parse_string_literal(const char *s, const int64_t len, int64_t &pos,
                                         ObString &value, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (pos >= len || ('\'' != s[pos] && '"' != s[pos])) {
    ret = OB_ERR_PARSE_SQL;
  } else {
    char *buf = static_cast<char *>(allocator.alloc(len - pos + 1));
    const char quote = s[pos++];
    bool closed = false;
    int64_t out_len = 0;
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    while (OB_SUCC(ret) && pos < len && !closed) {
      const char c = s[pos];
      if (quote == c) {
        if (pos + 1 < len && quote == s[pos + 1]) {
          buf[out_len++] = quote;
          pos += 2;
        } else {
          ++pos;
          closed = true;
        }
      } else if ('\\' == c && pos + 1 < len) {
        const char n = s[pos + 1];
        switch (n) {
          case 'n': buf[out_len++] = '\n'; break;
          case 't': buf[out_len++] = '\t'; break;
          case 'r': buf[out_len++] = '\r'; break;
          case '0': buf[out_len++] = '\0'; break;
          case 'b': buf[out_len++] = '\b'; break;
          case 'Z': buf[out_len++] = '\032'; break;
          case '\\': buf[out_len++] = '\\'; break;
          case '\'': buf[out_len++] = '\''; break;
          case '"': buf[out_len++] = '"'; break;
          default: buf[out_len++] = n; break;
        }
        pos += 2;
      } else {
        buf[out_len++] = c;
        ++pos;
      }
    }
    if (OB_SUCC(ret) && !closed) {
      ret = OB_ERR_PARSE_SQL;
    }
    if (OB_SUCC(ret)) {
      value.assign_ptr(buf, static_cast<int32_t>(out_len));
    }
  }
  return ret;
}

static const char *file_sql_type_name(const FileColType type)
{
  const char *name = "VARCHAR";
  switch (type) {
    case FILE_COL_NULL: name = "NULL"; break;
    case FILE_COL_BIGINT: name = "BIGINT"; break;
    case FILE_COL_DOUBLE: name = "DOUBLE"; break;
    case FILE_COL_BOOLEAN: name = "BOOLEAN"; break;
    case FILE_COL_VARCHAR: name = "VARCHAR"; break;
    case FILE_COL_DATE: name = "DATE"; break;
    case FILE_COL_DATETIME: name = "DATETIME"; break;
    default: break;
  }
  return name;
}

static const char *file_sql_ddl_type(const FileColumn &col)
{
  const char *ddl = NULL;
  switch (col.type_) {
    case FILE_COL_BIGINT: ddl = "bigint"; break;
    case FILE_COL_DOUBLE: ddl = "double"; break;
    case FILE_COL_BOOLEAN: ddl = "bool"; break;
    case FILE_COL_DATE: ddl = "date"; break;
    case FILE_COL_DATETIME: ddl = "datetime"; break;
    case FILE_COL_VARCHAR:
    case FILE_COL_NULL:
    default: ddl = "varchar(65535)"; break;
  }
  return ddl;
}

static bool file_sql_is_digit(const char c)
{
  return c >= '0' && c <= '9';
}

static bool file_sql_parse_int64(const char *s, const int64_t len, int64_t &value)
{
  bool ok = false;
  int64_t i = 0;
  bool neg = false;
  if (len > 0 && ('-' == s[0] || '+' == s[0])) {
    neg = ('-' == s[0]);
    i = 1;
  }
  if (i < len) {
    int64_t v = 0;
    bool overflow = false;
    for (; i < len && !overflow; ++i) {
      if (!file_sql_is_digit(s[i])) {
        overflow = true;
      } else {
        const int64_t digit = s[i] - '0';
        if (v > (INT64_MAX - digit) / 10) {
          overflow = true;
        } else {
          v = v * 10 + digit;
        }
      }
    }
    if (!overflow) {
      ok = true;
      value = neg ? -v : v;
    }
  }
  return ok;
}

static bool file_sql_parse_double(const char *s, const int64_t len)
{
  bool ok = false;
  if (len > 0 && len < 64) {
    char buf[128];
    MEMCPY(buf, s, len);
    buf[len] = '\0';
    char *end = NULL;
    errno = 0;
    (void)strtod(buf, &end);
    ok = (NULL != end && '\0' == *end && end != buf && ERANGE != errno);
  }
  return ok;
}

static bool file_sql_is_leap_year(const int year)
{
  return (0 == year % 4 && 0 != year % 100) || 0 == year % 400;
}

static int file_sql_days_in_month(const int year, const int month)
{
  static const int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int ret = 0;
  if (month >= 1 && month <= 12) {
    ret = days[month - 1];
    if (2 == month && file_sql_is_leap_year(year)) {
      ret = 29;
    }
  }
  return ret;
}

// strict date: YYYY-MM-DD
static bool file_sql_is_date(const char *s, const int64_t len)
{
  bool ok = false;
  if (10 == len) {
    int y = 0, m = 0, d = 0;
    bool digits = true;
    for (int64_t i = 0; digits && i < len; ++i) {
      if (i != 4 && i != 7 && !file_sql_is_digit(s[i])) {
        digits = false;
      }
    }
    if (digits && '-' == s[4] && '-' == s[7]) {
      y = (s[0]-'0')*1000 + (s[1]-'0')*100 + (s[2]-'0')*10 + (s[3]-'0');
      m = (s[5]-'0')*10 + (s[6]-'0');
      d = (s[8]-'0')*10 + (s[9]-'0');
      ok = (y >= 1 && y <= 9999 && m >= 1 && m <= 12 && d >= 1 && d <= file_sql_days_in_month(y, m));
    }
  }
  return ok;
}

// strict datetime: YYYY-MM-DD[ T]HH:MM:SS[.fraction]
static bool file_sql_is_datetime(const char *s, const int64_t len)
{
  bool ok = false;
  if (len >= 19 && ((' ' == s[10]) || ('T' == s[10]) || ('t' == s[10]))) {
    bool digits = true;
    for (int64_t i = 0; digits && i < 19; ++i) {
      if (i != 4 && i != 7 && i != 10 && i != 13 && i != 16 && !file_sql_is_digit(s[i])) {
        digits = false;
      }
    }
    if (digits && '-' == s[4] && '-' == s[7] && ':' == s[13] && ':' == s[16]) {
      const int y = (s[0]-'0')*1000 + (s[1]-'0')*100 + (s[2]-'0')*10 + (s[3]-'0');
      const int m = (s[5]-'0')*10 + (s[6]-'0');
      const int d = (s[8]-'0')*10 + (s[9]-'0');
      const int hh = (s[11]-'0')*10 + (s[12]-'0');
      const int mi = (s[14]-'0')*10 + (s[15]-'0');
      const int ss = (s[17]-'0')*10 + (s[18]-'0');
      if (y >= 1 && y <= 9999 && m >= 1 && m <= 12 && d >= 1
          && d <= file_sql_days_in_month(y, m) && hh >= 0 && hh <= 23
          && mi >= 0 && mi <= 59 && ss >= 0 && ss <= 59) {
        ok = true;
        if (len > 19) {
          int64_t pos = 19;
          if ('.' == s[pos]) {
            ++pos;
          }
          if (pos >= len) {
            ok = false;
          }
          for (; ok && pos < len; ++pos) {
            ok = file_sql_is_digit(s[pos]);
          }
        }
      }
    }
  }
  return ok;
}

static bool file_sql_is_bool_literal(const char *s, const int64_t len)
{
  return (4 == len && 0 == strncasecmp(s, "true", 4))
      || (5 == len && 0 == strncasecmp(s, "false", 5));
}

// classify a text token for CSV
static FileColType file_sql_classify_text(const char *s, const int64_t len)
{
  FileColType type = FILE_COL_VARCHAR;
  int64_t iv = 0;
  if (file_sql_is_bool_literal(s, len)) {
    type = FILE_COL_BOOLEAN;
  } else if (file_sql_parse_int64(s, len, iv)) {
    type = FILE_COL_BIGINT;
  } else if (file_sql_parse_double(s, len)) {
    type = FILE_COL_DOUBLE;
  } else if (file_sql_is_date(s, len)) {
    type = FILE_COL_DATE;
  } else if (file_sql_is_datetime(s, len)) {
    type = FILE_COL_DATETIME;
  }
  return type;
}

static FileColType file_sql_common_type(const FileColType a, const FileColType b)
{
  FileColType result = FILE_COL_VARCHAR;
  if (FILE_COL_NULL == a) {
    result = b;
  } else if (FILE_COL_NULL == b) {
    result = a;
  } else if (a == b) {
    result = a;
  } else if ((FILE_COL_BIGINT == a && FILE_COL_DOUBLE == b)
             || (FILE_COL_DOUBLE == a && FILE_COL_BIGINT == b)) {
    result = FILE_COL_DOUBLE;
  } else if ((FILE_COL_DATE == a && FILE_COL_DATETIME == b)
             || (FILE_COL_DATETIME == a && FILE_COL_DATE == b)) {
    result = FILE_COL_DATETIME;
  } else {
    result = FILE_COL_VARCHAR;
  }
  return result;
}

static int file_sql_read_whole_file(const char *path, ObIAllocator &allocator,
                                    char *&buf, int64_t &len)
{
  int ret = OB_SUCCESS;
  buf = NULL;
  len = 0;
  FILE *fp = NULL;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(fp = fopen(path, "rb"))) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("file sql open file failed", K(ret), "path", path);
  } else {
    int64_t cap = 1 << 20;
    char *data = static_cast<char *>(allocator.alloc(cap));
    int64_t used = 0;
    if (OB_ISNULL(data)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      bool eof = false;
      while (OB_SUCC(ret) && !eof) {
        if (used == cap) {
          char *new_data = static_cast<char *>(allocator.alloc(cap * 2));
          if (OB_ISNULL(new_data)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            MEMCPY(new_data, data, used);
            data = new_data;
            cap *= 2;
          }
        }
        if (OB_SUCC(ret)) {
          const size_t n = fread(data + used, 1, static_cast<size_t>(cap - used), fp);
          if (n > 0) {
            used += static_cast<int64_t>(n);
          } else if (0 == n && feof(fp)) {
            eof = true;
          } else if (0 == n && ferror(fp)) {
            ret = OB_IO_ERROR;
          }
        }
      }
      if (OB_SUCC(ret)) {
        buf = data;
        len = used;
      }
    }
  }
  if (NULL != fp) {
    fclose(fp);
    fp = NULL;
  }
  return ret;
}

} // namespace file_sql
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_FILE_SQL_UTIL_INC_
