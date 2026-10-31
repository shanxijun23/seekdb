/*
 * File SQL: JSONL reader (flat objects only, union schema).
 */
#ifndef OCEANBASE_SQL_OB_FILE_SQL_JSON_INC_
#define OCEANBASE_SQL_OB_FILE_SQL_JSON_INC_

namespace oceanbase
{
namespace sql
{
namespace file_sql
{

enum FileJsonKind
{
  FILE_JSON_NULL = 0,
  FILE_JSON_STRING = 1,
  FILE_JSON_NUMBER = 2,
  FILE_JSON_BOOL = 3,
};

struct FileJsonField
{
  ObString key_;
  ObString value_;   // decoded string / raw number
  FileJsonKind kind_;
  bool bool_value_;
  FileJsonField() : key_(), value_(), kind_(FILE_JSON_NULL), bool_value_(false) {}
};

class FileJsonParser
{
public:
  FileJsonParser() : buf_(NULL), len_(0), pos_(0), alloc_(NULL) {}
  void init(const char *buf, const int64_t len, ObIAllocator &alloc)
  {
    buf_ = buf;
    len_ = len;
    pos_ = 0;
    alloc_ = &alloc;
  }

  int parse_object(ObIArray<FileJsonField> &fields)
  {
    int ret = OB_SUCCESS;
    skip_space();
    if (pos_ >= len_ || buf_[pos_] != '{') {
      ret = OB_INVALID_DATA;
    } else {
      ++pos_;
      skip_space();
      if (pos_ < len_ && buf_[pos_] == '}') {
        ++pos_;
      } else {
        bool done = false;
        while (OB_SUCC(ret) && !done) {
          FileJsonField field;
          if (OB_FAIL(parse_key(field.key_))) {
          } else if (OB_FAIL(parse_value(field))) {
          } else if (OB_FAIL(fields.push_back(field))) {
          } else {
            skip_space();
            if (pos_ < len_ && buf_[pos_] == ',') {
              ++pos_;
              skip_space();
            } else if (pos_ < len_ && buf_[pos_] == '}') {
              ++pos_;
              done = true;
            } else {
              ret = OB_INVALID_DATA;
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        skip_space();
      }
    }
    return ret;
  }

  // checks that only whitespace remains
  bool at_line_end() const
  {
    bool end = true;
    for (int64_t i = pos_; end && i < len_; ++i) {
      if (' ' != buf_[i] && '\t' != buf_[i] && '\r' != buf_[i] && '\n' != buf_[i]) {
        end = false;
      }
    }
    return end;
  }

private:
  void skip_space()
  {
    while (pos_ < len_ && (' ' == buf_[pos_] || '\t' == buf_[pos_] || '\r' == buf_[pos_]
                          || '\n' == buf_[pos_])) {
      ++pos_;
    }
  }

  int parse_key(ObString &key)
  {
    int ret = OB_SUCCESS;
    skip_space();
    if (pos_ >= len_ || buf_[pos_] != '"') {
      ret = OB_INVALID_DATA;
    } else if (OB_FAIL(parse_string(key))) {
    } else {
      skip_space();
      if (pos_ >= len_ || buf_[pos_] != ':') {
        ret = OB_INVALID_DATA;
      } else {
        ++pos_;
        skip_space();
      }
    }
    return ret;
  }

  int parse_value(FileJsonField &field)
  {
    int ret = OB_SUCCESS;
    if (pos_ >= len_) {
      ret = OB_INVALID_DATA;
    } else if ('"' == buf_[pos_]) {
      field.kind_ = FILE_JSON_STRING;
      ret = parse_string(field.value_);
    } else if ('{' == buf_[pos_] || '[' == buf_[pos_]) {
      // nested objects / arrays are not supported by file SQL
      ret = OB_NOT_SUPPORTED;
    } else if (0 == strncmp(buf_ + pos_, "true", MIN(len_ - pos_, 4)) && len_ - pos_ >= 4) {
      field.kind_ = FILE_JSON_BOOL;
      field.bool_value_ = true;
      pos_ += 4;
    } else if (0 == strncmp(buf_ + pos_, "false", MIN(len_ - pos_, 5)) && len_ - pos_ >= 5) {
      field.kind_ = FILE_JSON_BOOL;
      field.bool_value_ = false;
      pos_ += 5;
    } else if (0 == strncmp(buf_ + pos_, "null", MIN(len_ - pos_, 4)) && len_ - pos_ >= 4) {
      field.kind_ = FILE_JSON_NULL;
      pos_ += 4;
    } else if ('-' == buf_[pos_] || file_sql_is_digit(buf_[pos_])) {
      const int64_t begin = pos_;
      if ('-' == buf_[pos_]) {
        ++pos_;
      }
      bool valid = (pos_ < len_ && file_sql_is_digit(buf_[pos_]));
      while (valid && pos_ < len_ && file_sql_is_digit(buf_[pos_])) {
        ++pos_;
      }
      if (valid && pos_ < len_ && '.' == buf_[pos_]) {
        ++pos_;
        while (pos_ < len_ && file_sql_is_digit(buf_[pos_])) {
          ++pos_;
        }
      }
      if (valid && pos_ < len_ && ('e' == buf_[pos_] || 'E' == buf_[pos_])) {
        ++pos_;
        if (pos_ < len_ && ('+' == buf_[pos_] || '-' == buf_[pos_])) {
          ++pos_;
        }
        while (pos_ < len_ && file_sql_is_digit(buf_[pos_])) {
          ++pos_;
        }
      }
      if (!valid) {
        ret = OB_INVALID_DATA;
      } else {
        field.kind_ = FILE_JSON_NUMBER;
        field.value_.assign_ptr(buf_ + begin, static_cast<int32_t>(pos_ - begin));
      }
    } else {
      ret = OB_INVALID_DATA;
    }
    return ret;
  }

  int parse_string(ObString &value)
  {
    int ret = OB_SUCCESS;
    ObSqlString collected;
    bool closed = false;
    if (pos_ >= len_ || '"' != buf_[pos_]) {
      ret = OB_INVALID_DATA;
    } else {
      ++pos_;
      while (OB_SUCC(ret) && !closed) {
        if (pos_ >= len_) {
          ret = OB_INVALID_DATA;
        } else if ('"' == buf_[pos_]) {
          ++pos_;
          closed = true;
        } else if ('\\' == buf_[pos_]) {
          ++pos_;
          if (pos_ >= len_) {
            ret = OB_INVALID_DATA;
          } else {
            const char esc = buf_[pos_++];
            switch (esc) {
              case '"': ret = collected.append_fmt("%c", '"'); break;
              case '\\': ret = collected.append_fmt("%c", '\\'); break;
              case '/': ret = collected.append_fmt("%c", '/'); break;
              case 'n': ret = collected.append_fmt("%c", '\n'); break;
              case 't': ret = collected.append_fmt("%c", '\t'); break;
              case 'r': ret = collected.append_fmt("%c", '\r'); break;
              case 'b': ret = collected.append_fmt("%c", '\b'); break;
              case 'f': ret = collected.append_fmt("%c", '\f'); break;
              case 'u': {
                if (pos_ + 4 <= len_) {
                  int code = 0;
                  bool ok = true;
                  for (int64_t i = 0; i < 4; ++i) {
                    const char h = buf_[pos_ + i];
                    int v = 0;
                    if (h >= '0' && h <= '9') v = h - '0';
                    else if (h >= 'a' && h <= 'f') v = h - 'a' + 10;
                    else if (h >= 'A' && h <= 'F') v = h - 'A' + 10;
                    else ok = false;
                    code = code * 16 + v;
                  }
                  if (ok) {
                    pos_ += 4;
                    // encode UTF-8 (no surrogate handling)
                    if (code < 0x80) {
                      ret = collected.append_fmt("%c", static_cast<char>(code));
                    } else if (code < 0x800) {
                      ret = collected.append_fmt("%c", static_cast<char>(0xC0 | (code >> 6)));
                      if (OB_SUCC(ret)) {
                        ret = collected.append_fmt("%c", static_cast<char>(0x80 | (code & 0x3F)));
                      }
                    } else {
                      ret = collected.append_fmt("%c", static_cast<char>(0xE0 | (code >> 12)));
                      if (OB_SUCC(ret)) {
                        ret = collected.append_fmt("%c", static_cast<char>(0x80 | ((code >> 6) & 0x3F)));
                      }
                      if (OB_SUCC(ret)) {
                        ret = collected.append_fmt("%c", static_cast<char>(0x80 | (code & 0x3F)));
                      }
                    }
                  } else {
                    ret = OB_INVALID_DATA;
                  }
                } else {
                  ret = OB_INVALID_DATA;
                }
                break;
              }
              default: ret = OB_INVALID_DATA; break;
            }
          }
        } else {
          ret = collected.append_fmt("%c", buf_[pos_++]);
        }
      }
      if (OB_SUCC(ret) && collected.length() > 0) {
        char *copy = static_cast<char *>(alloc_->alloc(collected.length()));
        if (OB_ISNULL(copy)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          MEMCPY(copy, collected.ptr(), collected.length());
          value.assign_ptr(copy, static_cast<int32_t>(collected.length()));
        }
      } else if (OB_SUCC(ret)) {
        value.assign_ptr(NULL, 0);
      }
    }
    return ret;
  }

private:
  const char *buf_;
  int64_t len_;
  int64_t pos_;
  ObIAllocator *alloc_;
};

// iterate lines of a jsonl buffer
static int file_sql_next_line(const char *buf, const int64_t len, int64_t &pos,
                              const char *&line, int64_t &line_len, bool &eof)
{
  int ret = OB_SUCCESS;
  line = NULL;
  line_len = 0;
  eof = (pos >= len);
  if (!eof) {
    const int64_t begin = pos;
    while (pos < len && '\n' != buf[pos]) {
      ++pos;
    }
    int64_t end = pos;
    if (end > begin && '\r' == buf[end - 1]) {
      --end;
    }
    line = buf + begin;
    line_len = end - begin;
    if (pos < len && '\n' == buf[pos]) {
      ++pos;
    }
  }
  return ret;
}

class FileJsonSchemaInfer
{
public:
  FileJsonSchemaInfer(FileSchema &schema, ObIAllocator &alloc)
    : schema_(schema), alloc_(alloc), line_no_(0), bad_line_(0) {}
  int64_t bad_line() const { return bad_line_; }

  int on_object(ObIArray<FileJsonField> &fields)
  {
    int ret = OB_SUCCESS;
    ++line_no_;
    for (int64_t i = 0; OB_SUCC(ret) && i < fields.count(); ++i) {
      FileJsonField &f = fields.at(i);
      int64_t idx = -1;
      if (OB_FAIL(find_column(f.key_, idx))) {
      } else if (idx < 0) {
        FileColumn col;
        ObString stored;
        if (OB_FAIL(ob_write_string(alloc_, f.key_, stored))) {
        } else {
          col.name_ = stored;
          if (OB_FAIL(schema_.columns_.push_back(col))) {
          } else {
            idx = schema_.columns_.count() - 1;
          }
        }
      }
      if (OB_SUCC(ret)) {
        FileColumn &col = schema_.columns_.at(idx);
        FileColType type = FILE_COL_NULL;
        if (FILE_JSON_STRING == f.kind_) {
          type = file_sql_classify_json_string(f.value_);
          if (f.value_.length() > col.max_bytes_) {
            col.max_bytes_ = f.value_.length();
          }
        } else if (FILE_JSON_NUMBER == f.kind_) {
          int64_t iv = 0;
          type = file_sql_parse_int64(f.value_.ptr(), f.value_.length(), iv)
                 ? FILE_COL_BIGINT : FILE_COL_DOUBLE;
        } else if (FILE_JSON_BOOL == f.kind_) {
          type = FILE_COL_BOOLEAN;
        } else {
          type = FILE_COL_NULL;
          col.nullable_ = true;
        }
        col.type_ = file_sql_common_type(col.type_, type);
      }
    }
    // columns missing in this document are nullable
    if (OB_SUCC(ret)) {
      for (int64_t c = 0; c < schema_.columns_.count(); ++c) {
        bool found = false;
        for (int64_t i = 0; !found && i < fields.count(); ++i) {
          if (0 == schema_.columns_.at(c).name_.case_compare(fields.at(i).key_)) {
            found = true;
          }
        }
        if (!found) {
          schema_.columns_.at(c).nullable_ = true;
        }
      }
    }
    return ret;
  }

  static FileColType file_sql_classify_json_string(const ObString &value)
  {
    FileColType type = FILE_COL_VARCHAR;
    if (file_sql_is_date(value.ptr(), value.length())) {
      type = FILE_COL_DATE;
    } else if (file_sql_is_datetime(value.ptr(), value.length())) {
      type = FILE_COL_DATETIME;
    }
    return type;
  }

private:
  int find_column(const ObString &name, int64_t &idx)
  {
    int ret = OB_SUCCESS;
    idx = -1;
    for (int64_t i = 0; -1 == idx && i < schema_.columns_.count(); ++i) {
      if (0 == schema_.columns_.at(i).name_.case_compare(name)) {
        idx = i;
      }
    }
    return ret;
  }

private:
  FileSchema &schema_;
  ObIAllocator &alloc_;
  int64_t line_no_;
  int64_t bad_line_;
};

} // namespace file_sql
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_FILE_SQL_JSON_INC_
