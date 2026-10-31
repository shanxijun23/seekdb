/*
 * File SQL: CSV reader (schema inference + row decoding).
 */
#ifndef OCEANBASE_SQL_OB_FILE_SQL_CSV_INC_
#define OCEANBASE_SQL_OB_FILE_SQL_CSV_INC_

namespace oceanbase
{
namespace sql
{
namespace file_sql
{

struct FileCsvOptions
{
  bool header_;
  char delim_;
  char quote_;
  char escape_;
  bool has_escape_;
  ObString nullstr_;
  bool has_nullstr_;
  int64_t sample_size_;
  int64_t skip_rows_;
  FileCsvOptions()
    : header_(true), delim_(','), quote_('"'), escape_('\0'), has_escape_(false),
      nullstr_(), has_nullstr_(false), sample_size_(-1), skip_rows_(0) {}
};

struct FileRawRow
{
  ObSEArray<ObString, 32> fields_;
  int64_t line_no_;
  FileRawRow() : fields_(), line_no_(0) {}
  void reset() { fields_.reset(); line_no_ = 0; }
};

// Streaming parser over an in-memory buffer.  Handler must provide
//   int on_row(FileRawRow &row, bool &stop);
class FileCsvParser
{
public:
  FileCsvParser() : buf_(NULL), len_(0), pos_(0), line_(1), opts_() {}
  void init(const char *buf, const int64_t len, const FileCsvOptions &opts)
  {
    buf_ = buf;
    len_ = len;
    pos_ = 0;
    line_ = 1;
    opts_ = opts;
    if (len_ >= 3 && 0 == memcmp(buf_, "\xEF\xBB\xBF", 3)) {
      pos_ = 3;
    }
  }

  template <typename Handler>
  int parse(Handler &handler)
  {
    int ret = OB_SUCCESS;
    bool eof = false;
    int64_t skipped = 0;
    while (OB_SUCC(ret) && !eof) {
      FileRawRow row;
      bool stop = false;
      int parse_ret = parse_row(row, eof);
      if (OB_FAIL(parse_ret)) {
        ret = parse_ret;
      } else if (row.fields_.count() == 0 && eof) {
        // trailing empty line
      } else if (skipped < opts_.skip_rows_) {
        ++skipped;
      } else if (row.fields_.count() == 1 && row.fields_.at(0).length() <= 0) {
        // blank line
      } else if (OB_FAIL(handler.on_row(row, stop))) {
      }
      if (stop) {
        break;
      }
    }
    return ret;
  }

private:
  int parse_row(FileRawRow &row, bool &eof)
  {
    int ret = OB_SUCCESS;
    eof = false;
    bool row_done = false;
    bool first_field = true;
    const int64_t start_line = line_;
    while (OB_SUCC(ret) && !row_done) {
      if (pos_ >= len_) {
        eof = true;
        row_done = true;
        if (!first_field) {
          // last field without delimiter, keep it
        }
        break;
      }
      // skip field separator at start of subsequent fields
      if (!first_field) {
        if (pos_ < len_ && buf_[pos_] == opts_.delim_) {
          ++pos_;
        } else {
          row_done = true;
          break;
        }
      }
      first_field = false;
      ObString value;
      bool line_done = false;
      int parse_ret = parse_field(value, eof, line_done);
      if (OB_FAIL(parse_ret)) {
        ret = parse_ret;
      } else if (OB_FAIL(row.fields_.push_back(value))) {
      }
      if (OB_SUCC(ret) && (eof || line_done)) {
        row_done = true;
      }
    }
    if (OB_SUCC(ret)) {
      row.line_no_ = start_line;
    }
    return ret;
  }

  int parse_field(ObString &value, bool &eof, bool &line_done)
  {
    int ret = OB_SUCCESS;
    eof = false;
    line_done = false;
    if (pos_ >= len_) {
      value.assign_ptr(NULL, 0);
      eof = true;
    } else if (buf_[pos_] == opts_.quote_) {
      // quoted field
      ++pos_;
      const int64_t begin = pos_;
      bool closed = false;
      ObSqlString collected;
      int64_t seg_begin = pos_;
      while (OB_SUCC(ret) && !closed) {
        if (pos_ >= len_) {
          // unterminated quote: accept rest
          if (OB_FAIL(collected.append(buf_ + seg_begin, static_cast<int32_t>(pos_ - seg_begin)))) {
          } else {
            closed = true;
            eof = true;
          }
        } else {
          const char c = buf_[pos_];
          if (opts_.has_escape_ && c == opts_.escape_ && opts_.escape_ != opts_.quote_) {
            if (OB_FAIL(collected.append(buf_ + seg_begin, static_cast<int32_t>(pos_ - seg_begin)))) {
            } else if (pos_ + 1 < len_) {
              if (OB_FAIL(collected.append_fmt("%c", buf_[pos_ + 1]))) {
              } else {
                pos_ += 2;
                seg_begin = pos_;
              }
            } else {
              ++pos_;
            }
          } else if (c == opts_.quote_) {
            if (pos_ + 1 < len_ && buf_[pos_ + 1] == opts_.quote_) {
              if (OB_FAIL(collected.append(buf_ + seg_begin, static_cast<int32_t>(pos_ - seg_begin)))) {
              } else if (OB_FAIL(collected.append_fmt("%c", opts_.quote_))) {
              } else {
                pos_ += 2;
                seg_begin = pos_;
              }
            } else {
              if (OB_FAIL(collected.append(buf_ + seg_begin, static_cast<int32_t>(pos_ - seg_begin)))) {
              } else {
                ++pos_;
                closed = true;
              }
            }
          } else {
            if ('\n' == c) {
              ++line_;
            }
            ++pos_;
          }
        }
      }
      if (OB_SUCC(ret) && collected.length() > 0) {
        char *copy = static_cast<char *>(parser_alloc_.alloc(collected.length()));
        if (OB_ISNULL(copy)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          MEMCPY(copy, collected.ptr(), collected.length());
          value.assign_ptr(copy, static_cast<int32_t>(collected.length()));
        }
      } else if (OB_SUCC(ret)) {
        value.assign_ptr(NULL, 0);
      }
      // consume trailing CR/LF outside quotes
      consume_line_end(eof, line_done);
    } else {
      // unquoted field: raw until delimiter / newline
      const int64_t begin = pos_;
      while (pos_ < len_ && buf_[pos_] != opts_.delim_ && buf_[pos_] != '\n'
             && buf_[pos_] != '\r') {
        ++pos_;
      }
      value.assign_ptr(buf_ + begin, static_cast<int32_t>(pos_ - begin));
      consume_line_end(eof, line_done);
    }
    if (OB_SUCC(ret) && !eof && pos_ >= len_) {
      eof = true;
    }
    return ret;
  }

  void consume_line_end(bool &eof, bool &line_done)
  {
    if (pos_ < len_ && '\r' == buf_[pos_]) {
      ++pos_;
    }
    if (pos_ < len_ && '\n' == buf_[pos_]) {
      ++line_;
      ++pos_;
      line_done = true;
    }
    if (pos_ >= len_) {
      eof = true;
    }
  }

private:
  const char *buf_;
  int64_t len_;
  int64_t pos_;
  int64_t line_;
  FileCsvOptions opts_;
  ObArenaAllocator parser_alloc_;
};

// Inference pass handler
class FileCsvSchemaInfer
{
public:
  FileCsvSchemaInfer(FileSchema &schema, ObIAllocator &allocator)
    : schema_(schema), allocator_(allocator), header_done_(false), opts_(NULL), has_header_(false) {}
  void set_options(const FileCsvOptions *opts) { opts_ = opts; }

  int on_row(FileRawRow &row, bool &stop)
  {
    int ret = OB_SUCCESS;
    stop = false;
    if (!header_done_) {
      header_done_ = true;
      has_header_ = (NULL != opts_ && opts_->header_);
      if (has_header_) {
        if (OB_FAIL(build_columns_from_header(row))) {
        }
      } else {
        if (OB_FAIL(build_columns_from_first_row(row))) {
        }
      }
      if (OB_SUCC(ret) && has_header_) {
        // header row is not data; continue to next row
      } else if (OB_SUCC(ret)) {
        if (OB_FAIL(infer_row(row))) {
        }
      }
    } else {
      if (OB_FAIL(infer_row(row))) {
      }
    }
    return ret;
  }

  bool has_header() const { return has_header_; }
  int64_t column_count() const { return schema_.columns_.count(); }

private:
  int add_column(const ObString &name)
  {
    int ret = OB_SUCCESS;
    ObString stored;
    FileColumn col;
    if (OB_FAIL(ob_write_string(allocator_, name, stored))) {
    } else {
      // de-duplicate header names: append __2, __3 ...
      ObString final_name;
      char suffix_buf[32];
      int64_t dup = 1;
      ObSqlString candidate;
      if (OB_FAIL(candidate.append_fmt("%.*s", stored.length(), stored.ptr()))) {
      }
      while (OB_SUCC(ret)) {
        bool exists = false;
        for (int64_t i = 0; !exists && i < schema_.columns_.count(); ++i) {
          if (0 == schema_.columns_.at(i).name_.case_compare(candidate.string())) {
            exists = true;
          }
        }
        if (!exists) {
          break;
        }
        ++dup;
        if (snprintf(suffix_buf, sizeof(suffix_buf), "__%ld", dup) <= 0) {
          ret = OB_ERR_UNEXPECTED;
        } else if (FALSE_IT(candidate.assign(stored)) || OB_FAIL(candidate.append(suffix_buf))) {
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator_, candidate.string(), final_name))) {
      } else if (OB_SUCC(ret)) {
        col.name_ = final_name;
        if (OB_FAIL(schema_.columns_.push_back(col))) {
        }
      }
    }
    return ret;
  }

  int build_columns_from_header(FileRawRow &row)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < row.fields_.count(); ++i) {
      const ObString &name = row.fields_.at(i);
      char generated[32];
      if (name.length() <= 0) {
        snprintf(generated, sizeof(generated), "column%ld", static_cast<long>(i));
        if (OB_FAIL(add_column(ObString(static_cast<int32_t>(strlen(generated)), generated)))) {
        }
      } else if (OB_FAIL(add_column(name))) {
      }
    }
    return ret;
  }

  int build_columns_from_first_row(FileRawRow &row)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < row.fields_.count(); ++i) {
      char generated[32];
      snprintf(generated, sizeof(generated), "column%ld", static_cast<long>(i));
      if (OB_FAIL(add_column(ObString(static_cast<int32_t>(strlen(generated)), generated)))) {
      }
    }
    return ret;
  }

  int infer_row(FileRawRow &row)
  {
    int ret = OB_SUCCESS;
    if (row.fields_.count() != schema_.columns_.count()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("file sql csv column count mismatch", K(ret), K(row.line_no_),
               "expected", schema_.columns_.count(), "actual", row.fields_.count());
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row.fields_.count(); ++i) {
      FileColumn &col = schema_.columns_.at(i);
      const ObString &field = row.fields_.at(i);
      if (field.length() <= 0) {
        col.nullable_ = true;
      } else {
        const FileColType type = file_sql_classify_text(field.ptr(), field.length());
        col.type_ = file_sql_common_type(col.type_, type);
        if (field.length() > col.max_bytes_) {
          col.max_bytes_ = field.length();
        }
      }
    }
    return ret;
  }

private:
  FileSchema &schema_;
  ObIAllocator &allocator_;
  bool header_done_;
  const FileCsvOptions *opts_;
  bool has_header_;
};

} // namespace file_sql
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_FILE_SQL_CSV_INC_
