/*
 * File SQL: Parquet reader (flat schema, v1 data pages, PLAIN/dictionary
 * encodings, uncompressed/snappy compression).
 */
#ifndef OCEANBASE_SQL_OB_FILE_SQL_PARQUET_INC_
#define OCEANBASE_SQL_OB_FILE_SQL_PARQUET_INC_

namespace oceanbase
{
namespace sql
{
namespace file_sql
{

// ---------------------------------------------------------------------------
// thrift compact protocol reader
class FileThriftReader
{
public:
  FileThriftReader() : buf_(NULL), len_(0), pos_(0), err_(false) {}
  void init(const char *buf, const int64_t len, const int64_t pos)
  {
    buf_ = buf;
    len_ = len;
    pos_ = pos;
    err_ = false;
  }
  bool ok() const { return !err_; }
  bool eof() const { return pos_ >= len_; }
  int64_t pos() const { return pos_; }

  uint8_t read_byte()
  {
    uint8_t b = 0;
    if (pos_ >= len_) {
      err_ = true;
    } else {
      b = static_cast<uint8_t>(buf_[pos_++]);
    }
    return b;
  }
  uint64_t read_varint()
  {
    uint64_t value = 0;
    int shift = 0;
    bool done = false;
    while (!done && !err_) {
      const uint8_t b = read_byte();
      value |= static_cast<uint64_t>(b & 0x7F) << shift;
      shift += 7;
      if (0 == (b & 0x80)) {
        done = true;
      } else if (shift > 63) {
        err_ = true;
      }
    }
    return value;
  }
  int64_t read_zigzag()
  {
    const uint64_t v = read_varint();
    return static_cast<int64_t>((v >> 1) ^ (~(v & 1) + 1));
  }
  int32_t read_i32() { return static_cast<int32_t>(read_zigzag()); }
  int64_t read_i64() { return read_zigzag(); }
  double read_double()
  {
    double v = 0;
    if (pos_ + 8 <= len_) {
      MEMCPY(&v, buf_ + pos_, 8);
      pos_ += 8;
    } else {
      err_ = true;
    }
    return v;
  }
  ObString read_binary(ObIAllocator &alloc)
  {
    ObString value;
    const int64_t n = static_cast<int64_t>(read_varint());
    if (!err_ && n >= 0 && pos_ + n <= len_) {
      char *copy = static_cast<char *>(alloc.alloc(n + 1));
      if (NULL == copy) {
        err_ = true;
      } else {
        MEMCPY(copy, buf_ + pos_, n);
        copy[n] = '\0';
        pos_ += n;
        value.assign_ptr(copy, static_cast<int32_t>(n));
      }
    } else {
      err_ = true;
    }
    return value;
  }
  uint8_t read_list_header(int64_t &size)
  {
    const uint8_t b = read_byte();
    const uint8_t elem_type = b & 0x0F;
    size = (b >> 4) & 0x0F;
    if (15 == size) {
      size = static_cast<int64_t>(read_varint());
    }
    return elem_type;
  }
  void skip_struct();
  void skip(uint8_t type)
  {
    switch (type) {
      case 1:
      case 2:
        break;  // boolean value encoded in field header
      case 3:
        read_byte();
        break;
      case 4:
      case 5:
      case 6:
        read_zigzag();
        break;
      case 7:
        read_double();
        break;
      case 8: {
        const int64_t n = static_cast<int64_t>(read_varint());
        if (n < 0 || pos_ + n > len_) {
          err_ = true;
        } else {
          pos_ += n;
        }
        break;
      }
      case 9: {
        int64_t size = 0;
        const uint8_t elem = read_list_header(size);
        for (int64_t i = 0; !err_ && i < size; ++i) {
          if (1 == elem || 2 == elem) {
            read_byte();
          } else {
            skip(elem);
          }
        }
        break;
      }
      case 10:
      case 11: {
        int64_t size = 0;
        const uint8_t elem = read_list_header(size);
        for (int64_t i = 0; !err_ && i < size; ++i) {
          if (1 == elem || 2 == elem) {
            read_byte();
          } else {
            skip(elem);
          }
        }
        break;
      }
      case 12:
        skip_struct();
        break;
      default:
        err_ = true;
        break;
    }
  }

private:
  const char *buf_;
  int64_t len_;
  int64_t pos_;
  bool err_;
};

inline void FileThriftReader::skip_struct()
{
  int64_t last_field = 0;
  while (!err_) {
    const uint8_t b = read_byte();
    if (0 == b) {
      break;
    }
    const uint8_t delta = (b >> 4) & 0x0F;
    const uint8_t type = b & 0x0F;
    if (delta > 0) {
      last_field += delta;
    } else {
      last_field = read_i32();
    }
    skip(type);
  }
}

// ---------------------------------------------------------------------------
// parquet metadata
struct FileParquetSchemaElement
{
  int32_t type_;
  int32_t type_length_;
  int32_t repetition_type_;
  ObString name_;
  int32_t num_children_;
  int32_t converted_type_;
  int32_t logical_string_;
  int32_t logical_date_;
  int32_t logical_timestamp_;
  FileParquetSchemaElement()
    : type_(-1), type_length_(0), repetition_type_(0), name_(), num_children_(0),
      converted_type_(-1), logical_string_(0), logical_date_(0), logical_timestamp_(0) {}
};

struct FileParquetColumnMeta
{
  int32_t type_;
  int32_t codec_;
  int64_t num_values_;
  int64_t data_page_offset_;
  int64_t dictionary_page_offset_;
  int64_t total_compressed_size_;
  ObString path_[8];
  int32_t path_depth_;
  FileParquetColumnMeta()
    : type_(-1), codec_(0), num_values_(0), data_page_offset_(-1),
      dictionary_page_offset_(-1), total_compressed_size_(0), path_(), path_depth_(0) {}
};

struct FileParquetRowGroup
{
  ObSEArray<FileParquetColumnMeta, 16> columns_;
  int64_t num_rows_;
  FileParquetRowGroup() : columns_(), num_rows_(0) {}
};

struct FileParquetMeta
{
  ObSEArray<FileParquetSchemaElement, 32> schema_;
  ObSEArray<FileParquetRowGroup, 8> row_groups_;
  int64_t num_rows_;
  FileParquetMeta() : schema_(), row_groups_(), num_rows_(0) {}
};

static int file_parquet_skip_logical_type(FileThriftReader &reader, int32_t &logical_kind)
{
  int ret = OB_SUCCESS;
  int64_t last_field = 0;
  logical_kind = 0;
  while (OB_SUCC(ret) && reader.ok()) {
    const uint8_t b = reader.read_byte();
    if (0 == b) {
      break;
    }
    const uint8_t delta = (b >> 4) & 0x0F;
    const uint8_t type = b & 0x0F;
    int64_t field_id = 0;
    if (delta > 0) {
      field_id = last_field + delta;
    } else {
      field_id = reader.read_i32();
    }
    last_field = field_id;
    if (1 == field_id) {
      logical_kind = 1;  // string
    } else if (6 == field_id) {
      logical_kind = 2;  // date
    } else if (8 == field_id) {
      logical_kind = 3;  // timestamp
    }
    reader.skip(type);
  }
  if (!reader.ok()) {
    ret = OB_INVALID_DATA;
  }
  return ret;
}

static int file_parquet_parse_schema_element(FileThriftReader &reader,
                                             FileParquetSchemaElement &element,
                                             ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  int64_t last_field = 0;
  int64_t field_id = 0;
  while (OB_SUCC(ret) && reader.ok()) {
    const uint8_t b = reader.read_byte();
    if (0 == b) {
      break;
    }
    const uint8_t delta = (b >> 4) & 0x0F;
    const uint8_t type = b & 0x0F;
    if (delta > 0) {
      field_id = last_field + delta;
    } else {
      field_id = reader.read_i32();
    }
    last_field = field_id;
    switch (field_id) {
      case 1: element.type_ = reader.read_i32(); break;
      case 2: element.type_length_ = reader.read_i32(); break;
      case 3: element.repetition_type_ = reader.read_i32(); break;
      case 4: element.name_ = reader.read_binary(alloc); break;
      case 5: element.num_children_ = reader.read_i32(); break;
      case 6: element.converted_type_ = reader.read_i32(); break;
      case 7: reader.read_i32(); break;
      case 8: reader.read_i32(); break;
      case 9: reader.read_i32(); break;
      case 10: {
        int32_t logical_kind = 0;
        if (OB_FAIL(file_parquet_skip_logical_type(reader, logical_kind))) {
        } else if (1 == logical_kind) {
          element.logical_string_ = 1;
        } else if (2 == logical_kind) {
          element.logical_date_ = 1;
        } else if (3 == logical_kind) {
          element.logical_timestamp_ = 1;
        }
        break;
      }
      default:
        reader.skip(type);
        break;
    }
    if (!reader.ok()) {
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

static int file_parquet_parse_column_meta(FileThriftReader &reader,
                                          FileParquetColumnMeta &column,
                                          ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  int64_t last_field = 0;
  int64_t field_id = 0;
  while (OB_SUCC(ret) && reader.ok()) {
    const uint8_t b = reader.read_byte();
    if (0 == b) {
      break;
    }
    const uint8_t delta = (b >> 4) & 0x0F;
    const uint8_t type = b & 0x0F;
    if (delta > 0) {
      field_id = last_field + delta;
    } else {
      field_id = reader.read_i32();
    }
    last_field = field_id;
    switch (field_id) {
      case 1: column.type_ = reader.read_i32(); break;
      case 2: {
        int64_t n = 0;
        const uint8_t elem = reader.read_list_header(n);
        for (int64_t i = 0; i < n && reader.ok(); ++i) {
          if (5 == elem) {
            reader.read_i32();
          } else {
            reader.skip(elem);
          }
        }
        break;
      }
      case 3: {
        int64_t n = 0;
        reader.read_list_header(n);
        for (int64_t i = 0; i < n && reader.ok(); ++i) {
          ObString path = reader.read_binary(alloc);
          if (column.path_depth_ < 8) {
            column.path_[column.path_depth_++] = path;
          }
        }
        break;
      }
      case 4: column.codec_ = reader.read_i32(); break;
      case 5: column.num_values_ = reader.read_i64(); break;
      case 6: reader.read_i64(); break;
      case 7: column.total_compressed_size_ = reader.read_i64(); break;
      case 8: reader.skip(type); break;
      case 9: column.data_page_offset_ = reader.read_i64(); break;
      case 10: reader.read_i64(); break;
      case 11: column.dictionary_page_offset_ = reader.read_i64(); break;
      default: reader.skip(type); break;
    }
    if (!reader.ok()) {
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

static int file_parquet_parse_row_group(FileThriftReader &reader,
                                        FileParquetRowGroup &row_group,
                                        ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  int64_t last_field = 0;
  int64_t field_id = 0;
  while (OB_SUCC(ret) && reader.ok()) {
    const uint8_t b = reader.read_byte();
    if (0 == b) {
      break;
    }
    const uint8_t delta = (b >> 4) & 0x0F;
    const uint8_t type = b & 0x0F;
    if (delta > 0) {
      field_id = last_field + delta;
    } else {
      field_id = reader.read_i32();
    }
    last_field = field_id;
    if (1 == field_id && 9 == type) {
      int64_t count = 0;
      const uint8_t elem = reader.read_list_header(count);
      if (12 != elem) {
        ret = OB_INVALID_DATA;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        FileParquetColumnMeta column;
        int64_t clf = 0;
        int64_t cfid = 0;
        while (OB_SUCC(ret) && reader.ok()) {
          const uint8_t cb = reader.read_byte();
          if (0 == cb) {
            break;
          }
          const uint8_t cdelta = (cb >> 4) & 0x0F;
          const uint8_t ct = cb & 0x0F;
          if (cdelta > 0) {
            cfid = clf + cdelta;
          } else {
            cfid = reader.read_i32();
          }
          clf = cfid;
          if (3 == cfid && 12 == ct) {
            if (OB_FAIL(file_parquet_parse_column_meta(reader, column, alloc))) {
            }
          } else {
            reader.skip(ct);
          }
          if (!reader.ok()) {
            ret = OB_INVALID_DATA;
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(row_group.columns_.push_back(column))) {
        }
      }
    } else if (3 == field_id && 6 == type) {
      row_group.num_rows_ = reader.read_i64();
    } else {
      reader.skip(type);
    }
    if (!reader.ok()) {
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

static int file_parquet_parse_metadata(FileThriftReader &reader, FileParquetMeta &meta,
                                       ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  int64_t last_field = 0;
  int64_t field_id = 0;
  while (OB_SUCC(ret) && !reader.eof()) {
    const uint8_t b = reader.read_byte();
    if (0 == b) {
      break;
    }
    const uint8_t delta = (b >> 4) & 0x0F;
    const uint8_t type = b & 0x0F;
    if (delta > 0) {
      field_id = last_field + delta;
    } else {
      field_id = reader.read_i32();
    }
    last_field = field_id;
    if (2 == field_id && 9 == type) {
      int64_t count = 0;
      const uint8_t elem = reader.read_list_header(count);
      if (12 != elem) {
        ret = OB_INVALID_DATA;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        FileParquetSchemaElement element;
        if (OB_FAIL(file_parquet_parse_schema_element(reader, element, alloc))) {
        } else if (OB_FAIL(meta.schema_.push_back(element))) {
        }
      }
    } else if (3 == field_id && 6 == type) {
      meta.num_rows_ = reader.read_i64();
    } else if (4 == field_id && 9 == type) {
      int64_t count = 0;
      const uint8_t elem = reader.read_list_header(count);
      if (12 != elem) {
        ret = OB_INVALID_DATA;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        FileParquetRowGroup row_group;
        if (OB_FAIL(file_parquet_parse_row_group(reader, row_group, alloc))) {
        } else if (OB_FAIL(meta.row_groups_.push_back(row_group))) {
        }
      }
    } else {
      reader.skip(type);
    }
    if (!reader.ok()) {
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// snappy raw block decompression
static int file_parquet_snappy_uncompress(const char *input, const int64_t input_len,
                                          char *output, const int64_t output_cap,
                                          int64_t &output_len)
{
  int ret = OB_SUCCESS;
  int64_t ip = 0;
  int64_t op = 0;
  output_len = 0;
  uint64_t uncompressed = 0;
  int shift = 0;
  bool done = false;
  while (!done && ip < input_len) {
    const uint8_t b = static_cast<uint8_t>(input[ip++]);
    uncompressed |= static_cast<uint64_t>(b & 0x7F) << shift;
    shift += 7;
    if (0 == (b & 0x80)) {
      done = true;
    }
  }
  if (!done || uncompressed > static_cast<uint64_t>(output_cap)) {
    ret = OB_INVALID_DATA;
  }
  while (OB_SUCC(ret) && ip < input_len && op < static_cast<int64_t>(uncompressed)) {
    const uint8_t tag = static_cast<uint8_t>(input[ip++]);
    const int type = tag & 0x03;
    if (0 == type) {
      int64_t len = (tag >> 2) + 1;
      if (len > 60) {
        const int extra = static_cast<int>(len - 60);
        len = 0;
        for (int i = 0; i < extra && ip < input_len; ++i) {
          len |= static_cast<int64_t>(static_cast<uint8_t>(input[ip++])) << (8 * i);
        }
        len += 1;
      }
      if (ip + len > input_len || op + len > static_cast<int64_t>(uncompressed)) {
        ret = OB_INVALID_DATA;
      } else {
        MEMCPY(output + op, input + ip, len);
        ip += len;
        op += len;
      }
    } else {
      int64_t len = 0;
      int64_t offset = 0;
      if (1 == type) {
        len = ((tag >> 2) & 0x07) + 4;
        offset = ((tag >> 5) << 8) | static_cast<uint8_t>(input[ip++]);
      } else if (2 == type) {
        len = (tag >> 2) + 1;
        offset = static_cast<uint8_t>(input[ip]) | (static_cast<uint8_t>(input[ip + 1]) << 8);
        ip += 2;
      } else {
        len = (tag >> 2) + 1;
        offset = static_cast<uint8_t>(input[ip]) | (static_cast<uint8_t>(input[ip + 1]) << 8)
               | (static_cast<uint8_t>(input[ip + 2]) << 16)
               | (static_cast<uint8_t>(input[ip + 3]) << 24);
        ip += 4;
      }
      if (offset <= 0 || offset > op || ip > input_len
          || op + len > static_cast<int64_t>(uncompressed)) {
        ret = OB_INVALID_DATA;
      } else {
        for (int64_t k = 0; k < len && OB_SUCC(ret); ++k) {
          output[op + k] = output[op - offset + k];
        }
        op += len;
      }
    }
  }
  if (OB_SUCC(ret) && op != static_cast<int64_t>(uncompressed)) {
    ret = OB_INVALID_DATA;
  }
  if (OB_SUCC(ret)) {
    output_len = op;
  }
  return ret;
}

// ---------------------------------------------------------------------------
// RLE/bit-packed hybrid decoder: decode `count` values into out
static int file_rle_decode(const char *buf, const int64_t len, const int32_t bit_width,
                           const int64_t count, ObIArray<int32_t> &out)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t produced = 0;
  if (bit_width <= 0) {
    for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
      if (OB_FAIL(out.push_back(0))) {
      }
    }
  }
  while (OB_SUCC(ret) && produced < count && pos < len) {
    uint64_t header = 0;
    int shift = 0;
    bool done = false;
    while (!done && pos < len) {
      const uint8_t b = static_cast<uint8_t>(buf[pos++]);
      header |= static_cast<uint64_t>(b & 0x7F) << shift;
      shift += 7;
      if (0 == (b & 0x80)) {
        done = true;
      }
    }
    if (!done) {
      ret = OB_INVALID_DATA;
    } else if (0 == (header & 1)) {
      // RLE run
      const int64_t run = static_cast<int64_t>(header >> 1);
      const int64_t width = (bit_width + 7) / 8;
      uint64_t v = 0;
      for (int64_t i = 0; i < width && pos < len; ++i) {
        v |= static_cast<uint64_t>(static_cast<uint8_t>(buf[pos++])) << (8 * i);
      }
      for (int64_t i = 0; i < run && produced < count && OB_SUCC(ret); ++i) {
        if (OB_FAIL(out.push_back(static_cast<int32_t>(v)))) {
        } else {
          ++produced;
        }
      }
    } else {
      // bit-packed groups of 8
      const int64_t groups = static_cast<int64_t>(header >> 1);
      const int64_t nvals = groups * 8;
      const int64_t bytes = (nvals * bit_width + 7) / 8;
      if (pos + bytes > len) {
        ret = OB_INVALID_DATA;
      } else {
        int64_t bit_pos = 0;
        for (int64_t i = 0; i < nvals && produced < count && OB_SUCC(ret); ++i) {
          uint32_t v = 0;
          for (int32_t bit = 0; bit < bit_width; ++bit) {
            const int64_t byte_pos = pos + bit_pos / 8;
            const int bit_in_byte = static_cast<int>(bit_pos % 8);
            if (((static_cast<uint8_t>(buf[byte_pos]) >> bit_in_byte) & 1)) {
              v |= (1u << bit);
            }
            ++bit_pos;
          }
          if (OB_FAIL(out.push_back(static_cast<int32_t>(v)))) {
          } else {
            ++produced;
          }
        }
        pos += bytes;
      }
    }
  }
  if (OB_SUCC(ret) && produced < count) {
    ret = OB_INVALID_DATA;
  }
  return ret;
}

// ---------------------------------------------------------------------------
// page decoding helpers
struct FileParquetPageHeader
{
  int32_t type_;
  int32_t uncompressed_size_;
  int32_t compressed_size_;
  int32_t num_values_;
  int32_t encoding_;
  int32_t def_level_encoding_;
  int32_t rep_level_encoding_;
  int32_t dict_num_values_;
  int32_t dict_encoding_;
  int32_t num_nulls_;
  int32_t num_rows_;
  int32_t def_bytes_;
  int32_t rep_bytes_;
  int32_t v2_is_compressed_;
  FileParquetPageHeader()
    : type_(-1), uncompressed_size_(0), compressed_size_(0), num_values_(0), encoding_(0),
      def_level_encoding_(0), rep_level_encoding_(0), dict_num_values_(0), dict_encoding_(0),
      num_nulls_(0), num_rows_(0), def_bytes_(0), rep_bytes_(0), v2_is_compressed_(1) {}
};

static int file_parquet_parse_data_page_header(FileThriftReader &reader, FileParquetPageHeader &h)
{
  int ret = OB_SUCCESS;
  int64_t last_field = 0;
  int64_t field_id = 0;
  while (OB_SUCC(ret) && reader.ok()) {
    const uint8_t b = reader.read_byte();
    if (0 == b) {
      break;
    }
    const uint8_t delta = (b >> 4) & 0x0F;
    const uint8_t type = b & 0x0F;
    if (delta > 0) {
      field_id = last_field + delta;
    } else {
      field_id = reader.read_i32();
    }
    last_field = field_id;
    switch (field_id) {
      case 1: h.num_values_ = reader.read_i32(); break;
      case 2: h.encoding_ = reader.read_i32(); break;
      case 3: h.def_level_encoding_ = reader.read_i32(); break;
      case 4: h.rep_level_encoding_ = reader.read_i32(); break;
      default: reader.skip(type); break;
    }
    if (!reader.ok()) {
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

static int file_parquet_parse_dict_page_header(FileThriftReader &reader, FileParquetPageHeader &h)
{
  int ret = OB_SUCCESS;
  int64_t last_field = 0;
  int64_t field_id = 0;
  while (OB_SUCC(ret) && reader.ok()) {
    const uint8_t b = reader.read_byte();
    if (0 == b) {
      break;
    }
    const uint8_t delta = (b >> 4) & 0x0F;
    const uint8_t type = b & 0x0F;
    if (delta > 0) {
      field_id = last_field + delta;
    } else {
      field_id = reader.read_i32();
    }
    last_field = field_id;
    switch (field_id) {
      case 1: h.dict_num_values_ = reader.read_i32(); break;
      case 2: h.dict_encoding_ = reader.read_i32(); break;
      default: reader.skip(type); break;
    }
    if (!reader.ok()) {
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

static int file_parquet_parse_v2_header(FileThriftReader &reader, FileParquetPageHeader &h)
{
  int ret = OB_SUCCESS;
  int64_t last_field = 0;
  int64_t field_id = 0;
  while (OB_SUCC(ret) && reader.ok()) {
    const uint8_t b = reader.read_byte();
    if (0 == b) {
      break;
    }
    const uint8_t delta = (b >> 4) & 0x0F;
    const uint8_t type = b & 0x0F;
    if (delta > 0) {
      field_id = last_field + delta;
    } else {
      field_id = reader.read_i32();
    }
    last_field = field_id;
    switch (field_id) {
      case 1: h.num_values_ = reader.read_i32(); break;
      case 2: h.num_nulls_ = reader.read_i32(); break;
      case 3: h.num_rows_ = reader.read_i32(); break;
      case 4: h.encoding_ = reader.read_i32(); break;
      case 5: h.def_bytes_ = reader.read_i32(); break;
      case 6: h.rep_bytes_ = reader.read_i32(); break;
      case 7:
        if (1 == type) {
          h.v2_is_compressed_ = 1;
        } else if (2 == type) {
          h.v2_is_compressed_ = 0;
        } else {
          h.v2_is_compressed_ = reader.read_i32() ? 1 : 0;
        }
        break;
      default: reader.skip(type); break;
    }
    if (!reader.ok()) {
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

static int file_parquet_parse_page_header(FileThriftReader &reader, FileParquetPageHeader &h)
{
  int ret = OB_SUCCESS;
  int64_t last_field = 0;
  int64_t field_id = 0;
  while (OB_SUCC(ret) && reader.ok()) {
    const uint8_t b = reader.read_byte();
    if (0 == b) {
      break;
    }
    const uint8_t delta = (b >> 4) & 0x0F;
    const uint8_t type = b & 0x0F;
    if (delta > 0) {
      field_id = last_field + delta;
    } else {
      field_id = reader.read_i32();
    }
    last_field = field_id;
    switch (field_id) {
      case 1: h.type_ = reader.read_i32(); break;
      case 2: h.uncompressed_size_ = reader.read_i32(); break;
      case 3: h.compressed_size_ = reader.read_i32(); break;
      case 4: reader.read_i32(); break;
      case 5:
        if (12 == type && OB_FAIL(file_parquet_parse_data_page_header(reader, h))) {
        }
        break;
      case 7:
        if (12 == type && OB_FAIL(file_parquet_parse_dict_page_header(reader, h))) {
        }
        break;
      case 8:
        if (12 == type && OB_FAIL(file_parquet_parse_v2_header(reader, h))) {
        }
        break;
      default: reader.skip(type); break;
    }
    if (!reader.ok()) {
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

// map parquet schema element to file column type
static FileColType file_parquet_column_type(const FileParquetSchemaElement &element)
{
  FileColType type = FILE_COL_VARCHAR;
  const int32_t converted = element.converted_type_;
  const bool is_date = (6 == converted || element.logical_date_);
  const bool is_ts = (9 == converted || 10 == converted || element.logical_timestamp_);
  const bool is_string = (0 == converted || element.logical_string_);
  switch (element.type_) {
    case 0:  // BOOLEAN
      type = FILE_COL_BOOLEAN;
      break;
    case 1:  // INT32
      type = is_date ? FILE_COL_DATE : FILE_COL_BIGINT;
      break;
    case 2:  // INT64
      type = is_ts ? FILE_COL_DATETIME : FILE_COL_BIGINT;
      break;
    case 3:  // INT96
      type = FILE_COL_DATETIME;
      break;
    case 4:  // FLOAT
    case 5:  // DOUBLE
      type = FILE_COL_DOUBLE;
      break;
    case 6:  // BYTE_ARRAY
      type = is_string ? FILE_COL_VARCHAR : FILE_COL_VARCHAR;
      break;
    case 7:  // FIXED_LEN_BYTE_ARRAY
      type = FILE_COL_VARCHAR;
      break;
    default:
      type = FILE_COL_VARCHAR;
      break;
  }
  return type;
}

// date/datetime formatting helpers
static void file_parquet_format_date(const int64_t days, char *buf, const int64_t cap)
{
  // civil_from_days algorithm
  int64_t z = days + 719468;
  const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
  const int64_t doe = z - era * 146097;
  const int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  int64_t y = yoe + era * 400;
  const int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  const int64_t mp = (5 * doy + 2) / 153;
  const int64_t d = doy - (153 * mp + 2) / 5 + 1;
  const int64_t m = mp < 10 ? mp + 3 : mp - 9;
  if (m <= 2) {
    ++y;
  }
  snprintf(buf, static_cast<size_t>(cap), "%04ld-%02ld-%02ld",
           static_cast<long>(y), static_cast<long>(m), static_cast<long>(d));
}

static void file_parquet_format_datetime_from_seconds(const int64_t seconds,
                                                      char *buf, const int64_t cap)
{
  const int64_t days = seconds >= 0 ? seconds / 86400 : (seconds - 86399) / 86400;
  const int64_t sod = seconds - days * 86400;
  char date_buf[32];
  file_parquet_format_date(days, date_buf, sizeof(date_buf));
  snprintf(buf, static_cast<size_t>(cap), "%s %02ld:%02ld:%02ld", date_buf,
           static_cast<long>(sod / 3600), static_cast<long>((sod % 3600) / 60),
           static_cast<long>(sod % 60));
}

static int file_parquet_append_value(ObIAllocator &alloc, const FileParquetSchemaElement &element,
                                     const FileColType col_type,
                                     const char *data, const int64_t index, const int64_t count,
                                     bool &is_null, ObString &value)
{
  int ret = OB_SUCCESS;
  is_null = false;
  switch (element.type_) {
    case 0: {  // BOOLEAN, bit packed
      const int64_t byte_pos = index / 8;
      const int bit = static_cast<int>(index % 8);
      char *v = static_cast<char *>(alloc.alloc(2));
      if (NULL == v) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        v[0] = ((static_cast<uint8_t>(data[byte_pos]) >> bit) & 1) ? '1' : '0';
        v[1] = '\0';
        value.assign_ptr(v, 1);
      }
      UNUSED(count);
      break;
    }
    case 1: {  // INT32
      int32_t v = 0;
      MEMCPY(&v, data + index * 4, 4);
      if (FILE_COL_DATE == col_type) {
        char *v_str = static_cast<char *>(alloc.alloc(32));
        if (NULL == v_str) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          file_parquet_format_date(v, v_str, 32);
          value.assign_ptr(v_str, static_cast<int32_t>(strlen(v_str)));
        }
      } else {
        char *v_str = static_cast<char *>(alloc.alloc(32));
        if (NULL == v_str) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          snprintf(v_str, 32, "%d", v);
          value.assign_ptr(v_str, static_cast<int32_t>(strlen(v_str)));
        }
      }
      UNUSED(count);
      break;
    }
    case 2: {  // INT64
      int64_t v = 0;
      MEMCPY(&v, data + index * 8, 8);
      char *v_str = static_cast<char *>(alloc.alloc(48));
      if (NULL == v_str) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        if (FILE_COL_DATETIME == col_type) {
          int64_t seconds = v / 1000;
          const int32_t converted = element.converted_type_;
          if (10 == converted || element.logical_timestamp_) {
            seconds = v / 1000000;
          }
          file_parquet_format_datetime_from_seconds(seconds, v_str, 48);
        } else {
          snprintf(v_str, 48, "%ld", static_cast<long>(v));
        }
        value.assign_ptr(v_str, static_cast<int32_t>(strlen(v_str)));
      }
      UNUSED(count);
      break;
    }
    case 3: {  // INT96
      int64_t nanos = 0;
      int32_t julian = 0;
      MEMCPY(&nanos, data + index * 12, 8);
      MEMCPY(&julian, data + index * 12 + 8, 4);
      const int64_t seconds = (static_cast<int64_t>(julian) - 2440588) * 86400 + nanos / 1000000000;
      char *v_str = static_cast<char *>(alloc.alloc(48));
      if (NULL == v_str) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        file_parquet_format_datetime_from_seconds(seconds, v_str, 48);
        value.assign_ptr(v_str, static_cast<int32_t>(strlen(v_str)));
      }
      UNUSED(count);
      break;
    }
    case 4: {  // FLOAT
      float v = 0;
      MEMCPY(&v, data + index * 4, 4);
      char *v_str = static_cast<char *>(alloc.alloc(48));
      if (NULL == v_str) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        snprintf(v_str, 48, "%.9g", static_cast<double>(v));
        value.assign_ptr(v_str, static_cast<int32_t>(strlen(v_str)));
      }
      UNUSED(count);
      break;
    }
    case 5: {  // DOUBLE
      double v = 0;
      MEMCPY(&v, data + index * 8, 8);
      char *v_str = static_cast<char *>(alloc.alloc(48));
      if (NULL == v_str) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        snprintf(v_str, 48, "%.17g", v);
        value.assign_ptr(v_str, static_cast<int32_t>(strlen(v_str)));
      }
      UNUSED(count);
      break;
    }
    case 6:
    case 7: {
      // byte array: computed by caller (offsets needed), handled specially
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    default:
      ret = OB_INVALID_DATA;
      break;
  }
  return ret;
}


// ---------------------------------------------------------------------------
// PLAIN decoding of one page into strings
static int file_parquet_read_u32_le(const char *p, uint32_t &v)
{
  v = static_cast<uint32_t>(static_cast<uint8_t>(p[0]))
      | (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 8)
      | (static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 16)
      | (static_cast<uint32_t>(static_cast<uint8_t>(p[3])) << 24);
  return OB_SUCCESS;
}

static int file_parquet_plain_decode(const FileParquetSchemaElement &element,
                                     const char *data, const int64_t data_len,
                                     const int64_t count, ObIAllocator &alloc,
                                     ObIArray<ObString> &out)
{
  int ret = OB_SUCCESS;
  if (element.type_ >= 1 && element.type_ <= 5 && 7 != element.type_) {
    // fixed width values
    const int64_t width = (0 == element.type_) ? 0
                        : (1 == element.type_ ? 4 : (2 == element.type_ ? 8
                           : (3 == element.type_ ? 12 : (4 == element.type_ ? 4 : 8))));
    if (0 == element.type_) {
      // boolean bit packed
      const int64_t need = (count + 7) / 8;
      if (need > data_len) {
        ret = OB_INVALID_DATA;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        bool is_null = false;
        ObString v;
        if (OB_FAIL(file_parquet_append_value(alloc, element,
                   file_parquet_column_type(element), data, i, count, is_null, v))) {
        } else if (OB_FAIL(out.push_back(v))) {
        }
      }
    } else if (count * width > data_len) {
      ret = OB_INVALID_DATA;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        bool is_null = false;
        ObString v;
        if (OB_FAIL(file_parquet_append_value(alloc, element,
                   file_parquet_column_type(element), data, i, count, is_null, v))) {
        } else if (OB_FAIL(out.push_back(v))) {
        }
      }
    }
  } else if (6 == element.type_) {
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      uint32_t len = 0;
      if (pos + 4 > data_len) {
        ret = OB_INVALID_DATA;
      } else if (FALSE_IT(file_parquet_read_u32_le(data + pos, len))) {
      } else if (pos + 4 + static_cast<int64_t>(len) > data_len) {
        ret = OB_INVALID_DATA;
      } else {
        char *copy = static_cast<char *>(alloc.alloc(len + 1));
        if (NULL == copy) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          MEMCPY(copy, data + pos + 4, len);
          copy[len] = '\0';
          ObString v(static_cast<int32_t>(len), copy);
          pos += 4 + len;
          if (OB_FAIL(out.push_back(v))) {
          }
        }
      }
    }
  } else if (7 == element.type_) {
    const int64_t width = element.type_length_ > 0 ? element.type_length_ : 16;
    if (count * width > data_len) {
      ret = OB_INVALID_DATA;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        char *copy = static_cast<char *>(alloc.alloc(width + 1));
        if (NULL == copy) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          MEMCPY(copy, data + i * width, width);
          copy[width] = '\0';
          if (OB_FAIL(out.push_back(ObString(static_cast<int32_t>(width), copy)))) {
          }
        }
      }
    }
  } else {
    ret = file_sql_raise_not_supported("parquet physical type");
  }
  return ret;
}

struct FileParquetDecoded
{
  ObSEArray<ObString, 64> values_;
  ObSEArray<bool, 64> nulls_;
  FileParquetDecoded() : values_(), nulls_() {}
};

static int file_parquet_decode_column(const char *file_buf, const int64_t file_len,
                                      const FileParquetSchemaElement &element,
                                      const FileParquetColumnMeta &column,
                                      const int64_t row_count,
                                      ObIAllocator &alloc,
                                      FileParquetDecoded &decoded)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator page_alloc;
  ObSEArray<ObString, 128> dictionary;
  int64_t pos = column.data_page_offset_;
  if (column.dictionary_page_offset_ > 0
      && column.dictionary_page_offset_ < column.data_page_offset_) {
    pos = column.dictionary_page_offset_;
  }
  const bool optional = (1 == element.repetition_type_);
  int64_t values_produced = 0;
  const char *stage = "begin";
  int32_t last_page_type = -1;
  int32_t last_encoding = -1;
  int64_t last_page_pos = -1;
  int32_t last_num_values = 0;
  while (OB_SUCC(ret) && pos < file_len && values_produced < row_count) {
    FileThriftReader header_reader;
    header_reader.init(file_buf, file_len, pos);
    stage = "page_header";
    FileParquetPageHeader header;
    if (OB_FAIL(file_parquet_parse_page_header(header_reader, header))) {
    } else if (!header_reader.ok()) {
      ret = OB_INVALID_DATA;
    } else if (header.compressed_size_ < 0 || header.uncompressed_size_ < 0
               || header_reader.pos() + header.compressed_size_ > file_len) {
      LOG_WARN("parquet bad page size", K(header.compressed_size_), K(header.uncompressed_size_),
               K(pos), K(file_len));
      ret = OB_INVALID_DATA;
    } else {
      const char *compressed = file_buf + header_reader.pos();
      pos = header_reader.pos() + header.compressed_size_;
      char *page_data = NULL;
      int64_t page_len = 0;
      stage = "decompress";
      if (3 == header.type_) {
        // DATA_PAGE_V2: levels are stored uncompressed, only the values section
        // may be compressed; handled inside the v2 branch below.
        page_data = const_cast<char *>(compressed);
        page_len = header.compressed_size_;
      } else if (1 == column.codec_) {
        char *out = static_cast<char *>(page_alloc.alloc(header.uncompressed_size_ + 1));
        if (NULL == out) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_FAIL(file_parquet_snappy_uncompress(
                     compressed, header.compressed_size_, out,
                     header.uncompressed_size_, page_len))) {
        } else {
          out[page_len] = '\0';
          page_data = out;
        }
      } else if (0 == column.codec_) {
        page_data = const_cast<char *>(compressed);
        page_len = header.compressed_size_;
      } else {
        ret = file_sql_raise_not_supported("parquet compression codec");
      }
      stage = "dictionary";
      last_page_type = header.type_;
      last_encoding = header.encoding_;
      last_page_pos = pos;
      last_num_values = header.num_values_;
      if (OB_SUCC(ret) && 2 == header.type_) {
        // dictionary page
        if (OB_FAIL(file_parquet_plain_decode(element, page_data, page_len,
                                              header.dict_num_values_, page_alloc, dictionary))) {
        }
      } else if (OB_SUCC(ret) && 0 == header.type_) {
        stage = "data_page_v1";
        int64_t data_pos = 0;
        stage = "def_levels_v1";
        ObSEArray<int32_t, 64> defs;
        if (optional) {
          uint32_t level_len = 0;
          if (data_pos + 4 > page_len) {
            ret = OB_INVALID_DATA;
          } else if (FALSE_IT(file_parquet_read_u32_le(page_data + data_pos, level_len))) {
          } else {
            data_pos += 4;
            if (data_pos + static_cast<int64_t>(level_len) > page_len) {
              ret = OB_INVALID_DATA;
            } else if (OB_FAIL(file_rle_decode(page_data + data_pos, level_len, 1,
                                               header.num_values_, defs))) {
            } else {
              data_pos += level_len;
            }
          }
        }
        int64_t non_null = 0;
        if (OB_SUCC(ret)) {
          for (int64_t i = 0; OB_SUCC(ret) && i < header.num_values_; ++i) {
            if (optional) {
              if (i < defs.count()) {
                if (defs.at(i) > 0) {
                  ++non_null;
                }
              } else {
                ret = OB_INVALID_DATA;
              }
            } else {
              ++non_null;
            }
          }
        }
        if (OB_SUCC(ret)) {
          ObSEArray<ObString, 128> page_values;
          const int32_t encoding = header.encoding_;
          stage = "plain_v1";
          if (0 == encoding) {
            if (OB_FAIL(file_parquet_plain_decode(element, page_data + data_pos,
                                                  page_len - data_pos, non_null,
                                                  page_alloc, page_values))) {
              LOG_WARN("parquet plain decode failed", K(ret), K(non_null),
                       "data_left", page_len - data_pos);
            }
          } else if (2 == encoding || 8 == encoding) {
            if (dictionary.count() <= 0) {
              ret = OB_INVALID_DATA;
            } else if (data_pos >= page_len) {
              ret = OB_INVALID_DATA;
            } else {
              const int32_t bit_width = static_cast<uint8_t>(page_data[data_pos++]);
              ObSEArray<int32_t, 64> indices;
              if (OB_FAIL(file_rle_decode(page_data + data_pos, page_len - data_pos,
                                          bit_width, non_null, indices))) {
              }
              for (int64_t i = 0; OB_SUCC(ret) && i < indices.count(); ++i) {
                const int64_t idx = indices.at(i);
                if (idx < 0 || idx >= dictionary.count()) {
                  ret = OB_INVALID_DATA;
                } else if (OB_FAIL(page_values.push_back(dictionary.at(idx)))) {
                }
              }
            }
          } else {
            ret = file_sql_raise_not_supported("parquet value encoding");
          }
          if (OB_SUCC(ret)) {
            int64_t value_idx = 0;
            for (int64_t i = 0; OB_SUCC(ret) && i < header.num_values_; ++i) {
              bool is_null = optional ? (i >= defs.count() || defs.at(i) == 0) : false;
              ObString value;
              if (!is_null) {
                if (value_idx >= page_values.count()) {
                  ret = OB_INVALID_DATA;
                } else {
                  value = page_values.at(value_idx++);
                }
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(decoded.values_.push_back(value))) {
                } else if (OB_FAIL(decoded.nulls_.push_back(is_null))) {
                }
              }
            }
            values_produced += header.num_values_;
          }
        }
      } else if (OB_SUCC(ret) && 3 == header.type_) {
        stage = "data_page_v2";
        // DATA_PAGE_V2: levels are not compressed, only the values section may be
        int64_t data_pos = 0;
        int64_t page_values_len = page_len;
        char *values_ptr = page_data;
        if (header.def_bytes_ < 0 || header.rep_bytes_ < 0
            || header.def_bytes_ + header.rep_bytes_ > page_len) {
          ret = OB_INVALID_DATA;
        } else if (1 == column.codec_ && header.v2_is_compressed_) {
          const int64_t level_bytes = header.def_bytes_ + header.rep_bytes_;
          const int64_t comp_values_len = page_len - level_bytes;
          char *out = static_cast<char *>(page_alloc.alloc(header.uncompressed_size_ + 1));
          int64_t out_len = 0;
          if (NULL == out) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else if (OB_FAIL(file_parquet_snappy_uncompress(
                       page_data + level_bytes, comp_values_len, out,
                       header.uncompressed_size_, out_len))) {
          } else {
            out[out_len] = '\0';
            values_ptr = out;
            page_values_len = out_len;
            data_pos = 0;
          }
        } else {
          values_ptr = page_data + header.def_bytes_ + header.rep_bytes_;
          page_values_len = page_len - header.def_bytes_ - header.rep_bytes_;
          data_pos = 0;
        }
        stage = "def_levels_v2";
        ObSEArray<int32_t, 64> defs;
        if (OB_SUCC(ret) && optional && header.def_bytes_ > 0) {
          if (OB_FAIL(file_rle_decode(page_data, header.def_bytes_, 1,
                                      header.num_values_, defs))) {
          }
        }
        int64_t non_null = header.num_values_ - header.num_nulls_;
        if (OB_SUCC(ret) && optional && defs.count() > 0) {
          non_null = 0;
          for (int64_t i = 0; i < defs.count(); ++i) {
            if (defs.at(i) > 0) {
              ++non_null;
            }
          }
        }
        if (OB_SUCC(ret)) {
          ObSEArray<ObString, 128> page_values;
          const int32_t encoding = header.encoding_;
          stage = "plain_v2";
          if (0 == encoding) {
            if (OB_FAIL(file_parquet_plain_decode(element, values_ptr, page_values_len,
                                                  non_null, page_alloc, page_values))) {
              LOG_WARN("parquet v2 plain decode failed", K(ret), K(non_null), K(page_values_len));
            }
          } else if (2 == encoding || 8 == encoding) {
            if (dictionary.count() <= 0 || page_values_len <= 0) {
              ret = OB_INVALID_DATA;
            } else {
              const int32_t bit_width = static_cast<uint8_t>(values_ptr[0]);
              ObSEArray<int32_t, 64> indices;
              if (OB_FAIL(file_rle_decode(values_ptr + 1, page_values_len - 1,
                                          bit_width, non_null, indices))) {
              }
              for (int64_t i = 0; OB_SUCC(ret) && i < indices.count(); ++i) {
                const int64_t idx = indices.at(i);
                if (idx < 0 || idx >= dictionary.count()) {
                  ret = OB_INVALID_DATA;
                } else if (OB_FAIL(page_values.push_back(dictionary.at(idx)))) {
                }
              }
            }
          } else {
            ret = file_sql_raise_not_supported("parquet v2 value encoding");
          }
          if (OB_SUCC(ret)) {
            int64_t value_idx = 0;
            for (int64_t i = 0; OB_SUCC(ret) && i < header.num_values_; ++i) {
              bool is_null = optional ? (i >= defs.count() || defs.at(i) == 0) : false;
              ObString value;
              if (!is_null) {
                if (value_idx >= page_values.count()) {
                  ret = OB_INVALID_DATA;
                } else {
                  value = page_values.at(value_idx++);
                }
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(decoded.values_.push_back(value))) {
                } else if (OB_FAIL(decoded.nulls_.push_back(is_null))) {
                }
              }
            }
            values_produced += header.num_values_;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && decoded.values_.count() > row_count) {
    // trim to row count
    while (decoded.values_.count() > row_count) {
      (void)decoded.values_.pop_back();
      (void)decoded.nulls_.pop_back();
    }
  }
  if (OB_SUCC(ret) && decoded.values_.count() != row_count) {
    ret = OB_INVALID_DATA;
  }
  if (OB_FAIL(ret)) {
    char msg[256];
    snprintf(msg, sizeof(msg),
             "parquet decode failed at %s (ret=%d, rows=%ld, produced=%ld, ptype=%d, enc=%d, "
             "nvals=%d, page_end=%ld)",
             stage, ret, static_cast<long>(row_count), static_cast<long>(decoded.values_.count()),
             last_page_type, last_encoding, last_num_values, static_cast<long>(last_page_pos));
    LOG_WARN("parquet decode failed", "stage", stage, K(ret),
             "rows", row_count, "produced", decoded.values_.count());
    LOG_USER_ERROR(OB_NOT_SUPPORTED, msg);
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

static int file_parquet_leaf_index(const FileParquetMeta &meta, const int64_t leaf,
                                   const FileParquetSchemaElement *&element)
{
  int ret = OB_SUCCESS;
  int64_t leaf_count = 0;
  element = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < meta.schema_.count(); ++i) {
    if (meta.schema_.at(i).num_children_ <= 0) {
      if (leaf_count == leaf) {
        element = &meta.schema_.at(i);
      }
      ++leaf_count;
    }
  }
  if (NULL == element) {
    ret = OB_INVALID_DATA;
  }
  return ret;
}

static int file_sql_materialize_parquet(const ObString &path,
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
  } else if (len < 12 || 0 != memcmp(buf, "PAR1", 4) || 0 != memcmp(buf + len - 4, "PAR1", 4)) {
    ret = OB_INVALID_DATA;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid parquet file (bad magic)");
    ret = OB_NOT_SUPPORTED;
  } else {
    uint32_t footer_len = 0;
    file_parquet_read_u32_le(buf + len - 8, footer_len);
    if (static_cast<int64_t>(footer_len) + 8 >= len) {
      ret = OB_INVALID_DATA;
    } else {
      FileThriftReader reader;
      reader.init(buf, len, len - 8 - static_cast<int64_t>(footer_len));
      FileParquetMeta meta;
      if (OB_FAIL(file_parquet_parse_metadata(reader, meta, alloc))) {
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "corrupted parquet metadata");
        ret = OB_NOT_SUPPORTED;
      } else if (meta.row_groups_.count() <= 0) {
        ret = file_sql_raise_not_supported("empty parquet file");
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < meta.schema_.count(); ++i) {
          if (meta.schema_.at(i).num_children_ <= 0) {
            FileColumn col;
            const FileParquetSchemaElement &el = meta.schema_.at(i);
            if (OB_FAIL(ob_write_string(alloc, el.name_, col.name_))) {
            } else {
              col.type_ = file_parquet_column_type(el);
              col.nullable_ = (1 == el.repetition_type_);
              col.max_bytes_ = 64;
              if (OB_FAIL(schema.columns_.push_back(col))) {
              }
            }
          }
        }
        if (OB_SUCC(ret) && schema.columns_.count() <= 0) {
          ret = file_sql_raise_not_supported("parquet schema");
        } else if (OB_SUCC(ret)) {
          builder.set_schema(schema);
          if (OB_FAIL(builder.create_table())) {
          } else {
            const int64_t column_count = schema.columns_.count();
            for (int64_t rg = 0; OB_SUCC(ret) && rg < meta.row_groups_.count(); ++rg) {
              FileParquetRowGroup &row_group = meta.row_groups_.at(rg);
              ObSEArray<FileParquetDecoded, 8> decoded_columns;
              for (int64_t c = 0; OB_SUCC(ret) && c < column_count; ++c) {
                const FileParquetSchemaElement *element = NULL;
                FileParquetDecoded decoded;
                if (OB_FAIL(file_parquet_leaf_index(meta, c, element))) {
                } else if (c >= row_group.columns_.count()) {
                  ret = OB_INVALID_DATA;
                } else if (OB_FAIL(file_parquet_decode_column(buf, len, *element,
                                                              row_group.columns_.at(c),
                                                              row_group.num_rows_,
                                                              alloc, decoded))) {
                  // the detailed stage message has already been set by decode_column
                  ret = OB_NOT_SUPPORTED;
                } else if (OB_FAIL(decoded_columns.push_back(decoded))) {
                }
              }
              ObArenaAllocator row_alloc;
              for (int64_t r = 0; OB_SUCC(ret) && r < row_group.num_rows_; ++r) {
                ObSEArray<ObString, 16> values;
                ObSEArray<bool, 16> nulls;
                for (int64_t c = 0; OB_SUCC(ret) && c < column_count; ++c) {
                  const FileParquetDecoded &dec = decoded_columns.at(c);
                  if (r >= dec.values_.count()) {
                    ret = OB_INVALID_DATA;
                  } else if (OB_FAIL(values.push_back(dec.values_.at(r)))) {
                  } else if (OB_FAIL(nulls.push_back(dec.nulls_.at(r)))) {
                  }
                }
                if (OB_SUCC(ret) && OB_FAIL(builder.add_row(values, nulls))) {
                }
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(builder.finish())) {
            }
          }
        }
      }
    }
  }
  return ret;
}

} // namespace file_sql
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_FILE_SQL_PARQUET_INC_
