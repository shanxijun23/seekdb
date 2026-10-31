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
#pragma once
// Standalone JSONL (newline-delimited JSON) reader + union schema inference
// for the seekdb "file as database" feature. Flat objects only: nested
// objects and arrays are rejected, matching the competition requirement.

#include "file_csv_util.h"

#include <cctype>
#include <string>
#include <utility>
#include <vector>

namespace file_sql {

struct JsonField {
  std::string name;
  std::string value;   // raw text for strings, numeric text for numbers
  bool is_null = false;
  bool is_number = false;
  bool is_bool = false;
  bool is_string = false;
};

inline void skip_ws(const std::string &s, size_t &i)
{
  while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) { ++i; }
}

// Parses one flat JSON object. Returns false when the line is not a flat object.
inline bool parse_json_object(const std::string &line, std::vector<JsonField> &fields, bool &nested)
{
  nested = false;
  size_t i = 0;
  skip_ws(line, i);
  if (i >= line.size() || line[i] != '{') return false;
  ++i;
  skip_ws(line, i);
  if (i < line.size() && line[i] == '}') return true;  // empty object
  while (i < line.size()) {
    skip_ws(line, i);
    if (i >= line.size() || line[i] != '"') return false;
    ++i;
    std::string key;
    while (i < line.size() && line[i] != '"') { key.push_back(line[i++]); }
    if (i >= line.size()) return false;
    ++i;  // closing quote
    skip_ws(line, i);
    if (i >= line.size() || line[i] != ':') return false;
    ++i;
    skip_ws(line, i);
    if (i >= line.size()) return false;

    JsonField f;
    f.name = key;
    char c = line[i];
    if (c == '"') {
      ++i;
      std::string v;
      while (i < line.size() && line[i] != '"') {
        if (line[i] == '\\' && i + 1 < line.size()) { ++i; }
        v.push_back(line[i++]);
      }
      if (i >= line.size()) return false;
      ++i;
      f.value = v;
      f.is_string = true;
    } else if (c == '{' || c == '[') {
      nested = true;
      return false;
    } else if (c == 't' || c == 'f') {
      if (line.compare(i, 4, "true") == 0) { f.value = "true"; i += 4; }
      else if (line.compare(i, 5, "false") == 0) { f.value = "false"; i += 5; }
      else return false;
      f.is_bool = true;
    } else if (c == 'n') {
      if (line.compare(i, 4, "null") != 0) return false;
      i += 4;
      f.is_null = true;
    } else {
      size_t start = i;
      while (i < line.size() && (std::isdigit(static_cast<unsigned char>(line[i]))
             || line[i] == '-' || line[i] == '+' || line[i] == '.' || line[i] == 'e'
             || line[i] == 'E')) { ++i; }
      if (start == i) return false;
      f.value = line.substr(start, i - start);
      f.is_number = true;
    }
    fields.push_back(f);

    skip_ws(line, i);
    if (i < line.size() && line[i] == ',') { ++i; continue; }
    if (i < line.size() && line[i] == '}') { ++i; break; }
    return false;
  }
  skip_ws(line, i);
  return i == line.size();
}

inline std::vector<Column> infer_jsonl_schema(const std::string &text)
{
  std::vector<Column> cols;
  size_t pos = 0;
  while (pos <= text.size()) {
    size_t nl = text.find('\n', pos);
    std::string line = text.substr(pos, nl == std::string::npos ? std::string::npos : nl - pos);
    pos = (nl == std::string::npos) ? text.size() + 1 : nl + 1;
    if (!line.empty() && line.back() == '\r') { line.pop_back(); }
    bool blank = true;
    for (char ch : line) { if (!std::isspace(static_cast<unsigned char>(ch))) { blank = false; break; } }
    if (blank) continue;

    std::vector<JsonField> fields;
    bool nested = false;
    if (!parse_json_object(line, fields, nested)) continue;

    for (auto &col : cols) { col.nullable = true; }  // absent unless present below
    for (auto &col : cols) {
      bool present = false;
      for (auto &fld : fields) { if (fld.name == col.name) { present = true; break; } }
      if (present) { col.nullable = false; }
    }

    for (const auto &fld : fields) {
      size_t idx = cols.size();
      for (size_t k = 0; k < cols.size(); ++k) { if (cols[k].name == fld.name) { idx = k; break; } }
      if (idx == cols.size()) {
        Column c;
        c.name = fld.name;
        c.nullable = true;  // rows before this one did not carry it
        cols.push_back(c);
      }
      Column &col = cols[idx];
      if (fld.is_null) { col.nullable = true; continue; }

      FileType t;
      if (fld.is_bool) { t = FileType::BOOLEAN; }
      else if (fld.is_number) { t = (fld.value.find('.') != std::string::npos) ? FileType::DOUBLE : FileType::BIGINT; }
      else { t = merge_type(FileType::VARCHAR, true, fld.value); }

      if (col.type == t) { /* unchanged */ }
      else if (col.type == FileType::BIGINT && t == FileType::DOUBLE) { col.type = FileType::DOUBLE; }
      else if (col.type == FileType::BIGINT && t == FileType::BOOLEAN) { col.type = FileType::BOOLEAN; }
      else if (col.type == FileType::BOOLEAN && t == FileType::BIGINT) { /* keep */ }
      else if (col.type == FileType::DOUBLE && t == FileType::BIGINT) { /* keep */ }
      else if (col.type == FileType::DATE && t == FileType::DATETIME) { col.type = FileType::DATETIME; }
      else { col.type = t; }
    }
  }
  // a column is NOT NULL only when every row carried it with a non-null value
  return cols;
}
}  // namespace file_sql
