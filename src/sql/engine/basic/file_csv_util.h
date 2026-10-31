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
// Standalone CSV parsing + deterministic type inference for the seekdb
// "file as database" feature. No OceanBase dependencies so it can be
// iterated on and unit-tested outside the engine build.

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace file_sql {

enum class FileType { BIGINT, DOUBLE, BOOLEAN, VARCHAR, DATE, DATETIME };

inline const char *type_name(FileType t)
{
  switch (t) {
    case FileType::BIGINT: return "BIGINT";
    case FileType::DOUBLE: return "DOUBLE";
    case FileType::BOOLEAN: return "BOOLEAN";
    case FileType::VARCHAR: return "VARCHAR";
    case FileType::DATE: return "DATE";
    case FileType::DATETIME: return "DATETIME";
  }
  return "VARCHAR";
}

struct Column {
  std::string name;
  FileType type = FileType::VARCHAR;
  bool nullable = false;
};

// Minimal RFC4180 reader: quotes, doubled quotes, embedded newlines.
inline std::vector<std::vector<std::string>> parse_csv(const std::string &text, int64_t *bad_lines = nullptr)
{
  std::vector<std::vector<std::string>> rows;
  std::vector<std::string> row;
  std::string field;
  bool in_quotes = false;
  bool field_started = false;
  size_t i = 0;
  int64_t bad = 0;

  auto end_field = [&]() {
    row.push_back(field);
    field.clear();
    field_started = false;
  };
  auto end_row = [&]() {
    end_field();
    rows.push_back(row);
    row.clear();
  };

  while (i < text.size()) {
    char c = text[i];
    if (in_quotes) {
      if (c == '"') {
        if (i + 1 < text.size() && text[i + 1] == '"') {
          field.push_back('"');
          i += 2;
          continue;
        }
        in_quotes = false;
        ++i;
        continue;
      }
      field.push_back(c);
      ++i;
      continue;
    }
    if (c == '"' && !field_started) {
      in_quotes = true;
      field_started = true;
      ++i;
      continue;
    }
    if (c == ',') {
      end_field();
      ++i;
      continue;
    }
    if (c == '\r') {
      ++i;
      continue;
    }
    if (c == '\n') {
      end_row();
      ++i;
      continue;
    }
    field.push_back(c);
    field_started = true;
    ++i;
  }
  if (in_quotes) { ++bad; }
  if (field_started || !row.empty() || !field.empty()) { end_row(); }
  if (bad_lines) { *bad_lines = bad; }
  return rows;
}

inline bool is_int(const std::string &s)
{
  if (s.empty()) return false;
  size_t i = (s[0] == '+' || s[0] == '-') ? 1 : 0;
  if (i >= s.size()) return false;
  for (; i < s.size(); ++i) { if (s[i] < '0' || s[i] > '9') return false; }
  return true;
}

inline bool is_double(const std::string &s)
{
  if (s.empty()) return false;
  char *end = nullptr;
  std::strtod(s.c_str(), &end);
  return end != nullptr && *end == '\0';
}

inline bool is_bool(const std::string &s)
{
  return s == "true" || s == "false" || s == "TRUE" || s == "FALSE" || s == "True" || s == "False";
}

inline bool is_date(const std::string &s)
{
  // YYYY-MM-DD with a valid calendar day
  if (s.size() != 10 || s[4] != '-' || s[7] != '-') return false;
  for (int i : {0,1,2,3,5,6,8,9}) { if (s[i] < '0' || s[i] > '9') return false; }
  int m = std::atoi(s.substr(5, 2).c_str());
  int d = std::atoi(s.substr(8, 2).c_str());
  return m >= 1 && m <= 12 && d >= 1 && d <= 31;
}

inline bool is_datetime(const std::string &s)
{
  if (s.size() < 19) return false;
  if (!is_date(s.substr(0, 10))) return false;
  char sep = s[10];
  if (sep != ' ' && sep != 'T') return false;
  if (s[13] != ':' || s[16] != ':') return false;
  return true;
}

// Widen a column's type when a new value is seen.
inline FileType merge_type(FileType cur, bool has_value, const std::string &v)
{
  if (!has_value) return cur;
  if (!is_int(v)) {
    if (cur == FileType::BIGINT) { cur = is_double(v) ? FileType::DOUBLE : FileType::VARCHAR; }
  }
  if (!is_int(v) && !is_double(v)) {
    if (is_bool(v)) {
      if (cur != FileType::BIGINT && cur != FileType::DOUBLE) cur = FileType::BOOLEAN;
    } else if (is_datetime(v)) {
      if (cur == FileType::VARCHAR) cur = FileType::DATETIME;
      if (cur == FileType::DATE) cur = FileType::DATETIME;
    } else if (is_date(v)) {
      if (cur == FileType::VARCHAR) cur = FileType::DATE;
    } else {
      cur = FileType::VARCHAR;
    }
  }
  return cur;
}

inline std::vector<Column> infer_csv_schema(const std::vector<std::vector<std::string>> &rows,
                                            bool has_header)
{
  std::vector<Column> cols;
  if (rows.empty()) return cols;
  size_t first_data = has_header ? 1 : 0;
  size_t ncol = 0;
  for (const auto &r : rows) { if (r.size() > ncol) ncol = r.size(); }

  // Names: header or c1..cn; duplicate header names get __2, __3 ...
  std::vector<std::string> names;
  if (has_header && !rows[0].empty()) {
    names = rows[0];
  } else {
    for (size_t i = 0; i < ncol; ++i) names.push_back("column" + std::to_string(i + 1));
  }
  std::vector<std::string> final_names;
  for (size_t i = 0; i < ncol; ++i) {
    std::string base = i < names.size() ? names[i] : ("column" + std::to_string(i + 1));
    std::string candidate = base;
    int suffix = 2;
    bool clash = true;
    while (clash) {
      clash = false;
      for (const auto &existing : final_names) {
        if (existing == candidate) { clash = true; break; }
      }
      if (clash) { candidate = base + "__" + std::to_string(suffix++); }
    }
    final_names.push_back(candidate);
  }

  for (size_t c = 0; c < ncol; ++c) {
    Column col;
    col.name = final_names[c];
    bool saw_value = false;
    FileType t = FileType::BIGINT;
    for (size_t r = first_data; r < rows.size(); ++r) {
      if (c >= rows[r].size()) continue;
      const std::string &v = rows[r][c];
      if (v.empty()) { col.nullable = true; continue; }
      t = merge_type(t, saw_value, v);
      saw_value = true;
    }
    if (!saw_value) { t = FileType::VARCHAR; }
    col.type = t;
    cols.push_back(col);
  }
  return cols;
}

}  // namespace file_sql
