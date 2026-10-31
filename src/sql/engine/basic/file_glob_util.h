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
// Standalone glob() support for the seekdb "file as database" feature.
// A very small subset of shell globbing: '*' and '?' inside the last path
// component. Matches the competition requirement that glob() only discovers
// files and returns a single "file" column holding the full path.

#include <algorithm>
#include <filesystem>
#include <string>
#include <vector>

namespace file_sql {

inline bool wildcard_match(const std::string &pattern, const std::string &name)
{
  size_t p = 0, n = 0;
  size_t star = std::string::npos, mark = 0;
  while (n < name.size()) {
    if (p < pattern.size() && (pattern[p] == '?' || pattern[p] == name[n])) { ++p; ++n; }
    else if (p < pattern.size() && pattern[p] == '*') { star = p++; mark = n; }
    else if (star != std::string::npos) { p = star + 1; n = ++mark; }
    else { return false; }
  }
  while (p < pattern.size() && pattern[p] == '*') { ++p; }
  return p == pattern.size();
}

// Returns the full paths of the files matching the pattern, sorted by path.
inline std::vector<std::string> glob_files(const std::string &pattern)
{
  namespace fs = std::filesystem;
  std::vector<std::string> out;
  fs::path pat(pattern);
  fs::path dir = pat.parent_path();
  std::string file_pat = pat.filename().string();
  if (file_pat.find('*') == std::string::npos && file_pat.find('?') == std::string::npos) {
    std::error_code ec;
    if (fs::exists(pat, ec) && fs::is_regular_file(pat, ec)) {
      out.push_back(pat.generic_string());
    }
    return out;
  }
  if (dir.empty()) { dir = "."; }
  std::error_code ec;
  fs::directory_iterator it(dir, ec), end;
  if (ec) { return out; }
  for (; it != end; it.increment(ec)) {
    if (ec) { break; }
    if (!it->is_regular_file(ec) || ec) { continue; }
    std::string name = it->path().filename().string();
    if (wildcard_match(file_pat, name)) {
      out.push_back(it->path().generic_string());
    }
  }
  std::sort(out.begin(), out.end());
  return out;
}

}  // namespace file_sql
