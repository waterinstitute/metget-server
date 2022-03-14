// MIT License
//
// Copyright (c) 2020 ADCIRC Development Group
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// Author: Zach Cobell
// Contact: zcobell@thewaterinstitute.org
//
#ifndef METGET_LIBRARY_OWIASCII_H_
#define METGET_LIBRARY_OWIASCII_H_

#include <string>

#include "Grid.h"
#include "MeteorologicalData.h"
#include "OutputFile.h"
#include "OwiAsciiDomain.h"

namespace MetBuild {

class OwiAscii : public OutputFile {
 public:
  OwiAscii(const MetBuild::Date &date_start, const MetBuild::Date &date_end,
           unsigned time_step, bool use_compression = false);

  void addDomain(const MetBuild::Grid &w,
                 const std::vector<std::string> &filenames) override;

  int write(
      const MetBuild::Date &date, size_t domain_index,
      const MetBuild::MeteorologicalData<1, MetBuild::MeteorologicalDataType>
          &data) override;

  int write(
      const MetBuild::Date &date, size_t domain_index,
      const MetBuild::MeteorologicalData<3, MetBuild::MeteorologicalDataType>
          &data) override;

  void close_domain(size_t domain);

 private:
  const bool m_use_compression;
};
}  // namespace MetBuild
#endif  // METGET_LIBRARY_OWIASCII_H_
