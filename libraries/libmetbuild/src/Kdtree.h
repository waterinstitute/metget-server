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
#ifndef KDTREE_H
#define KDTREE_H

#include <cstddef>
#include <memory>
#include <vector>

namespace MetBuild {

class KdtreePrivate;

class Kdtree {
 public:
  Kdtree();

  Kdtree(const std::vector<double> &x, const std::vector<double> &y);

  ~Kdtree();

  enum _errors { NoError, SizeMismatch };

  size_t size();

  size_t findNearest(double x, double y) const;

  std::vector<std::pair<size_t, double>> findXNearest(double x, double y,
                                                      size_t n) const;

  std::vector<size_t> findWithinRadius(double x, double y, double radius) const;

  bool initialized();

 private:
  std::unique_ptr<KdtreePrivate> m_ptr;
};
}  // namespace MetBuild
#endif  // KDTREE_H
