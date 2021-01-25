#ifndef KDTREE_PRIVATE_H
#define KDTREE_PRIVATE_H

#include <cstdlib>
#include <memory>
#include <vector>

#include "nanoflann.hpp"

namespace MetBuild {

class KdtreePrivate {
 public:
  KdtreePrivate();

  KdtreePrivate(const std::vector<double> &x, const std::vector<double> &y);

  bool initialized() const;

  size_t size() const;

  size_t findNearest(double x, double y) const;

  std::vector<std::pair<size_t, double>> findXNearest(double x, double y, size_t n) const;

  std::vector<size_t> findWithinRadius(double x, double y, double radius) const;

 private:
  int build(const std::vector<double> &x, const std::vector<double> &y);

  bool m_initialized;

  template <typename T>
  struct PointCloud {
    struct Point {
      T x, y;
    };

    std::vector<Point> pts;

    // Must return the number of data points
    inline size_t kdtree_get_point_count() const { return pts.size(); }

    // Returns the dim'th component of the idx'th point in the class:
    // Since this is inlined and the "dim" argument is typically an immediate
    // value, the
    //  "if/else's" are actually solved at compile time.
    inline T kdtree_get_pt(const size_t idx, const size_t dim) const {
      if (dim == 0)
        return pts[idx].x;
      else
        return pts[idx].y;
    }

    // Optional bounding-box computation: return false to default to a standard
    // bbox computation loop.
    //   Return true if the BBOX was already computed by the class and returned
    //   in "bb" so it can be avoided to redo it again. Look at bb.size() to
    //   find out the expected dimensionality (e.g. 2 or 3 for point clouds)
    template <class BBOX>
    bool kdtree_get_bbox(BBOX & /* bb */) const {
      return false;
    }
  };

  // construct a kd-tree index:
  typedef nanoflann::KDTreeSingleIndexAdaptor<
      nanoflann::L2_Simple_Adaptor<double, PointCloud<double> >,
      PointCloud<double>, 2>
      kd_tree_t;

  PointCloud<double> m_cloud;
  std::unique_ptr<kd_tree_t> m_tree;
  const nanoflann::SearchParams m_params;
};
}  // namespace MetBuild

#endif  // KDTREE_PRIVATE_H
