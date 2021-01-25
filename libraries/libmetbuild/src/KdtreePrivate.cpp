#include "KdtreePrivate.h"

#include <memory>

#include "Kdtree.h"

using namespace MetBuild;

Kdtree::~Kdtree() = default;

KdtreePrivate::KdtreePrivate() : m_initialized(false) {}

KdtreePrivate::KdtreePrivate(const std::vector<double> &x,
                             const std::vector<double> &y)
    : m_initialized(false), m_params(32, 0, true) {
  this->build(x, y);
}

bool KdtreePrivate::initialized() const { return this->m_initialized; }

size_t KdtreePrivate::size() const { return this->m_cloud.pts.size(); }

int KdtreePrivate::build(const std::vector<double> &x,
                         const std::vector<double> &y) {
  if (x.size() != y.size()) return 1;
  this->m_cloud.pts.resize(x.size());
  for (size_t i = 0; i < x.size(); ++i) {
    this->m_cloud.pts[i].x = x[i];
    this->m_cloud.pts[i].y = y[i];
  }
  this->m_tree = std::make_unique<kd_tree_t>(
      2, this->m_cloud, nanoflann::KDTreeSingleIndexAdaptorParams(10));
  this->m_tree->buildIndex();
  this->m_initialized = true;
  return 0;
}

size_t KdtreePrivate::findNearest(const double x, const double y) const {
  size_t index;
  double out_dist_sqr;
  nanoflann::KNNResultSet<double> resultSet(1);
  resultSet.init(&index, &out_dist_sqr);
  const double query_pt[2] = {x, y};
  this->m_tree->findNeighbors(resultSet, &query_pt[0], m_params);
  return index;
}

std::vector<std::pair<size_t, double>> KdtreePrivate::findXNearest(
    double x, double y, size_t n) const {
  n = std::min(this->size(), n);
  std::vector<size_t> index(n);
  std::vector<double> out_dist_sqr(n);

  nanoflann::KNNResultSet<double> resultSet(n);
  resultSet.init(index.data(), out_dist_sqr.data());
  const double query_pt[2] = {x, y};

  this->m_tree->findNeighbors(resultSet, &query_pt[0], m_params);

  std::vector<std::pair<size_t, double>> output;
  output.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    output.emplace_back(index[i], std::sqrt(out_dist_sqr[i]));
  }
  return output;
}

std::vector<size_t> KdtreePrivate::findWithinRadius(const double x,
                                                    const double y,
                                                    const double radius) const {
  //...Square radius since distance metric is a square distance
  const double search_radius = std::pow(radius, 2.0);

  const double query_pt[2] = {x, y};

  std::vector<std::pair<size_t, double>> matches;

  //...Perform the search
  const size_t nmatches =
      this->m_tree->radiusSearch(query_pt, search_radius, matches, m_params);

  //...Save the indices into a vector to return
  std::vector<size_t> outMatches;
  outMatches.reserve(nmatches);
  for (auto &match : matches) {
    outMatches.push_back(match.first);
  }
  return outMatches;
}
