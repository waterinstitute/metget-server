#include "Grib.h"

#include <cmath>
#include <iostream>
#include <memory>
#include <utility>

#include "Geometry.h"
#include "Logging.h"
#include "boost/algorithm/string/trim.hpp"
#include "boost/format.hpp"
#include "eccodes.h"

using namespace MetBuild;

Grib::Grib(std::string filename)
    : m_filename(std::move(filename)),
      m_tree(nullptr),
      m_ni(0),
      m_nj(0),
      m_size(0),
      m_convention(0) {
  initialize();
}

Grib::~Grib() = default;

std::string Grib::filename() const { return m_filename; }

bool isnotalpha(char c) { return !isalpha(c) && !isalnum(c); }

codes_handle *Grib::make_handle(const std::string &filename,
                                const std::string &name) {
  FILE *f = fopen(filename.c_str(), "r");
  int ierr = 0;

  while (auto h = codes_handle_new_from_file(codes_context_get_default(), f,
                                             PRODUCT_GRIB, &ierr)) {
    CODES_CHECK(ierr, nullptr);
    std::string pname;
    size_t plen = 0;
    CODES_CHECK(codes_get_length(h, "shortName", &plen), nullptr);
    pname.resize(plen, ' ');
    CODES_CHECK(codes_get_string(h, "shortName", &pname[0], &plen), nullptr);
    boost::trim_if(pname, isnotalpha);
    if (pname == name) {
      fclose(f);
      return h;
    }
  }
  fclose(f);
  metbuild_throw_exception("Could not generate the eccodes handle");
}

void Grib::close_handle(codes_handle *handle) {
  if (handle) {
    codes_handle_delete(handle);
  }
}

void Grib::initialize() {
  codes_grib_multi_support_on(grib_context_get_default());

  auto handle = Grib::make_handle(m_filename, "prmsl");
  CODES_CHECK(codes_get_long(handle, "Ni", &m_ni), nullptr);
  CODES_CHECK(codes_get_long(handle, "Nj", &m_nj), nullptr);
  CODES_CHECK(codes_get_size(handle, "values", &m_size), nullptr);

  this->readCoordinates(handle);
  Grib::close_handle(handle);

  m_tree = std::make_unique<Kdtree>(m_longitude, m_latitude);
  this->findCorners();
}

std::vector<double> Grib::getGribArray1d(const std::string &name) {
  auto pvm = m_preread_value_map.find(name);
  if (pvm == m_preread_value_map.end()) {
    std::vector<double> arr1d(m_size, 0.0);
    size_t s = m_size;
    auto handle = Grib::make_handle(m_filename, name);
    CODES_CHECK(codes_get_double_array(handle, "values", arr1d.data(), &s),
                nullptr);
    Grib::close_handle(handle);
    m_preread_values.push_back(arr1d);
    m_preread_value_map[name] = m_preread_values.size() - 1;
    return m_preread_values.back();
  } else {
    return m_preread_values[pvm->second];
  }
}

std::vector<std::vector<double>> Grib::getGribArray2d(const std::string &name) {
  return mapTo2d(this->getGribArray1d(name), ni(), nj());
}

std::vector<std::vector<double>> Grib::mapTo2d(const std::vector<double> &v,
                                               size_t ni, size_t nj) {
  std::vector<std::vector<double>> arr2d(ni);
  for (size_t i = 0; i < ni; ++i) {
    arr2d[i] = std::vector<double>(nj, 0.0);
  }

  for (size_t i = 0; i < v.size(); ++i) {
    arr2d[i / nj][i % nj] = v[i];
  }
  return arr2d;
}

const std::vector<double> &Grib::latitude1d() const { return m_latitude; }

const std::vector<double> &Grib::longitude1d() const { return m_longitude; }

std::vector<std::vector<double>> Grib::longitude2d() {
  return mapTo2d(this->m_longitude, ni(), nj());
}

std::vector<std::vector<double>> Grib::latitude2d() {
  return mapTo2d(this->m_latitude, ni(), nj());
}

const Kdtree *Grib::kdtree() const { return this->m_tree.get(); }

size_t Grib::size() const { return m_size; }

long Grib::ni() const { return m_ni; }

long Grib::nj() const { return m_nj; }

void Grib::readCoordinates(codes_handle *handle) {
  if (this->m_latitude.empty()) {
    m_latitude.resize(m_size);
    size_t s = m_size;
    CODES_CHECK(
        codes_get_double_array(handle, "latitudes", m_latitude.data(), &s),
        nullptr);
  }
  if (this->m_longitude.empty()) {
    m_longitude.resize(m_size);
    size_t s = m_size;
    CODES_CHECK(
        codes_get_double_array(handle, "longitudes", m_longitude.data(), &s),
        nullptr);
    if (m_convention == 0) {
      for (auto &v : m_longitude) {
        v = (std::fmod(v + 180.0, 360.0)) - 180.0;
      }
    }
  }
}

std::tuple<size_t, size_t> Grib::indexToPair(const size_t index) const {
  size_t j = index % nj();
  size_t i = index / nj();
  return std::make_pair(i, j);
}

bool Grib::point_inside(const Point &p) const {
  return this->m_geometry->is_inside(p);
}

void Grib::findCorners() {
  double xtl =
      *(std::min_element(m_longitude.begin(), m_longitude.begin() + m_ni - 1));
  double xtr =
      *(std::max_element(m_longitude.begin(), m_longitude.begin() + m_ni - 1));
  double xll = *(std::min_element(m_longitude.end() - m_ni, m_longitude.end()));
  double xlr = *(std::max_element(m_longitude.end() - m_ni, m_longitude.end()));

  double ytl =
      *(std::min_element(m_latitude.begin(), m_latitude.begin() + m_ni - 1));
  double ytr =
      *(std::max_element(m_latitude.begin(), m_latitude.begin() + m_ni - 1));
  double yll = *(std::min_element(m_latitude.end() - m_ni, m_latitude.end()));
  double ylr = *(std::max_element(m_latitude.end() - m_ni, m_latitude.end()));

  m_corners = {Point(xll, yll), Point(xlr, ylr), Point(xtr, ytr),
               Point(xtl, ytl)};
  m_geometry = std::make_unique<Geometry>(m_corners);
}