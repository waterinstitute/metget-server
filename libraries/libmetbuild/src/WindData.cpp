//
// Created by Zach Cobell on 1/21/21.
//

#include "WindData.h"
#include <cassert>

using namespace MetBuild;

WindData::WindData(size_t ni, size_t nj)
    : m_ni(ni), m_nj(nj), m_u(nj, std::vector<double>(ni, 0.0)), m_v(nj, std::vector<double>(ni, 0.0)),
      m_p(nj, std::vector<double>(ni, background_pressure())) {}

const std::vector<std::vector<double>>& WindData::u() const { return m_u; }

const std::vector<std::vector<double>>& WindData::v() const { return m_v; }

const std::vector<std::vector<double>>& WindData::p() const { return m_p; }

void WindData::setU(size_t i, size_t j, double value){
  assert(i < m_ni);
  assert(j < m_nj);
  m_u[j][i] = value;
}

void WindData::setV(size_t i, size_t j, double value){
  assert(i < m_ni);
  assert(j < m_nj);
  m_v[j][i] = value;
}

void WindData::setP(size_t i, size_t j, double value){
  assert(i < m_ni);
  assert(j < m_nj);
  m_p[j][i] = value;
}
