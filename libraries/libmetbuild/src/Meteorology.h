////////////////////////////////////////////////////////////////////////////////////
// MIT License
//
// Copyright (c) 2023 The Water Institute
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
// Author: Zachary Cobell
// Contact: zcobell@thewaterinstitute.org
// Organization: The Water Institute
//
////////////////////////////////////////////////////////////////////////////////////
#ifndef METBUILD_METEOROLOGY_H
#define METBUILD_METEOROLOGY_H

#include <memory>
#include <string>

#include "Date.h"
#include "Grid.h"
#include "InterpolationData.h"
#include "MetBuild_Global.h"
#include "MeteorologicalData.h"
#include "data_sources/GriddedData.h"
#include "data_sources/GriddedDataTypes.h"

namespace MetBuild {

class Meteorology {
 public:
  enum SOURCE {
    GFS,
    GEFS,
    NAM,
    HWRF,
    COAMPS,
    HRRR_CONUS,
    HRRR_ALASKA,
    WPC,
    HAFS
  };

  METBUILD_EXPORT explicit Meteorology(const MetBuild::Grid *grid,
                                       Meteorology::SOURCE source_type,
                                       MetBuild::GriddedDataTypes::TYPE type,
                                       bool backfill = false,
                                       int epsg_output = 4326);

  void set_next_file(const std::vector<std::string> &filenames);
  void set_next_file(const std::string &filename);

  int METBUILD_EXPORT process_data();

  int METBUILD_EXPORT write_debug_file(int index) const;

  MetBuild::MeteorologicalData<3, MetBuild::MeteorologicalDataType>
      METBUILD_EXPORT to_wind_grid(double time_weight = 1.0);

  MetBuild::MeteorologicalData<1, MetBuild::MeteorologicalDataType>
      METBUILD_EXPORT to_grid(double time_weight = 1.0);

  static double METBUILD_EXPORT
  generate_time_weight(const MetBuild::Date &t1, const MetBuild::Date &t2,
                       const MetBuild::Date &t_output);

 private:
  constexpr static double epsilon_squared() {
    return std::numeric_limits<double>::epsilon() *
           std::numeric_limits<double>::epsilon();
  }

  static std::unique_ptr<GriddedData> gridded_data_factory(
      const std::vector<std::string> &filenames, Meteorology::SOURCE source);

  MetBuild::Grid::grid reproject_grid(MetBuild::Grid::grid g) const;

  std::tuple<double, double> getScalingRates(
      const GriddedDataTypes::VARIABLES &variable) const;

  constexpr static size_t c_idw_depth = 6;

  static InterpolationWeights generate_interpolation_weight(
      const MetBuild::Triangulation *triangulation,
      const MetBuild::Grid::grid *grid);

  MetBuild::MeteorologicalData<1> scalar_value_interpolation(
      double time_weight);

  static double getPressureScaling(const GriddedData *g);

  static constexpr unsigned typeLengthMap(
      MetBuild::GriddedDataTypes::TYPE type) {
    switch (type) {
      case MetBuild::GriddedDataTypes::RAINFALL:
      case MetBuild::GriddedDataTypes::TEMPERATURE:
      case MetBuild::GriddedDataTypes::HUMIDITY:
      case MetBuild::GriddedDataTypes::ICE:
        return 1;
      case MetBuild::GriddedDataTypes::WIND_PRESSURE:
        return 3;
      default:
        return 1;
    }
  }

  static std::vector<MetBuild::GriddedDataTypes::VARIABLES>
  generate_variable_list(MetBuild::GriddedDataTypes::TYPE type) {
    switch (type) {
      case MetBuild::GriddedDataTypes::WIND_PRESSURE:
        return {MetBuild::GriddedDataTypes::VARIABLES::VAR_PRESSURE,
                MetBuild::GriddedDataTypes::VARIABLES::VAR_U10,
                MetBuild::GriddedDataTypes::VARIABLES::VAR_V10};
      case MetBuild::GriddedDataTypes::RAINFALL:
        return {MetBuild::GriddedDataTypes::VARIABLES::VAR_RAINFALL};
      case MetBuild::GriddedDataTypes::HUMIDITY:
        return {MetBuild::GriddedDataTypes::VARIABLES::VAR_HUMIDITY};
      case MetBuild::GriddedDataTypes::TEMPERATURE:
        return {MetBuild::GriddedDataTypes::VARIABLES::VAR_TEMPERATURE};
      case MetBuild::GriddedDataTypes::ICE:
        return {MetBuild::GriddedDataTypes::VARIABLES::VAR_ICE};
      default:
        return {MetBuild::GriddedDataTypes::VARIABLES::VAR_PRESSURE};
    }
  }

  MetBuild::GriddedDataTypes::TYPE m_type;
  SOURCE m_source;
  const Grid *m_windGrid;
  Grid::grid m_grid_positions;
  std::unique_ptr<GriddedData> m_gridded1;
  std::unique_ptr<GriddedData> m_gridded2;
  double m_rate_scaling_1;
  double m_rate_scaling_2;
  std::shared_ptr<InterpolationData> m_interpolation_1;
  std::shared_ptr<InterpolationData> m_interpolation_2;
  bool m_useBackgroundFlag;
  int m_epsg_output;
  std::vector<MetBuild::GriddedDataTypes::VARIABLES> m_variables;
  std::vector<std::string> m_file1;
  std::vector<std::string> m_file2;
};
}  // namespace MetBuild
#endif  // METBUILD_METEOROLOGY_H
