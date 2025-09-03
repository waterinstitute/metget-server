#pragma once

#include <algorithm>
#include <array>
#include <ranges>

namespace FastTri {
/**
 * @class InterpolationWeight
 * @brief Barycentric interpolation weights for a point in a triangle
 *
 * Contains the vertex indices and corresponding barycentric weights
 * for interpolation at a query point.
 */
class InterpolationWeight {
 public:
  /**
   * @brief Default constructor creates an invalid weight
   */
  constexpr InterpolationWeight() noexcept
      : m_vertices{0, 0, 0}, m_weights{0.0, 0.0, 0.0}, m_valid(false) {}

  /**
   * @brief Constructs a valid interpolation weight
   * @param vertices_ Triangle vertex indices
   * @param weights_ Barycentric weights (must sum to 1.0)
   */
  constexpr InterpolationWeight(const std::array<unsigned, 3> &vertices_,
                                const std::array<double, 3> &weights_) noexcept
      : m_vertices(vertices_), m_weights(weights_), m_valid(true) {}

  [[nodiscard]] constexpr auto vertices() const noexcept
      -> const std::array<unsigned, 3> & {
    return m_vertices;
  }

  [[nodiscard]] constexpr auto weights() const noexcept
      -> const std::array<double, 3> & {
    return m_weights;
  }

  [[nodiscard]] constexpr auto valid() const noexcept -> bool {
    return m_valid;
  }

  [[nodiscard]] static auto interpolate(
      const std::vector<InterpolationWeight> &weights,
      const std::vector<double> &values) -> std::vector<double> {
    std::vector<double> results;
    results.reserve(weights.size());
    std::ranges::transform(
        weights, std::back_inserter(results), [&](const auto &this_weight) {
          if (!this_weight.valid()) {
            return std::numeric_limits<double>::quiet_NaN();
          }
          return (this_weight.weights()[0] *
                  values[this_weight.vertices()[0]]) +
                 (this_weight.weights()[1] *
                  values[this_weight.vertices()[1]]) +
                 (this_weight.weights()[2] * values[this_weight.vertices()[2]]);
        });
    return results;
  }

 private:
  std::array<unsigned, 3> m_vertices;  ///< Indices of the three vertices
  std::array<double, 3> m_weights;     ///< Barycentric weights (sum to 1.0)
  bool m_valid;  ///< True if point is inside triangulation
};
}  // namespace FastTri
