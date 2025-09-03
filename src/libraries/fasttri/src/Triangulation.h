#pragma once

#include <CGAL/Constrained_Delaunay_triangulation_2.h>
#include <CGAL/Constrained_triangulation_face_base_2.h>
#include <CGAL/Exact_predicates_inexact_constructions_kernel.h>
#include <CGAL/Polygon_2.h>
#include <CGAL/Triangulation_face_base_with_info_2.h>
#include <CGAL/Triangulation_vertex_base_with_info_2.h>

#include <array>
#include <memory>
#include <vector>

#include "FaceInfo2.h"
#include "InterpolationWeight.h"

namespace FastTri {

/**
 * @class Triangulation
 * @brief Constrained Delaunay Triangulation with domain marking and
 * interpolation
 *
 * This class provides a wrapper around CGAL's Constrained Delaunay
 * Triangulation functionality, with support for constraint polygons, domain
 * marking, and barycentric interpolation weight computation.
 */
class Triangulation {
 public:
  /**
   * @struct t_Triangle
   * @brief Represents a triangle using vertex indices
   */
  struct t_Triangle {
    std::array<unsigned, 3>
        vertices;  ///< Indices of the three triangle vertices
  };

  using t_Kernel = CGAL::Exact_predicates_inexact_constructions_kernel;
  using t_Vb = CGAL::Triangulation_vertex_base_with_info_2<unsigned, t_Kernel>;
  using t_Fbi = CGAL::Triangulation_face_base_with_info_2<FaceInfo2, t_Kernel>;
  using t_Fb = CGAL::Constrained_triangulation_face_base_2<t_Kernel, t_Fbi>;
  using t_Tds = CGAL::Triangulation_data_structure_2<t_Vb, t_Fb>;
  using t_CDT = CGAL::Constrained_Delaunay_triangulation_2<t_Kernel, t_Tds>;
  using t_Point = t_Kernel::Point_2;
  using t_Vertex_handle = t_CDT::Vertex_handle;
  using t_Face_handle = t_CDT::Face_handle;
  using t_Edge = t_CDT::Edge;
  using t_Polygon = CGAL::Polygon_2<t_Kernel>;

  /**
   * @brief Constructs a triangulation from x and y coordinate vectors
   * @param points_x Vector of x-coordinates for triangulation vertices
   * @param points_y Vector of y-coordinates for triangulation vertices
   * @throws std::invalid_argument if vectors have different sizes or fewer than
   * 3 points
   */
  Triangulation(const std::vector<double> &points_x,
                const std::vector<double> &points_y);

  /**
   * @brief Applies a constraint polygon using coordinate vectors
   * @param region_x X-coordinates of polygon vertices
   * @param region_y Y-coordinates of polygon vertices
   * @throws std::invalid_argument if coordinate vectors have different sizes
   *
   * The polygon will be inserted as a closed constraint and domain
   * marking will be updated.
   */
  void apply_constraint_polygon(const std::vector<double> &region_x,
                                const std::vector<double> &region_y);

  /**
   * @brief Applies a constraint polygon using CGAL points
   * @param region Vector of polygon vertices as CGAL points
   */
  void apply_constraint_polygon(const std::vector<t_Point> &region);

  /**
   * @brief Applies a CGAL polygon as a constraint
   * @param poly CGAL polygon to use as constraint
   * @throws std::invalid_argument if polygon has fewer than 3 vertices
   */
  void apply_constraint_polygon(const t_Polygon &poly);

  /**
   * @brief Computes interpolation weights for a single query point
   * @param point Query point as a CGAL point
   * @return Interpolation weight structure (invalid if point outside
   * triangulation)
   */
  [[nodiscard]] auto get_interpolation_weight(t_Point point) const
      -> InterpolationWeight;

  /**
   * @brief Computes interpolation weights for a single query point
   * @param pt_x X-coordinate of the query point
   * @param pt_y Y-coordinate of the query point
   * @return Interpolation weight structure (invalid if point outside
   * triangulation)
   */
  [[nodiscard]] auto get_interpolation_weight(double pt_x, double pt_y) const
      -> InterpolationWeight;

  /**
   * @brief Computes interpolation weights for multiple query points
   * @param points Vector of query points as CGAL points
   * @return Vector of interpolation weights for each query point
   */
  [[nodiscard]] auto get_interpolation_weights(
      const std::vector<t_Point> &points) const
      -> std::vector<InterpolationWeight>;

  /**
   * @brief Computes interpolation weights for multiple query points
   * @param points_x X-coordinates of query points
   * @param points_y Y-coordinates of query points
   * @return Vector of interpolation weights for each query point
   */
  [[nodiscard]] auto get_interpolation_weights(
      const std::vector<double> &points_x,
      const std::vector<double> &points_y) const
      -> std::vector<InterpolationWeight>;

  /**
   * @brief Retrieves all triangles inside the domain
   * @return Vector of triangles with vertex indices
   */
  [[nodiscard]] auto get_triangles() const -> std::vector<t_Triangle>;

  /**
   * @brief Retrieves all vertices in the triangulation
   * @return Vector of vertex positions ordered by vertex index
   */
  [[nodiscard]] auto get_vertices() const -> std::vector<t_Point>;

 private:
  std::unique_ptr<t_CDT>
      m_triangulation;  ///< CGAL triangulation data structure

  struct t_LookupHint {
    t_Face_handle face;
    bool has_hint{false};
  };

  /**
   * @brief Computes interpolation weights for a single query point
   * @param point Query point as a CGAL point
   * @param hint Lookup hint to optimize face search
   * @return Interpolation weight structure (invalid if point outside
   * triangulation)
   */
  [[nodiscard]] auto get_interpolation_weight(t_Point point,
                                              t_LookupHint &hint) const
      -> InterpolationWeight;
};
}  // namespace FastTri
