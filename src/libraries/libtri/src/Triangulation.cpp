#include "Triangulation.h"

#include <CGAL/Barycentric_coordinates_2/triangle_coordinates_2.h>
#include <CGAL/mark_domain_in_triangulation.h>

#include <algorithm>
#include <ranges>
#include <stdexcept>
#include <unordered_map>

namespace {

/**
 * @brief Marks the domain status for triangulation faces
 * @param triangulation Pointer to the CDT triangulation object
 *
 * This function marks faces as either inside or outside the domain
 * based on the constraint polygons applied to the triangulation.
 */
void mark_domain_status(CxxTri::Triangulation::t_CDT *triangulation) {
  std::unordered_map<CxxTri::Triangulation::t_Face_handle, bool> in_domain_map;
  in_domain_map.reserve(triangulation->number_of_faces());
  boost::associative_property_map in_domain(in_domain_map);

  CGAL::mark_domain_in_triangulation(*triangulation, in_domain);
  for (auto fit = triangulation->finite_faces_begin();
       fit != triangulation->finite_faces_end(); ++fit) {
    auto it = in_domain_map.find(fit);
    fit->info().set_in_domain(it != in_domain_map.end() && it->second);
  }
}

/**
 * @brief Adds a collection of mesh points to the triangulation
 * @param triangulation Pointer to the CDT triangulation object
 * @param points Vector of points to be added to the triangulation
 *
 * This function inserts mesh points along with their indices as info
 * into the triangulation data structure.
 */
void add_mesh_points(
    CxxTri::Triangulation::t_CDT *triangulation,
    const std::vector<CxxTri::Triangulation::t_Point> &points) {
  std::vector<std::pair<CxxTri::Triangulation::t_Point, unsigned>> pts_vec;
  pts_vec.reserve(points.size());
  std::ranges::transform(
      std::views::iota(unsigned{0}, static_cast<unsigned>(points.size())),
      std::back_inserter(pts_vec), [&](const auto pt_idx) {
        return std::make_pair(points[pt_idx], pt_idx);
      });
  triangulation->insert(pts_vec.begin(), pts_vec.end());
}

/**
 * @brief Constructs CGAL points from separate x and y coordinate vectors
 * @param points_x Vector of x-coordinates
 * @param points_y Vector of y-coordinates
 * @return Vector of CGAL Point_2 objects
 * @throws std::invalid_argument if x and y vectors have different sizes
 */
std::vector<CxxTri::Triangulation::t_Point> construct_points(
    const std::vector<double> &points_x, const std::vector<double> &points_y) {
  if (points_x.size() != points_y.size()) {
    throw std::invalid_argument(
        "The number of x and y coordinates must be the same.");
  }

  std::vector<CxxTri::Triangulation::t_Point> points;
  points.reserve(points_x.size());
  std::ranges::transform(
      std::views::iota(unsigned{0}, static_cast<unsigned>(points_x.size())),
      std::back_inserter(points), [&](const auto pt_idx) {
        return CxxTri::Triangulation::t_Point{points_x[pt_idx],
                                              points_y[pt_idx]};
      });
  return points;
}

/**
 * @brief Constructs a constrained Delaunay triangulation from x,y coordinates
 * @param x Vector of x-coordinates
 * @param y Vector of y-coordinates
 * @return Unique pointer to the constructed CDT object
 * @throws std::invalid_argument if fewer than 3 points are provided
 */
auto construct_triangulation(const std::vector<double> &x,
                             const std::vector<double> &y)
    -> std::unique_ptr<CxxTri::Triangulation::t_CDT> {
  if (x.size() < 3) {
    throw std::invalid_argument(
        "At least 3 points are required for triangulation.");
  }

  const auto pts = construct_points(x, y);
  auto triangulation = std::make_unique<CxxTri::Triangulation::t_CDT>();
  add_mesh_points(triangulation.get(), pts);
  return triangulation;
}

}  // namespace

namespace CxxTri {
/**
 * @brief Constructs a Triangulation object from x and y coordinate vectors
 * @param points_x Vector of x-coordinates for the triangulation vertices
 * @param points_y Vector of y-coordinates for the triangulation vertices
 * @throws std::invalid_argument if vectors have different sizes or fewer than 3
 * points
 */
Triangulation::Triangulation(const std::vector<double> &points_x,
                             const std::vector<double> &points_y)
    : m_triangulation(construct_triangulation(points_x, points_y)) {}

/**
 * @brief Applies a constraint polygon to the triangulation using coordinate
 * vectors
 * @param region_x Vector of x-coordinates defining the constraint polygon
 * @param region_y Vector of y-coordinates defining the constraint polygon
 * @throws std::invalid_argument if x and y vectors have different sizes
 *
 * This method inserts a constraint polygon into the triangulation and marks
 * the domain status of faces accordingly.
 */
void Triangulation::apply_constraint_polygon(
    const std::vector<double> &region_x, const std::vector<double> &region_y) {
  std::vector<t_Point> region;
  region.reserve(region_x.size());
  if (region_x.size() != region_y.size()) {
    throw std::invalid_argument(
        "The number of x and y coordinates must be the same.");
  }
  std::ranges::transform(
      std::views::iota(unsigned{0}, static_cast<unsigned>(region_x.size())),
      std::back_inserter(region), [&](const auto pt_idx) {
        return t_Point{region_x[pt_idx], region_y[pt_idx]};
      });
  this->apply_constraint_polygon(region);
}

/**
 * @brief Applies a constraint polygon to the triangulation using CGAL points
 * @param region Vector of CGAL points defining the constraint polygon
 *
 * This method converts the point vector to a polygon and applies it as a
 * constraint.
 */
void Triangulation::apply_constraint_polygon(
    const std::vector<t_Point> &region) {
  const auto polygon = t_Polygon(region.begin(), region.end());
  this->apply_constraint_polygon(polygon);
}

/**
 * @brief Applies a CGAL polygon as a constraint to the triangulation
 * @param poly CGAL polygon to be applied as constraint
 * @throws std::invalid_argument if polygon has fewer than 3 vertices
 *
 * This method inserts the polygon vertices as a closed constraint and updates
 * the domain marking for all triangulation faces.
 */
void Triangulation::apply_constraint_polygon(const t_Polygon &poly) {
  if (poly.size() < 3) {
    throw std::invalid_argument(
        "At least 3 points are required for a constraint polygon.");
  }
  m_triangulation->insert_constraint(poly.vertices_begin(), poly.vertices_end(),
                                     true);
  mark_domain_status(m_triangulation.get());
}

/**
 * @brief Computes barycentric interpolation weights for a query point
 * @param pt_x X-coordinate of the query point
 * @param pt_y Y-coordinate of the query point
 * @return InterpolationWeight structure containing vertex indices and weights
 *
 * This method locates the triangle containing the query point and computes
 * barycentric coordinates. Returns an invalid weight if point is outside
 * triangulation.
 */
auto Triangulation::get_interpolation_weight(const double pt_x,
                                             const double pt_y) const
    -> t_InterpolationWeight {
  using t_CartesianKernel = CGAL::Simple_cartesian<double>;
  using t_FT = t_CartesianKernel::FT;
  using t_Pt2 = t_CartesianKernel::Point_2;

  auto locate_type = t_CDT::OUTSIDE_AFFINE_HULL;
  int dmy = 0;
  const auto face_handle =
      m_triangulation->locate({pt_x, pt_y}, locate_type, dmy);
  if (locate_type == t_CDT::FACE || locate_type == t_CDT::EDGE ||
      locate_type == t_CDT::VERTEX) {
    const auto pt_query = t_Pt2{pt_x, pt_y};
    const auto vertex_0 = t_Pt2(face_handle->vertex(0)->point().x(),
                                face_handle->vertex(0)->point().y());
    const auto vertex_1 = t_Pt2(face_handle->vertex(1)->point().x(),
                                face_handle->vertex(1)->point().y());
    const auto vertex_2 = t_Pt2(face_handle->vertex(2)->point().x(),
                                face_handle->vertex(2)->point().y());

    std::array<t_FT, 3> result{};
    const std::array vertices = {face_handle->vertex(0)->info(),
                                 face_handle->vertex(1)->info(),
                                 face_handle->vertex(2)->info()};
    CGAL::Barycentric_coordinates::triangle_coordinates_2(
        vertex_0, vertex_1, vertex_2, pt_query, result.data());
    return t_InterpolationWeight(
        vertices, {CGAL::to_double(result[0]), CGAL::to_double(result[1]),
                   CGAL::to_double(result[2])});
  }
  return {};
}

/**
 * @brief Computes interpolation weights for multiple query points
 * @param points_x Vector of x-coordinates for query points
 * @param points_y Vector of y-coordinates for query points
 * @return Vector of InterpolationWeight structures
 *
 * This method batch-processes multiple points and returns their corresponding
 * interpolation weights for efficient interpolation operations.
 */
auto Triangulation::get_interpolation_weights(
    const std::vector<double> &points_x,
    const std::vector<double> &points_y) const
    -> std::vector<t_InterpolationWeight> {
  assert(points_x.size() == points_y.size());
  std::vector<t_InterpolationWeight> weights;
  weights.reserve(points_x.size());
  std::ranges::transform(
      std::views::iota(unsigned{0}, static_cast<unsigned>(points_x.size())),
      std::back_inserter(weights), [&](const auto pt_idx) {
        return this->get_interpolation_weight(points_x[pt_idx],
                                              points_y[pt_idx]);
      });
  return weights;
}

/**
 * @brief Retrieves all triangles that are inside the domain
 * @return Vector of Triangle structures containing vertex indices
 *
 * This method iterates through all finite faces in the triangulation and
 * returns only those marked as being inside the domain constraints.
 */
std::vector<Triangulation::t_Triangle> Triangulation::get_triangles() const {
  std::vector<t_Triangle> triangles;

  // Use the vertex info() which already contains the index
  for (auto fit = m_triangulation->finite_faces_begin();
       fit != m_triangulation->finite_faces_end(); ++fit) {
    if (fit->info().is_in_domain()) {
      triangles.push_back(
          t_Triangle{{fit->vertex(0)->info(), fit->vertex(1)->info(),
                      fit->vertex(2)->info()}});
    }
  }

  return triangles;
}

/**
 * @brief Retrieves all vertices in the triangulation
 * @return Vector of CGAL points representing the triangulation vertices
 *
 * This method returns all finite vertices in the triangulation, ordered by
 * their vertex info index for consistent ordering.
 */
auto Triangulation::get_vertices() const -> std::vector<t_Point> {
  const auto num_vertices = static_cast<size_t>(
      std::distance(m_triangulation->finite_vertices_begin(),
                    m_triangulation->finite_vertices_end()));

  std::vector<t_Point> vertices(num_vertices);
  for (auto vit = m_triangulation->finite_vertices_begin();
       vit != m_triangulation->finite_vertices_end(); ++vit) {
    vertices[vit->info()] = t_Point{vit->point().x(), vit->point().y()};
  }

  return vertices;
}

}  // namespace CxxTri
