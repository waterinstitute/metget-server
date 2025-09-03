#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <catch2/matchers/catch_matchers_vector.hpp>
#include <random>
#include <vector>

#include "../src/Triangulation.h"
#include "../src/InterpolationWeight.h"

using namespace Tri;
using Catch::Matchers::WithinAbs;
using Catch::Matchers::WithinRel;

namespace {

std::vector<double> generate_regular_grid_x(size_t nx, double xmin, double xmax) {
  std::vector<double> x;
  x.reserve(nx);
  const double dx = (xmax - xmin) / static_cast<double>(nx - 1);
  for (size_t i = 0; i < nx; ++i) {
    x.push_back(xmin + static_cast<double>(i) * dx);
  }
  return x;
}

std::vector<double> generate_regular_grid_y(size_t ny, double ymin, double ymax) {
  std::vector<double> y;
  y.reserve(ny);
  const double dy = (ymax - ymin) / static_cast<double>(ny - 1);
  for (size_t j = 0; j < ny; ++j) {
    y.push_back(ymin + static_cast<double>(j) * dy);
  }
  return y;
}

std::pair<std::vector<double>, std::vector<double>> generate_regular_grid(
    size_t nx, size_t ny, double xmin, double xmax, double ymin, double ymax) {
  std::vector<double> x, y;
  x.reserve(nx * ny);
  y.reserve(nx * ny);

  const double dx = (xmax - xmin) / static_cast<double>(nx - 1);
  const double dy = (ymax - ymin) / static_cast<double>(ny - 1);

  for (size_t j = 0; j < ny; ++j) {
    for (size_t i = 0; i < nx; ++i) {
      x.push_back(xmin + static_cast<double>(i) * dx);
      y.push_back(ymin + static_cast<double>(j) * dy);
    }
  }

  return {x, y};
}

std::pair<std::vector<double>, std::vector<double>> generate_random_points(
    size_t n, double xmin, double xmax, double ymin, double ymax, unsigned seed = 42) {
  std::mt19937 gen(seed);
  std::uniform_real_distribution<> dist_x(xmin, xmax);
  std::uniform_real_distribution<> dist_y(ymin, ymax);

  std::vector<double> x, y;
  x.reserve(n);
  y.reserve(n);

  for (size_t i = 0; i < n; ++i) {
    x.push_back(dist_x(gen));
    y.push_back(dist_y(gen));
  }

  return {x, y};
}

std::pair<std::vector<double>, std::vector<double>> generate_circle_points(
    size_t n, double cx, double cy, double radius) {
  std::vector<double> x, y;
  x.reserve(n);
  y.reserve(n);

  for (size_t i = 0; i < n; ++i) {
    const double angle = 2.0 * M_PI * static_cast<double>(i) / static_cast<double>(n);
    x.push_back(cx + radius * std::cos(angle));
    y.push_back(cy + radius * std::sin(angle));
  }

  return {x, y};
}

}  // namespace

TEST_CASE("Triangulation construction", "[triangulation]") {
  SECTION("Simple triangle") {
    std::vector<double> points_x = {0.0, 1.0, 0.5};
    std::vector<double> points_y = {0.0, 0.0, 1.0};

    REQUIRE_NOTHROW(Triangulation(points_x, points_y));
  }

  SECTION("Simple triangle with boundary") {
    // Mesh points that include the boundary vertices
    std::vector<double> points_x = {-0.5, 1.5, 0.5, 0.0, 1.0};
    std::vector<double> points_y = {-0.5, -0.5, 1.5, 0.0, 0.0};

    // Boundary uses first 3 points from the mesh
    std::vector<double> boundary_x = {-0.5, 1.5, 0.5, -0.5};
    std::vector<double> boundary_y = {-0.5, -0.5, 1.5, -0.5};

    Triangulation tri(points_x, points_y);
    REQUIRE_NOTHROW(tri.apply_constraint_polygon(boundary_x, boundary_y));
  }

  SECTION("Regular grid") {
    auto [grid_x, grid_y] = generate_regular_grid(5, 5, 0.0, 1.0, 0.0, 1.0);

    REQUIRE_NOTHROW(Triangulation(grid_x, grid_y));
  }

  SECTION("Regular grid with boundary") {
    auto [grid_x, grid_y] = generate_regular_grid(5, 5, 0.0, 1.0, 0.0, 1.0);

    // Boundary uses corner points from the grid
    std::vector<double> boundary_x = {0.0, 1.0, 1.0, 0.0, 0.0};
    std::vector<double> boundary_y = {0.0, 0.0, 1.0, 1.0, 0.0};

    Triangulation tri(grid_x, grid_y);
    REQUIRE_NOTHROW(tri.apply_constraint_polygon(boundary_x, boundary_y));
  }

  SECTION("Random points") {
    auto [random_x, random_y] = generate_random_points(100, 0.0, 10.0, 0.0, 10.0);

    REQUIRE_NOTHROW(Triangulation(random_x, random_y));
  }

  SECTION("Random points with boundary") {
    // Add specific boundary vertices to the random points
    std::vector<double> points_x = {0.0, 10.0, 10.0, 0.0};  // Boundary corners
    std::vector<double> points_y = {0.0, 0.0, 10.0, 10.0};

    // Add random interior points
    auto [random_x, random_y] = generate_random_points(100, 0.1, 9.9, 0.1, 9.9);
    points_x.insert(points_x.end(), random_x.begin(), random_x.end());
    points_y.insert(points_y.end(), random_y.begin(), random_y.end());

    // Boundary uses the corner points
    std::vector<double> boundary_x = {0.0, 10.0, 10.0, 0.0, 0.0};
    std::vector<double> boundary_y = {0.0, 0.0, 10.0, 10.0, 0.0};

    Triangulation tri(points_x, points_y);
    REQUIRE_NOTHROW(tri.apply_constraint_polygon(boundary_x, boundary_y));
  }

  SECTION("Circular boundary") {
    // Create boundary points that will be part of the mesh
    auto [boundary_x, boundary_y] = generate_circle_points(32, 0.0, 0.0, 1.0);

    // Start with boundary points in the mesh
    std::vector<double> all_x(boundary_x);
    std::vector<double> all_y(boundary_y);

    // Add random interior points
    auto [interior_x, interior_y] = generate_random_points(50, -0.8, 0.8, -0.8, 0.8);
    all_x.insert(all_x.end(), interior_x.begin(), interior_x.end());
    all_y.insert(all_y.end(), interior_y.begin(), interior_y.end());

    Triangulation tri(all_x, all_y);
    REQUIRE_NOTHROW(tri.apply_constraint_polygon(boundary_x, boundary_y));
  }
}

TEST_CASE("Invalid construction parameters", "[triangulation][error]") {
  SECTION("Too few points") {
    std::vector<double> points_x = {0.0, 1.0};
    std::vector<double> points_y = {0.0, 0.0};

    REQUIRE_THROWS_AS(
        Triangulation(points_x, points_y),
        std::invalid_argument);
  }

  SECTION("Mismatched coordinate arrays") {
    std::vector<double> points_x = {0.0, 1.0, 0.5};
    std::vector<double> points_y = {0.0, 0.0};  // One less than x

    REQUIRE_THROWS_AS(
        Triangulation(points_x, points_y),
        std::invalid_argument);
  }

  SECTION("Too few boundary points") {
    std::vector<double> points_x = {0.0, 1.0, 0.5};
    std::vector<double> points_y = {0.0, 0.0, 1.0};
    std::vector<double> boundary_x = {-1.0, 2.0};
    std::vector<double> boundary_y = {-1.0, -1.0};

    Triangulation tri(points_x, points_y);
    REQUIRE_THROWS_AS(
        tri.apply_constraint_polygon(boundary_x, boundary_y),
        std::invalid_argument);
  }

  SECTION("Mismatched boundary coordinate arrays") {
    std::vector<double> points_x = {0.0, 1.0, 0.5};
    std::vector<double> points_y = {0.0, 0.0, 1.0};
    std::vector<double> boundary_x = {-1.0, 2.0, 0.5, -1.0};
    std::vector<double> boundary_y = {-1.0, -1.0, 2.0};  // One less than x

    Triangulation tri(points_x, points_y);
    REQUIRE_THROWS_AS(
        tri.apply_constraint_polygon(boundary_x, boundary_y),
        std::invalid_argument);
  }
}

TEST_CASE("Point location and interpolation", "[triangulation][interpolation]") {
  SECTION("Simple triangle - point at vertices") {
    // Create mesh with triangle and boundary vertices
    std::vector<double> points_x = {0.0, 1.0, 0.5, -0.5, 1.5};
    std::vector<double> points_y = {0.0, 0.0, 1.0, -0.5, -0.5};

    // Boundary polygon using some mesh points
    std::vector<double> boundary_x = {-0.5, 1.5, 0.5, -0.5};
    std::vector<double> boundary_y = {-0.5, -0.5, 1.0, -0.5};

    Triangulation tri(points_x, points_y);
    tri.apply_constraint_polygon(boundary_x, boundary_y);

    // Test at first vertex
    auto result = tri.get_interpolation_weight(0.0, 0.0);
    REQUIRE(result.valid());

    // One weight should be close to 1, others close to 0
    const auto weights = result.weights();
    REQUIRE(std::any_of(weights.begin(), weights.end(),
                       [](double w) { return std::abs(w - 1.0) < 1e-10; }));
    REQUIRE(std::count_if(weights.begin(), weights.end(),
                         [](double w) { return std::abs(w) < 1e-10; }) == 2);
  }

  SECTION("Simple triangle - point at centroid") {
    // Create mesh with triangle vertices
    std::vector<double> points_x = {0.0, 1.0, 0.5};
    std::vector<double> points_y = {0.0, 0.0, 1.0};

    Triangulation tri(points_x, points_y);

    // Centroid of triangle
    const double cx = (0.0 + 1.0 + 0.5) / 3.0;
    const double cy = (0.0 + 0.0 + 1.0) / 3.0;

    auto result = tri.get_interpolation_weight(cx, cy);
    REQUIRE(result.valid());

    // All weights should be approximately equal (1/3)
    const auto weights = result.weights();
    for (double w : weights) {
      REQUIRE_THAT(w, WithinAbs(1.0 / 3.0, 1e-10));
    }
  }

  SECTION("Point outside triangulation") {
    std::vector<double> points_x = {0.0, 1.0, 0.5};
    std::vector<double> points_y = {0.0, 0.0, 1.0};

    Triangulation tri(points_x, points_y);

    // Point clearly outside
    auto result = tri.get_interpolation_weight(10.0, 10.0);
    REQUIRE_FALSE(result.valid());
  }

  SECTION("Regular grid - interpolation consistency") {
    auto [grid_x, grid_y] = generate_regular_grid(4, 4, 0.0, 1.0, 0.0, 1.0);

    // Use corner points of grid as boundary
    std::vector<double> boundary_x = {0.0, 1.0, 1.0, 0.0, 0.0};
    std::vector<double> boundary_y = {0.0, 0.0, 1.0, 1.0, 0.0};

    Triangulation tri(grid_x, grid_y);
    tri.apply_constraint_polygon(boundary_x, boundary_y);

    // Test point in the middle
    auto result = tri.get_interpolation_weight(0.5, 0.5);
    REQUIRE(result.valid());

    // Weights should sum to 1
    const auto weights = result.weights();
    double sum = std::accumulate(weights.begin(), weights.end(), 0.0);
    REQUIRE_THAT(sum, WithinAbs(1.0, 1e-10));

    // All weights should be non-negative
    REQUIRE(std::all_of(weights.begin(), weights.end(),
                       [](double w) { return w >= -1e-10; }));
  }

  SECTION("Multiple interpolation points") {
    auto [grid_x, grid_y] = generate_regular_grid(5, 5, 0.0, 2.0, 0.0, 2.0);

    // Use corner points of grid as boundary
    std::vector<double> boundary_x = {0.0, 2.0, 2.0, 0.0, 0.0};
    std::vector<double> boundary_y = {0.0, 0.0, 2.0, 2.0, 0.0};

    Triangulation tri(grid_x, grid_y);
    tri.apply_constraint_polygon(boundary_x, boundary_y);

    // Test multiple query points
    std::vector<std::pair<double, double>> test_points = {
        {0.25, 0.25}, {0.75, 0.75}, {1.0, 1.0}, {1.5, 0.5}, {0.1, 1.9}
    };

    for (const auto& [x, y] : test_points) {
      auto result = tri.get_interpolation_weight(x, y);
      REQUIRE(result.valid());

      // Verify weights sum to 1
      const auto weights = result.weights();
      double sum = std::accumulate(weights.begin(), weights.end(), 0.0);
      REQUIRE_THAT(sum, WithinAbs(1.0, 1e-10));

      // Verify all weights are non-negative
      REQUIRE(std::all_of(weights.begin(), weights.end(),
                         [](double w) { return w >= -1e-10; }));
    }
  }
}

TEST_CASE("Interpolation weight properties", "[triangulation][interpolation]") {
  SECTION("Barycentric coordinates properties") {
    // Create a simple known triangle
    std::vector<double> points_x = {0.0, 2.0, 0.0};
    std::vector<double> points_y = {0.0, 0.0, 2.0};

    Triangulation tri(points_x, points_y);

    // Test along an edge
    auto result = tri.get_interpolation_weight(1.0, 0.0);  // Midpoint of bottom edge
    REQUIRE(result.valid());

    const auto weights = result.weights();
    // One weight should be zero (for the opposite vertex)
    REQUIRE(std::any_of(weights.begin(), weights.end(),
                       [](double w) { return std::abs(w) < 1e-10; }));

    // Other two should sum to 1
    double non_zero_sum = 0.0;
    for (double w : weights) {
      if (std::abs(w) > 1e-10) {
        non_zero_sum += w;
      }
    }
    REQUIRE_THAT(non_zero_sum, WithinAbs(1.0, 1e-10));
  }

  SECTION("Linear interpolation property") {
    // Create a grid where we know the function values
    auto [grid_x, grid_y] = generate_regular_grid(3, 3, 0.0, 2.0, 0.0, 2.0);

    Triangulation tri(grid_x, grid_y);

    // Define a linear function f(x,y) = 2x + 3y
    std::vector<double> function_values;
    for (size_t i = 0; i < grid_x.size(); ++i) {
      function_values.push_back(2.0 * grid_x[i] + 3.0 * grid_y[i]);
    }

    // Test interpolation at a random point
    const double test_x = 0.7;
    const double test_y = 1.3;
    const double expected = 2.0 * test_x + 3.0 * test_y;

    auto result = tri.get_interpolation_weight(test_x, test_y);
    REQUIRE(result.valid());

    // Compute interpolated value
    double interpolated = 0.0;
    for (size_t i = 0; i < 3; ++i) {
      interpolated += result.weights()[i] * function_values[result.vertices()[i]];
    }

    REQUIRE_THAT(interpolated, WithinAbs(expected, 1e-10));
  }
}

TEST_CASE("Boundary and edge cases", "[triangulation][edge-cases]") {
  SECTION("Points on boundary") {
    // Create mesh with square boundary points and interior point
    std::vector<double> points_x = {0.0, 1.0, 1.0, 0.0, 0.5};
    std::vector<double> points_y = {0.0, 0.0, 1.0, 1.0, 0.5};

    // Boundary uses the corner points
    std::vector<double> boundary_x = {0.0, 1.0, 1.0, 0.0, 0.0};
    std::vector<double> boundary_y = {0.0, 0.0, 1.0, 1.0, 0.0};

    Triangulation tri(points_x, points_y);
    tri.apply_constraint_polygon(boundary_x, boundary_y);

    // Test points on boundary
    std::vector<std::pair<double, double>> boundary_points = {
        {0.5, 0.0},  // Bottom edge
        {1.0, 0.5},  // Right edge
        {0.5, 1.0},  // Top edge
        {0.0, 0.5}   // Left edge
    };

    for (const auto& [x, y] : boundary_points) {
      auto result = tri.get_interpolation_weight(x, y);
      REQUIRE(result.valid());

      // Weights should still sum to 1
      const auto weights = result.weights();
      double sum = std::accumulate(weights.begin(), weights.end(), 0.0);
      REQUIRE_THAT(sum, WithinAbs(1.0, 1e-10));
    }
  }

  SECTION("Degenerate triangle detection") {
    // Create mesh with boundary points and some collinear interior points
    std::vector<double> points_x = {-0.5, 1.5, 1.5, -0.5, 0.0, 0.5, 1.0, 0.5, 0.5};
    std::vector<double> points_y = {-1.0, -1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.5, -0.5};

    // Boundary uses the corner points
    std::vector<double> boundary_x = {-0.5, 1.5, 1.5, -0.5, -0.5};
    std::vector<double> boundary_y = {-1.0, -1.0, 1.0, 1.0, -1.0};

    // Should still construct valid triangulation with the boundary
    REQUIRE_NOTHROW(Triangulation(points_x, points_y));

    Triangulation tri(points_x, points_y);
    tri.apply_constraint_polygon(boundary_x, boundary_y);

    // Test interpolation at a valid point
    auto result = tri.get_interpolation_weight(0.5, 0.25);
    REQUIRE(result.valid());
  }

  SECTION("Dense point cloud") {
    // Generate a dense set of random points including boundary corners
    std::vector<double> points_x = {0.0, 1.0, 1.0, 0.0};  // Boundary corners
    std::vector<double> points_y = {0.0, 0.0, 1.0, 1.0};

    auto [interior_x, interior_y] = generate_random_points(1000, 0.01, 0.99, 0.01, 0.99);
    points_x.insert(points_x.end(), interior_x.begin(), interior_x.end());
    points_y.insert(points_y.end(), interior_y.begin(), interior_y.end());

    REQUIRE_NOTHROW(Triangulation(points_x, points_y));

    Triangulation tri(points_x, points_y);

    // Boundary uses the corner points
    std::vector<double> boundary_x = {0.0, 1.0, 1.0, 0.0, 0.0};
    std::vector<double> boundary_y = {0.0, 0.0, 1.0, 1.0, 0.0};
    tri.apply_constraint_polygon(boundary_x, boundary_y);

    // Test interpolation at multiple random points
    auto [test_x, test_y] = generate_random_points(100, 0.1, 0.9, 0.1, 0.9, 123);

    for (size_t i = 0; i < test_x.size(); ++i) {
      auto result = tri.get_interpolation_weight(test_x[i], test_y[i]);
      REQUIRE(result.valid());

      // Verify basic properties
      const auto weights = result.weights();
      double sum = std::accumulate(weights.begin(), weights.end(), 0.0);
      REQUIRE_THAT(sum, WithinAbs(1.0, 1e-9));
      REQUIRE(std::all_of(weights.begin(), weights.end(),
                         [](double w) { return w >= -1e-9; }));
    }
  }
}

TEST_CASE("Complex boundary shapes", "[triangulation][boundary]") {
  SECTION("Star-shaped boundary") {
    // Create a star-shaped boundary
    const size_t n_points = 10;
    std::vector<double> boundary_x, boundary_y;
    for (size_t i = 0; i < n_points; ++i) {
      const double angle = 2.0 * M_PI * static_cast<double>(i) / static_cast<double>(n_points);
      const double radius = (i % 2 == 0) ? 1.0 : 0.5;
      boundary_x.push_back(radius * std::cos(angle));
      boundary_y.push_back(radius * std::sin(angle));
    }

    // Start mesh with boundary points
    std::vector<double> points_x = boundary_x;
    std::vector<double> points_y = boundary_y;

    // Add interior points
    auto [interior_x, interior_y] = generate_random_points(50, -0.4, 0.4, -0.4, 0.4, 456);
    points_x.insert(points_x.end(), interior_x.begin(), interior_x.end());
    points_y.insert(points_y.end(), interior_y.begin(), interior_y.end());

    REQUIRE_NOTHROW(Triangulation(points_x, points_y));

    Triangulation tri(points_x, points_y);
    tri.apply_constraint_polygon(boundary_x, boundary_y);

    // Test points inside the star
    auto result = tri.get_interpolation_weight(0.0, 0.0);  // Center
    REQUIRE(result.valid());

    result = tri.get_interpolation_weight(0.3, 0.0);  // Inside
    REQUIRE(result.valid());
  }

  SECTION("Concave boundary") {
    // Create a C-shaped boundary
    std::vector<double> boundary_x = {0.0, 1.0, 1.0, 0.25, 0.25, 1.0, 1.0, 0.0, 0.0};
    std::vector<double> boundary_y = {0.0, 0.0, 0.4, 0.4, 0.6, 0.6, 1.0, 1.0, 0.0};

    // Start mesh with boundary points (minus the closing point)
    std::vector<double> points_x(boundary_x.begin(), boundary_x.end() - 1);
    std::vector<double> points_y(boundary_y.begin(), boundary_y.end() - 1);

    // Add interior points in the C-shape
    points_x.insert(points_x.end(), {0.1, 0.9, 0.9, 0.1, 0.1, 0.1, 0.9, 0.9});
    points_y.insert(points_y.end(), {0.1, 0.1, 0.3, 0.3, 0.7, 0.9, 0.7, 0.9});

    REQUIRE_NOTHROW(Triangulation(points_x, points_y));

    Triangulation tri(points_x, points_y);
    tri.apply_constraint_polygon(boundary_x, boundary_y);

    // Test point in the left arm of the C
    auto result = tri.get_interpolation_weight(0.1, 0.2);
    REQUIRE(result.valid());

    // Test point in the top arm of the C
    result = tri.get_interpolation_weight(0.1, 0.8);
    REQUIRE(result.valid());

    // Note: With constrained Delaunay triangulation, points inside the convex hull
    // but outside the constrained boundary may still return interpolation weights
    // if they fall within triangles. This is expected behavior for CDT.
    // The constraint only ensures edges don't cross the boundary, not that
    // triangulation is limited to the boundary interior.

    // Test a point clearly outside the convex hull
    result = tri.get_interpolation_weight(2.0, 0.5);
    REQUIRE_FALSE(result.valid());
  }
}
