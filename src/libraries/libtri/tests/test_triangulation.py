"""
Test script for libtri triangulation library.
Generates points, triangulates with a boundary constraint, and visualizes the result.
"""

import matplotlib.pyplot as plt
import matplotlib.tri as mtri
import numpy as np
from libtri import PyTriangulation


def point_in_polygon(x, y, poly_x, poly_y):
    """Check if points are inside a polygon using ray casting algorithm."""
    n = len(poly_x)
    inside = np.zeros(len(x), dtype=bool)

    for i in range(len(x)):
        xi, yi = x[i], y[i]
        count = 0

        for j in range(n):
            j_next = (j + 1) % n
            x1, y1 = poly_x[j], poly_y[j]
            x2, y2 = poly_x[j_next], poly_y[j_next]

            # Check if ray crosses edge
            if ((y1 > yi) != (y2 > yi)) and (
                xi < (x2 - x1) * (yi - y1) / (y2 - y1) + x1
            ):
                count += 1

        inside[i] = (count % 2) == 1

    return inside


def generate_test_data():
    """Generate test points and boundary polygon with curved boundaries."""

    # Create a boundary with:
    # - Concave (inward curving) south side
    # - Convex (outward curving) north side
    # - Straight sides on east and west

    n_boundary = 60  # Number of boundary points for smooth curves

    # Bottom (south) boundary - concave curve
    x_bottom = np.linspace(-2.0, 2.0, n_boundary // 4)
    y_bottom = -1.0 + 0.5 * np.sin(np.pi * (x_bottom + 2.0) / 4.0)  # Concave upward

    # Right (east) boundary - straight line
    y_right = np.linspace(y_bottom[-1], 1.5, n_boundary // 4)
    x_right = np.full_like(y_right, 2.0)

    # Top (north) boundary - convex curve
    x_top = np.linspace(2.0, -2.0, n_boundary // 4)
    y_top = 1.5 - 0.4 * np.cos(np.pi * (x_top + 2.0) / 4.0)  # Convex downward

    # Left (west) boundary - straight line
    y_left = np.linspace(y_top[-1], y_bottom[0], n_boundary // 4)
    x_left = np.full_like(y_left, -2.0)

    # Combine all boundary segments (ensure closed polygon)
    boundary_x = np.concatenate([x_bottom[:-1], x_right[:-1], x_top[:-1], x_left[:-1]])
    boundary_y = np.concatenate([y_bottom[:-1], y_right[:-1], y_top[:-1], y_left[:-1]])

    # Generate interior mesh points
    # Create a denser grid in the region of interest
    x_grid = np.linspace(-2.5, 2.5, 30)
    y_grid = np.linspace(-1.5, 2.0, 25)
    xx, yy = np.meshgrid(x_grid, y_grid)

    # Flatten grid points
    grid_x = xx.flatten()
    grid_y = yy.flatten()

    # Filter grid points to keep only those inside the boundary
    inside_mask = point_in_polygon(grid_x, grid_y, boundary_x, boundary_y)
    grid_x = grid_x[inside_mask]
    grid_y = grid_y[inside_mask]

    # Add some random points in the interior for variety
    n_random = 100
    rng = np.random.default_rng()
    random_x = rng.uniform(-2.0, 2.0, n_random)
    random_y = rng.uniform(-1.0, 1.5, n_random)

    # Filter random points to keep only those inside the boundary
    random_inside = point_in_polygon(random_x, random_y, boundary_x, boundary_y)
    random_x = random_x[random_inside]
    random_y = random_y[random_inside]

    # Combine boundary points with interior points
    # Boundary vertices must exist in the mesh for the constraint to work
    all_points_x = np.concatenate([boundary_x, grid_x, random_x])
    all_points_y = np.concatenate([boundary_y, grid_y, random_y])

    print(f"  Boundary points: {len(boundary_x)}")
    print(f"  Interior grid points: {len(grid_x)}")
    print(f"  Interior random points: {len(random_x)}")
    print(f"  Total mesh points: {len(all_points_x)}")

    return all_points_x, all_points_y, boundary_x, boundary_y


def test_interpolation(triangulation, points_x, points_y):
    """Test interpolation at some query points."""

    # Create a simple function to interpolate (e.g., z = x^2 + y^2)
    values = points_x**2 + points_y**2

    # Test interpolation at a few points
    test_points = [(0.0, 0.0), (0.3, 0.3), (-0.2, 0.4), (0.5, -0.1)]

    print("\nInterpolation test results:")
    print("Function: z = x² + y²")
    print("-" * 40)

    for x, y in test_points:
        result = triangulation.interpolate(x, y, values)
        expected = x**2 + y**2
        if not np.isnan(result):
            error = abs(result - expected)
            print(
                f"Point ({x:5.2f}, {y:5.2f}): interpolated={result:.4f}, expected={expected:.4f}, error={error:.4e}"
            )
        else:
            print(f"Point ({x:5.2f}, {y:5.2f}): Outside triangulation domain")

    return values


def test_vector_interpolation(triangulation, points_x, points_y):
    """Test vector-based interpolation methods."""

    print("\n" + "=" * 50)
    print("Vector-based Interpolation Tests")
    print("=" * 50)

    # Create test values
    values = points_x**2 + points_y**2

    # Test 1: Vector-based weights calculation
    print("\nTest 1: Vector-based weights calculation")
    test_x = np.array([0.0, 0.3, -0.2, 0.5, 10.0])  # Last point is outside
    test_y = np.array([0.0, 0.3, 0.4, -0.1, 10.0])  # Last point is outside

    weights_result = triangulation.get_interpolation_weights(test_x, test_y)

    print(f"  Query points: {len(test_x)}")
    print(f"  Valid points: {np.sum(weights_result['valid'])}")
    print(f"  Invalid points: {np.sum(~weights_result['valid'])}")

    # Verify weights sum to 1 for valid points
    for i in range(len(test_x)):
        if weights_result["valid"][i]:
            weight_sum = np.sum(weights_result["weights"][i])
            print(
                f"  Point ({test_x[i]:5.2f}, {test_y[i]:5.2f}): weight sum = {weight_sum:.6f}"
            )

    # Test 2: Vector-based interpolation
    print("\nTest 2: Vector-based interpolation (interpolate_many)")
    interpolated = triangulation.interpolate_many(test_x, test_y, values)

    print("  Results:")
    for i in range(len(test_x)):
        expected = (
            test_x[i] ** 2 + test_y[i] ** 2 if weights_result["valid"][i] else np.nan
        )
        if not np.isnan(interpolated[i]):
            error = abs(interpolated[i] - expected)
            print(
                f"    ({test_x[i]:5.2f}, {test_y[i]:5.2f}): interp={interpolated[i]:.4f}, expected={expected:.4f}, error={error:.4e}"
            )
        else:
            print(f"    ({test_x[i]:5.2f}, {test_y[i]:5.2f}): Outside domain (NaN)")

    # Test 3: Performance comparison
    print("\nTest 3: Performance comparison (1000 points)")
    import time

    # Generate random test points
    n_test = 1000
    rng = np.random.default_rng()
    rand_x = rng.uniform(-1.5, 1.5, n_test)
    rand_y = rng.uniform(-0.8, 1.3, n_test)

    # Time single-point method (sample only)
    sample_size = 100
    start = time.time()
    for i in range(sample_size):
        _ = triangulation.interpolate(rand_x[i], rand_y[i], values)
    single_time = (time.time() - start) * (n_test / sample_size)

    # Time vector method
    start = time.time()
    _ = triangulation.interpolate_many(rand_x, rand_y, values)
    vector_time = time.time() - start

    print(f"  Single-point method (estimated): {single_time:.3f} seconds")
    print(f"  Vector method: {vector_time:.3f} seconds")
    print(f"  Speedup: {single_time/vector_time:.1f}x")

    # Test 4: Different input types
    print("\nTest 4: Different input types")

    # Test with Python lists
    list_x = [0.1, 0.2, 0.3]
    list_y = [0.1, 0.2, 0.3]
    result_list = triangulation.interpolate_many(list_x, list_y, values)
    print(f"  From lists: shape={result_list.shape}, first value={result_list[0]:.4f}")

    # Test with 2D arrays (should be flattened)
    arr_2d_x = np.array([[0.1, 0.2], [0.3, 0.4]])
    arr_2d_y = np.array([[0.1, 0.2], [0.3, 0.4]])
    result_2d = triangulation.interpolate_many(arr_2d_x, arr_2d_y, values)
    print(f"  From 2D arrays: shape={result_2d.shape}, values={result_2d}")

    print("\nVector tests completed successfully!")


def visualize_triangulation(triangulation, boundary_x, boundary_y, values=None):
    """Create a visualization of the triangulation."""

    # Get mesh data from triangulation
    vertices = triangulation.get_vertices()
    triangles = triangulation.get_triangles()

    print("\nTriangulation statistics:")
    print(f"  Number of vertices: {len(vertices)}")
    print(f"  Number of triangles: {len(triangles)}")

    # Create figure with subplots
    fig = plt.figure(figsize=(15, 5))

    # Plot 1: Triangulation mesh
    ax1 = fig.add_subplot(131)
    ax1.triplot(
        vertices[:, 0], vertices[:, 1], triangles, "b-", linewidth=0.5, alpha=0.7
    )
    ax1.plot(vertices[:, 0], vertices[:, 1], "ro", markersize=2, alpha=0.5)
    ax1.plot(boundary_x, boundary_y, "g-", linewidth=2, label="Boundary constraint")
    ax1.fill(boundary_x, boundary_y, alpha=0.1, color="green")

    # Add annotations for boundary characteristics
    ax1.text(0, -0.5, "Concave South", ha="center", fontsize=10, color="darkgreen")
    ax1.text(0, 1.2, "Convex North", ha="center", fontsize=10, color="darkgreen")
    ax1.text(-2.2, 0.3, "West", rotation=90, va="center", fontsize=9, color="darkgreen")
    ax1.text(2.2, 0.3, "East", rotation=90, va="center", fontsize=9, color="darkgreen")

    ax1.set_aspect("equal")
    ax1.set_title("Triangulation Mesh with Curved Boundaries")
    ax1.set_xlabel("X")
    ax1.set_ylabel("Y")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # Plot 2: Colored by triangle index
    ax2 = fig.add_subplot(132)
    tri = mtri.Triangulation(vertices[:, 0], vertices[:, 1], triangles)
    colors = np.arange(len(triangles))
    tpc = ax2.tripcolor(tri, colors, cmap="viridis", shading="flat")
    ax2.plot(boundary_x, boundary_y, "w-", linewidth=2)
    ax2.set_aspect("equal")
    ax2.set_title("Triangles Colored by Index")
    ax2.set_xlabel("X")
    ax2.set_ylabel("Y")
    fig.colorbar(tpc, ax=ax2, label="Triangle Index")

    # Plot 3: Interpolated values (if provided)
    if values is not None:
        ax3 = fig.add_subplot(133)
        # Reorder values to match vertex ordering
        vertex_values = np.zeros(len(vertices))
        vertex_values[: len(values)] = values

        tpc2 = ax3.tripcolor(tri, vertex_values, cmap="coolwarm", shading="gouraud")
        ax3.plot(boundary_x, boundary_y, "k-", linewidth=2)
        ax3.set_aspect("equal")
        ax3.set_title("Interpolated Function (z = x² + y²)")
        ax3.set_xlabel("X")
        ax3.set_ylabel("Y")
        fig.colorbar(tpc2, ax=ax3, label="z value")

    plt.tight_layout()
    return fig


def main():
    """Main test function."""

    print("=" * 50)
    print("LibTri Triangulation Test")
    print("=" * 50)

    # Generate test data
    print("\nGenerating test data...")
    points_x, points_y, boundary_x, boundary_y = generate_test_data()
    print(f"  Generated {len(points_x)} interior points")
    print(f"  Boundary has {len(boundary_x)} vertices")

    # Create triangulation
    print("\nCreating triangulation...")
    triangulation = PyTriangulation(points_x, points_y)
    print("  Triangulation created successfully!")

    # Apply boundary constraint
    print("\nApplying boundary constraint...")
    triangulation.apply_constraint_polygon(boundary_x, boundary_y)
    print("  Boundary constraint applied successfully!")

    # Test interpolation
    values = test_interpolation(triangulation, points_x, points_y)

    # Test vector-based interpolation methods
    test_vector_interpolation(triangulation, points_x, points_y)

    # Visualize
    print("\nCreating visualization...")
    visualize_triangulation(triangulation, boundary_x, boundary_y, values)

    # Test getting interpolation weights directly
    print("\nTesting get_interpolation_weight at (0.1, 0.1):")
    weights_info = triangulation.get_interpolation_weight(0.1, 0.1)
    if weights_info is not None:
        print(f"  Triangle vertices: {weights_info['vertices']}")
        print(f"  Barycentric weights: {weights_info['weights']}")
        print(
            f"  Sum of weights: {np.sum(weights_info['weights']):.6f} (should be 1.0)"
        )
    else:
        print("  Point is outside triangulation")

    plt.show()
    print("\nTest completed successfully!")


if __name__ == "__main__":
    main()
