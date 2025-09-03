# distutils: language = c++
# cython: language_level=3

from libcpp.vector cimport vector
from libcpp.memory cimport unique_ptr
from libcpp cimport bool
from libc.stdint cimport uint32_t
cimport numpy as np
import numpy as np

# Wrap the InterpolationWeight class - we'll access the arrays through helper functions
cdef extern from *:
    """
    #include "../src/InterpolationWeight.h"
    // Helper functions to access array elements
    inline unsigned int get_vertex(const FastTri::InterpolationWeight& w, size_t i) {
        return w.vertices()[i];
    }
    inline double get_weight(const FastTri::InterpolationWeight& w, size_t i) {
        return w.weights()[i];
    }
    """
    unsigned int get_vertex(const InterpolationWeight& w, size_t i)
    double get_weight(const InterpolationWeight& w, size_t i)

cdef extern from "../src/InterpolationWeight.h" namespace "FastTri":
    cdef cppclass InterpolationWeight:
        InterpolationWeight()  # Default constructor
        bool valid() const
        @staticmethod
        vector[double] interpolate(const vector[InterpolationWeight]& weights,
                                   const vector[double]& values)

cdef extern from "../src/Triangulation.h" namespace "FastTri":
    cdef cppclass CppTriangulation "FastTri::Triangulation":
        cppclass t_Triangle:
            unsigned int vertices[3]

        cppclass t_Point:
            t_Point(double x, double y)
            double x() const
            double y() const

        CppTriangulation(const vector[double] &points_x,
                     const vector[double] &points_y) except +

        void apply_constraint_polygon(const vector[double] &region_x,
                                     const vector[double] &region_y) except +

        void apply_constraint_polygon(const vector[t_Point] &region) except +

        InterpolationWeight get_interpolation_weight(double x, double y) const
        InterpolationWeight get_interpolation_weight(t_Point point) const
        vector[InterpolationWeight] get_interpolation_weights(
            const vector[double] &points_x,
            const vector[double] &points_y) const
        vector[InterpolationWeight] get_interpolation_weights(
            const vector[t_Point] &points) const
        vector[t_Triangle] get_triangles() const
        vector[t_Point] get_vertices() const

cdef class InterpolationWeights:
    """Wrapper for C++ vector<InterpolationWeight> for efficient interpolation."""
    cdef vector[InterpolationWeight] weights
    cdef readonly int size

    def __init__(self):
        # Default constructor creates empty weights
        self.size = 0

    @staticmethod
    def interpolate(InterpolationWeights weights, values):
        """
        Interpolate using native C++ InterpolationWeight objects.

        Parameters:
        -----------
        weights : InterpolationWeights
            The interpolation weights object
        values : np.ndarray
            Values at mesh vertices to interpolate from

        Returns:
        --------
        np.ndarray
            Interpolated values (NaN for invalid points)
        """
        # Ensure values is float64
        values = np.asarray(values, dtype=np.float64)

        # Convert to C++ vector
        cdef vector[double] vec_values
        cdef double[:] values_view = values
        cdef int i

        vec_values.reserve(values.shape[0])
        for i in range(values.shape[0]):
            vec_values.push_back(values_view[i])

        # Use C++ interpolation
        cdef vector[double] results = InterpolationWeight.interpolate(weights.weights, vec_values)

        # Convert results to numpy
        cdef np.ndarray[double, ndim=1] interpolated = np.zeros(weights.size, dtype=np.float64)
        for i in range(weights.size):
            interpolated[i] = results[i]

        return interpolated

cdef class Triangulation:
    cdef CppTriangulation* _triangulation

    def __cinit__(self, np.ndarray[double, ndim=1] points_x,
                  np.ndarray[double, ndim=1] points_y):
        cdef vector[double] pts_x, pts_y
        cdef int i

        for i in range(points_x.shape[0]):
            pts_x.push_back(points_x[i])
            pts_y.push_back(points_y[i])

        self._triangulation = new CppTriangulation(pts_x, pts_y)

    def __dealloc__(self):
        if self._triangulation != NULL:
            del self._triangulation

    def apply_constraint_polygon(self, np.ndarray[double, ndim=1] region_x,
                                np.ndarray[double, ndim=1] region_y):
        cdef vector[double] bound_x, bound_y
        cdef int i

        if region_x.shape[0] != region_y.shape[0]:
            raise ValueError("region_x and region_y must have the same length")

        for i in range(region_x.shape[0]):
            bound_x.push_back(region_x[i])
            bound_y.push_back(region_y[i])

        self._triangulation.apply_constraint_polygon(bound_x, bound_y)

    def get_interpolation_weight(self, double x, double y):
        cdef InterpolationWeight result = self._triangulation.get_interpolation_weight(x, y)

        if not result.valid():
            return None

        # Access vertices and weights through helper functions
        vertices = np.array([get_vertex(result, 0), get_vertex(result, 1), get_vertex(result, 2)], dtype=np.uint32)
        weights = np.array([get_weight(result, 0), get_weight(result, 1), get_weight(result, 2)], dtype=np.float64)

        return {
            'vertices': vertices,
            'weights': weights,
            'valid': result.valid()
        }

    def get_interpolation_weights(self, points_x, points_y):
        """
        Get interpolation weights as native C++ InterpolationWeight objects.

        This returns an InterpolationWeights object that stores the C++ vector
        internally and can be passed directly to InterpolationWeights.interpolate()
        for efficient interpolation without Python overhead.

        Parameters:
        -----------
        points_x, points_y : array-like
            Query point coordinates

        Returns:
        --------
        InterpolationWeights
            Object containing the C++ vector<InterpolationWeight>
        """
        # Convert inputs to contiguous numpy arrays
        cdef np.ndarray[double, ndim=1, mode='c'] pts_x = np.ascontiguousarray(points_x, dtype=np.float64).ravel()
        cdef np.ndarray[double, ndim=1, mode='c'] pts_y = np.ascontiguousarray(points_y, dtype=np.float64).ravel()

        if pts_x.shape[0] != pts_y.shape[0]:
            raise ValueError("points_x and points_y must have the same length")

        cdef int n_points = pts_x.shape[0]
        cdef vector[double] vec_x, vec_y
        cdef int i

        vec_x.reserve(n_points)
        vec_y.reserve(n_points)

        cdef double[:] x_view = pts_x
        cdef double[:] y_view = pts_y

        for i in range(n_points):
            vec_x.push_back(x_view[i])
            vec_y.push_back(y_view[i])

        # Create the weights object
        cdef InterpolationWeights weights_obj = InterpolationWeights()
        weights_obj.weights = self._triangulation.get_interpolation_weights(vec_x, vec_y)
        weights_obj.size = n_points

        return weights_obj

    def get_triangles(self):
        cdef vector[CppTriangulation.t_Triangle] triangles = self._triangulation.get_triangles()
        cdef int i
        cdef int n_triangles = triangles.size()

        tri_array = np.zeros((n_triangles, 3), dtype=np.uint32)

        for i in range(n_triangles):
            tri_array[i, 0] = triangles[i].vertices[0]
            tri_array[i, 1] = triangles[i].vertices[1]
            tri_array[i, 2] = triangles[i].vertices[2]

        return tri_array

    def get_vertices(self):
        cdef vector[CppTriangulation.t_Point] vertices = self._triangulation.get_vertices()
        cdef int i
        cdef int n_vertices = vertices.size()

        # Create numpy array for vertices
        vert_array = np.zeros((n_vertices, 2), dtype=np.float64)

        for i in range(n_vertices):
            vert_array[i, 0] = vertices[i].x()
            vert_array[i, 1] = vertices[i].y()

        return vert_array
