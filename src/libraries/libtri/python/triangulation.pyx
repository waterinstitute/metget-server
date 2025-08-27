# distutils: language = c++
# cython: language_level=3

from libcpp.vector cimport vector
from libcpp.memory cimport unique_ptr
from libcpp cimport bool
from libc.stdint cimport uint32_t
cimport numpy as np
import numpy as np

cdef extern from "../src/Triangulation.h" namespace "CxxTri":
    cdef cppclass Triangulation:
        cppclass t_InterpolationWeight:
            unsigned int vertices[3]
            double weights[3]
            bool valid
            t_InterpolationWeight()  # Default constructor

        cppclass t_Triangle:
            unsigned int vertices[3]

        cppclass t_Point:
            double x() const
            double y() const

        Triangulation(const vector[double] &points_x,
                     const vector[double] &points_y) except +

        void apply_constraint_polygon(const vector[double] &region_x,
                                     const vector[double] &region_y) except +

        void apply_constraint_polygon(const vector[t_Point] &region) except +

        t_InterpolationWeight get_interpolation_weight(double x, double y) const
        vector[t_InterpolationWeight] get_interpolation_weights(
            const vector[double] &points_x,
            const vector[double] &points_y) const
        vector[t_Triangle] get_triangles() const
        vector[t_Point] get_vertices() const

cdef class PyTriangulation:
    cdef Triangulation* _triangulation

    def __cinit__(self, np.ndarray[double, ndim=1] points_x,
                  np.ndarray[double, ndim=1] points_y):
        cdef vector[double] pts_x, pts_y
        cdef int i

        for i in range(points_x.shape[0]):
            pts_x.push_back(points_x[i])
            pts_y.push_back(points_y[i])

        self._triangulation = new Triangulation(pts_x, pts_y)

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
        cdef Triangulation.t_InterpolationWeight result = self._triangulation.get_interpolation_weight(x, y)

        if not result.valid:
            return None

        vertices = np.array([result.vertices[0], result.vertices[1], result.vertices[2]], dtype=np.uint32)
        weights = np.array([result.weights[0], result.weights[1], result.weights[2]], dtype=np.float64)

        return {
            'vertices': vertices,
            'weights': weights,
            'valid': result.valid
        }

    def get_interpolation_weights(self, points_x, points_y):
        # Convert inputs to contiguous numpy arrays
        cdef np.ndarray[double, ndim=1, mode='c'] pts_x = np.ascontiguousarray(points_x, dtype=np.float64).ravel()
        cdef np.ndarray[double, ndim=1, mode='c'] pts_y = np.ascontiguousarray(points_y, dtype=np.float64).ravel()

        if pts_x.shape[0] != pts_y.shape[0]:
            raise ValueError("points_x and points_y must have the same length")

        cdef int n_points = pts_x.shape[0]
        cdef vector[double] vec_x, vec_y
        cdef int i, j

        vec_x.reserve(n_points)
        vec_y.reserve(n_points)

        cdef double[:] x_view = pts_x
        cdef double[:] y_view = pts_y

        for i in range(n_points):
            vec_x.push_back(x_view[i])
            vec_y.push_back(y_view[i])

        cdef vector[Triangulation.t_InterpolationWeight] results = self._triangulation.get_interpolation_weights(vec_x, vec_y)

        cdef np.ndarray[np.uint32_t, ndim=2, mode='c'] vertices = np.zeros((n_points, 3), dtype=np.uint32)
        cdef np.ndarray[double, ndim=2, mode='c'] weights = np.zeros((n_points, 3), dtype=np.float64)
        cdef np.ndarray[np.uint8_t, ndim=1, mode='c'] valid = np.zeros(n_points, dtype=np.uint8)

        cdef np.uint32_t[:, :] vert_view = vertices
        cdef double[:, :] weight_view = weights
        cdef np.uint8_t[:] valid_view = valid

        for i in range(n_points):
            valid_view[i] = results[i].valid
            if results[i].valid:
                for j in range(3):
                    vert_view[i, j] = results[i].vertices[j]
                    weight_view[i, j] = results[i].weights[j]

        return {
            'vertices': vertices,
            'weights': weights,
            'valid': valid.astype(np.bool_)
        }

    def interpolate(self, double x, double y, np.ndarray[double, ndim=1] values):
        result = self.get_interpolation_weight(x, y)
        if result is None:
            return np.nan

        interpolated_value = 0.0
        for i in range(3):
            interpolated_value += result['weights'][i] * values[result['vertices'][i]]

        return interpolated_value

    def interpolate_many(self, points_x, points_y, np.ndarray[double, ndim=1] values):
        result = self.get_interpolation_weights(points_x, points_y)

        vertices = result['vertices']
        weights = result['weights']
        valid = result['valid']

        n_points = len(valid)
        interpolated = np.full(n_points, np.nan, dtype=np.float64)

        valid_mask = valid
        if np.any(valid_mask):
            interpolated[valid_mask] = np.sum(
                weights[valid_mask] * values[vertices[valid_mask]],
                axis=1
            )

        return interpolated

    def get_triangles(self):
        cdef vector[Triangulation.t_Triangle] triangles = self._triangulation.get_triangles()
        cdef int i
        cdef int n_triangles = triangles.size()

        tri_array = np.zeros((n_triangles, 3), dtype=np.uint32)

        for i in range(n_triangles):
            tri_array[i, 0] = triangles[i].vertices[0]
            tri_array[i, 1] = triangles[i].vertices[1]
            tri_array[i, 2] = triangles[i].vertices[2]

        return tri_array

    def get_vertices(self):
        cdef vector[Triangulation.t_Point] vertices = self._triangulation.get_vertices()
        cdef int i
        cdef int n_vertices = vertices.size()

        # Create numpy array for vertices
        vert_array = np.zeros((n_vertices, 2), dtype=np.float64)

        for i in range(n_vertices):
            vert_array[i, 0] = vertices[i].x()
            vert_array[i, 1] = vertices[i].y()

        return vert_array
