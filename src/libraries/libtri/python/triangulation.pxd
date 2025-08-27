# triangulation.pxd
from libcpp.vector cimport vector
from libcpp cimport bool

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

        t_InterpolationWeight get_interpolation_weights(double x, double y) const
        vector[t_Triangle] get_triangles() const
        vector[t_Point] get_vertices() const
