import os

import numpy as np
from Cython.Build import cythonize
from setuptools import Extension, setup

# Get the directory of this setup.py file
base_dir = os.path.dirname(os.path.abspath(__file__))

# Define the extension
extensions = [
    Extension(
        "fasttri.triangulation",
        sources=[
            "python/triangulation.pyx",
            "src/Triangulation.cpp",
        ],
        include_dirs=[
            np.get_include(),
            os.path.join(base_dir, "src"),
            # os.path.join(base_dir, "thirdparty/CGAL-6.0.1/include"),
        ],
        libraries=["gmp", "mpfr"],
        library_dirs=["/usr/local/lib", "/usr/lib", "/opt/homebrew/lib"],
        language="c++",
        extra_compile_args=["-std=c++20", "-O3"],
        extra_link_args=["-std=c++20"],
    )
]

setup(
    name="fasttri",
    ext_modules=cythonize(
        extensions,
        compiler_directives={"language_level": "3"},
    ),
    packages=["fasttri"],
    package_dir={"fasttri": "python"},
    zip_safe=False,
)
