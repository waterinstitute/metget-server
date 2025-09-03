cmake_minimum_required(VERSION 3.10)
project(fasttri)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
mark_as_advanced(CLEAR CMAKE_BUILD_TYPE)
mark_as_advanced(CLEAR CMAKE_CXX_COMPILER)
mark_as_advanced(CLEAR CMAKE_CXX_FLAGS)
mark_as_advanced(CLEAR CMAKE_CXX_FLAGS_RELEASE)
mark_as_advanced(CLEAR CMAKE_CXX_FLAGS_RELWITHDEBINFO)
mark_as_advanced(CLEAR CMAKE_CXX_FLAGS_MINSIZEREL)
mark_as_advanced(CLEAR CMAKE_CXX_FLAGS_DEBUG)

# Check for C++20 support
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-std=c++20" COMPILER_SUPPORTS_CXX20)
if(NOT COMPILER_SUPPORTS_CXX20)
    message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no C++20 support. Please use a different C++ compiler.")
endif()

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/PreventInSourceBuilds.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/ProjectOptions.cmake)
fasttri_setup_options()
fasttri_global_options()
fasttri_local_options()
add_library(fasttri::fasttri_options ALIAS fasttri_options)
add_library(fasttri::fasttri_warnings ALIAS fasttri_warnings)

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Dependencies.cmake)
fasttri_setup_dependencies()

# Enable testing if tests are being built
if(fasttri_ENABLE_TESTS)
    enable_testing()
endif()

add_subdirectory(src)
add_subdirectory(tests)
