
if(cxxtri_ENABLE_TESTS)
    # Find Catch2
    find_package(Catch2 3 REQUIRED)

    # Include system directories for dependencies
    include(${CMAKE_CURRENT_SOURCE_DIR}/../cmake/SystemLink.cmake)

    # Create test executable
    add_executable(cxxtri_tests
        ${CMAKE_CURRENT_SOURCE_DIR}/test_triangulation.cpp
    )

    # Find GMP and MPFR manually (required by CGAL)
    find_path(GMP_INCLUDE_DIR gmp.h
        HINTS
            /usr/local/include
            /opt/homebrew/include
            /usr/include
    )

    find_library(GMP_LIBRARY gmp
        HINTS
            /usr/local/lib
            /opt/homebrew/lib
            /usr/lib
    )

    find_path(MPFR_INCLUDE_DIR mpfr.h
        HINTS
            /usr/local/include
            /opt/homebrew/include
            /usr/include
    )

    find_library(MPFR_LIBRARY mpfr
        HINTS
            /usr/local/lib
            /opt/homebrew/lib
            /usr/lib
    )

    if(NOT GMP_LIBRARY OR NOT MPFR_LIBRARY)
        message(FATAL_ERROR "GMP and MPFR are required for CGAL. Please install them.")
    endif()

    # Link libraries
    target_link_libraries(cxxtri_tests
        PRIVATE
            cxxtri
            Catch2::Catch2WithMain
            cxxtri::cxxtri_options
            cxxtri::cxxtri_warnings
            ${GMP_LIBRARY}
            ${MPFR_LIBRARY}
    )

    # Include directories
    target_include_system_directories(cxxtri_tests
        PRIVATE
            ${Boost_INCLUDE_DIRS}
            ${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/CGAL-6.0.1/include
            ${GMP_INCLUDE_DIR}
            ${MPFR_INCLUDE_DIR}
    )

    # Set C++ standard
    target_compile_features(cxxtri_tests PRIVATE cxx_std_20)

    # Register tests with CTest
    # Use list(APPEND) to add Catch2 module path if available
    list(APPEND CMAKE_MODULE_PATH ${Catch2_DIR})

    # Check if Catch2 provides the discovery module
    include(${CMAKE_CURRENT_BINARY_DIR}/../_deps/catch2-src/extras/Catch.cmake)
    catch_discover_tests(cxxtri_tests
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        EXTRA_ARGS --reporter console --durations yes
    )

    # Add a custom target to run tests
    add_custom_target(run_tests
        COMMAND $<TARGET_FILE:cxxtri_tests>
        DEPENDS cxxtri_tests
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Running triangulation tests"
    )

endif()
