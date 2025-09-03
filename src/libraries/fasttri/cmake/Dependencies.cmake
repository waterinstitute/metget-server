function(mark_as_advanced_wildcard variable_prefix)
  get_cmake_property(CACHE_VARS VARIABLES)
  foreach(CACHE_VAR ${CACHE_VARS})
    if(CACHE_VAR MATCHES "^${variable_prefix}")
      mark_as_advanced(${CACHE_VAR})
    endif()
  endforeach()
endfunction()

macro(cxxtri_setup_dependencies)

  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/CPM.cmake)

  # Mark the CPM variables as advanced
  mark_as_advanced_wildcard("CPM_")

  # Boost
  find_package(Boost REQUIRED CONFIG)

  # ####################################################################################################################
  # Catch2
  # ####################################################################################################################
  if(cxxtri_ENABLE_TESTS)
    cpmaddpackage(
      NAME
      Catch2
      GITHUB_REPOSITORY
      catchorg/Catch2
      VERSION
      3.10.0
      EXCLUDE_FROM_ALL
      SYSTEM)
    mark_as_advanced_wildcard("CATCH_")

    # Disable cppcheck and clang-tidy on Catch2
    set_target_properties(Catch2 PROPERTIES CXX_CPPCHECK "" CXX_CLANG_TIDY "")
    set_target_properties(Catch2WithMain PROPERTIES CXX_CPPCHECK "" CXX_CLANG_TIDY "")

  endif()

  # ####################################################################################################################
  # Google Benchmark
  # ####################################################################################################################
  if(cxxtri_ENABLE_BENCHMARKS)
    # set(BENCHMARK_DOWNLOAD_DEPENDENCIES ON) set(BENCHMARK_ENABLE_TESTING OFF) set(BENCHMARK_ENABLE_GTEST_TESTS OFF)
    # set(BENCHMARK_INSTALL_DOCS OFF) set(BENCHMARK_ENABLE_INSTALL OFF) CPMAddPackage( NAME benchmark GITHUB_REPOSITORY
    # google/benchmark GIT_TAG v1.9.0 EXCLUDE_FROM_ALL SYSTEM ) mark_as_advanced_wildcard("BENCHMARK_")
    # mark_as_advanced(GOOGLETEST_PATH) mark_as_advanced(CXXFEATURECHECK_DEBUG) mark_as_advanced(LLVM_FILECHECK_EXE)
    #
    # # Disable cppcheck and clang-tidy on Google Benchmark set_target_properties(benchmark PROPERTIES CXX_CPPCHECK ""
    # CXX_CLANG_TIDY "") set_target_properties(benchmark_main PROPERTIES CXX_CPPCHECK "" CXX_CLANG_TIDY "")
    # target_compile_options(benchmark PRIVATE "-w")
  endif()

  # Mark the FetchContent variables as advanced
  mark_as_advanced_wildcard("FETCHCONTENT_")
  mark_as_advanced("pugixml_DIR")

endmacro()
