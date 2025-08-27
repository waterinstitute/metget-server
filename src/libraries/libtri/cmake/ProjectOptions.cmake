include(cmake/SystemLink.cmake)
include(cmake/LibFuzzer.cmake)
include(CMakeDependentOption)
include(CheckCXXCompilerFlag)

macro(cxxtri_supports_sanitizers)

  # If we are on Darwin arm64, we can't use sanitizers
  if(CMAKE_SYSTEM_NAME STREQUAL "Darwin" AND CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
    set(SUPPORTS_ASAN OFF)
    set(SUPPORTS_UBSAN OFF)
  else()
    if((CMAKE_CXX_COMPILER_ID MATCHES ".*Clang.*" OR CMAKE_CXX_COMPILER_ID MATCHES ".*GNU.*") AND NOT WIN32)
      set(SUPPORTS_UBSAN ON)
    else()
      set(SUPPORTS_UBSAN OFF)
    endif()

    if((CMAKE_CXX_COMPILER_ID MATCHES ".*Clang.*" OR CMAKE_CXX_COMPILER_ID MATCHES ".*GNU.*") AND WIN32)
      set(SUPPORTS_ASAN OFF)
    else()
      set(SUPPORTS_ASAN ON)
    endif()
  endif()
endmacro()

macro(cxxtri_setup_options)

  cxxtri_supports_sanitizers()

  if(NOT PROJECT_IS_TOP_LEVEL OR NOT cxxtri_MAINTAINER_MODE)
    option(cxxtri_MAINTAINER_MODE "Enable maintainer mode" OFF)
    option(cxxtri_ENABLE_HARDENING "Enable hardening" OFF)
    cmake_dependent_option(
      cxxtri_ENABLE_GLOBAL_HARDENING
      "Attempt to push hardening options to built dependencies"
      OFF
      cxxtri_ENABLE_HARDENING
      OFF)
    option(cxxtri_ENABLE_IPO "Enable IPO/LTO" OFF)
    option(cxxtri_WARNINGS_AS_ERRORS "Treat Warnings As Errors" OFF)
    option(cxxtri_ENABLE_USER_LINKER "Enable user-selected linker" OFF)
    option(cxxtri_ENABLE_SANITIZER_ADDRESS "Enable address sanitizer" OFF)
    option(cxxtri_ENABLE_SANITIZER_LEAK "Enable leak sanitizer" OFF)
    option(cxxtri_ENABLE_SANITIZER_UNDEFINED "Enable undefined sanitizer" OFF)
    option(cxxtri_ENABLE_SANITIZER_THREAD "Enable thread sanitizer" OFF)
    option(cxxtri_ENABLE_SANITIZER_MEMORY "Enable memory sanitizer" OFF)
    option(cxxtri_ENABLE_UNITY_BUILD "Enable unity builds" OFF)
    # option(cxxtri_ENABLE_CLANG_TIDY "Enable clang-tidy" OFF)
    option(cxxtri_ENABLE_CPPCHECK "Enable cpp-check analysis" OFF)
    option(cxxtri_ENABLE_PCH "Enable precompiled headers" OFF)
    option(cxxtri_ENABLE_CACHE "Enable ccache" OFF)
  else()
    option(cxxtri_MAINTAINER_MODE "Enable maintainer mode" ON)
    option(cxxtri_ENABLE_HARDENING "Enable hardening" ON)
    cmake_dependent_option(
      cxxtri_ENABLE_GLOBAL_HARDENING
      "Attempt to push hardening options to built dependencies"
      ON
      cxxtri_ENABLE_HARDENING
      OFF)
    option(cxxtri_ENABLE_IPO "Enable IPO/LTO" OFF)
    option(cxxtri_WARNINGS_AS_ERRORS "Treat Warnings As Errors" ON)
    option(cxxtri_ENABLE_USER_LINKER "Enable user-selected linker" OFF)
    option(cxxtri_ENABLE_SANITIZER_ADDRESS "Enable address sanitizer" ${SUPPORTS_ASAN})
    option(cxxtri_ENABLE_SANITIZER_LEAK "Enable leak sanitizer" OFF)
    option(cxxtri_ENABLE_SANITIZER_UNDEFINED "Enable undefined sanitizer" ${SUPPORTS_UBSAN})
    option(cxxtri_ENABLE_SANITIZER_THREAD "Enable thread sanitizer" OFF)
    option(cxxtri_ENABLE_SANITIZER_MEMORY "Enable memory sanitizer" OFF)
    option(cxxtri_ENABLE_UNITY_BUILD "Enable unity builds" OFF)
    # option(cxxtri_ENABLE_CLANG_TIDY "Enable clang-tidy" ON)
    option(cxxtri_ENABLE_CPPCHECK "Enable cpp-check analysis" ON)
    option(cxxtri_ENABLE_PCH "Enable precompiled headers" OFF)
    option(cxxtri_ENABLE_CACHE "Enable ccache" ON)
  endif()

  if(NOT PROJECT_IS_TOP_LEVEL OR NOT cxxtri_MAINTAINER_MODE)
    mark_as_advanced(
      cxxtri_MAINTAINER_MODE
      cxxtri_ENABLE_HARDENING
      cxxtri_ENABLE_GLOBAL_HARDENING
      cxxtri_ENABLE_IPO
      cxxtri_WARNINGS_AS_ERRORS
      cxxtri_ENABLE_USER_LINKER
      cxxtri_ENABLE_SANITIZER_ADDRESS
      cxxtri_ENABLE_SANITIZER_LEAK
      cxxtri_ENABLE_SANITIZER_UNDEFINED
      cxxtri_ENABLE_SANITIZER_THREAD
      cxxtri_ENABLE_SANITIZER_MEMORY
      cxxtri_ENABLE_UNITY_BUILD
      # cxxtri_ENABLE_CLANG_TIDY
      cxxtri_ENABLE_CPPCHECK
      cxxtri_ENABLE_PCH
      cxxtri_ENABLE_CACHE)
  endif()

  if(cxxtri_MAINTAINER_MODE)
    cxxtri_check_libfuzzer_support(LIBFUZZER_SUPPORTED)
    if(LIBFUZZER_SUPPORTED
       AND (cxxtri_ENABLE_SANITIZER_ADDRESS
            OR cxxtri_ENABLE_SANITIZER_THREAD
            OR cxxtri_ENABLE_SANITIZER_UNDEFINED))
      set(DEFAULT_FUZZER ON)
    else()
      set(DEFAULT_FUZZER OFF)
    endif()

    option(cxxtri_BUILD_FUZZ_TESTS "Enable fuzz testing executable" ${DEFAULT_FUZZER})
  endif()

  option(cxxtri_ENABLE_TESTS "Enable building tests" OFF)

endmacro()

macro(cxxtri_global_options)
  if(cxxtri_ENABLE_IPO)
    include(cmake/InterproceduralOptimization.cmake)
    cxxtri_enable_ipo()
  endif()

  cxxtri_supports_sanitizers()

  if(cxxtri_ENABLE_HARDENING AND cxxtri_ENABLE_GLOBAL_HARDENING)
    include(cmake/Hardening.cmake)
    if(NOT SUPPORTS_UBSAN
       OR cxxtri_ENABLE_SANITIZER_UNDEFINED
       OR cxxtri_ENABLE_SANITIZER_ADDRESS
       OR cxxtri_ENABLE_SANITIZER_THREAD
       OR cxxtri_ENABLE_SANITIZER_LEAK)
      set(ENABLE_UBSAN_MINIMAL_RUNTIME FALSE)
    else()
      set(ENABLE_UBSAN_MINIMAL_RUNTIME TRUE)
    endif()
    # message("${cxxtri_ENABLE_HARDENING} ${ENABLE_UBSAN_MINIMAL_RUNTIME} ${cxxtri_ENABLE_SANITIZER_UNDEFINED}")
    cxxtri_enable_hardening(cxxtri_options ON ${ENABLE_UBSAN_MINIMAL_RUNTIME})
  endif()
endmacro()

macro(cxxtri_local_options)
  if(PROJECT_IS_TOP_LEVEL)
    include(cmake/StandardProjectSettings.cmake)
  endif()

  add_library(cxxtri_warnings INTERFACE)
  add_library(cxxtri_options INTERFACE)

  include(cmake/CompilerWarnings.cmake)
  cxxtri_set_project_warnings(
    cxxtri_warnings
    ${cxxtri_WARNINGS_AS_ERRORS}
    ""
    ""
    ""
    "")

  if(cxxtri_ENABLE_USER_LINKER)
    include(cmake/Linker.cmake)
    cxxtri_configure_linker(cxxtri_options)
  endif()

  include(cmake/Sanitizers.cmake)
  cxxtri_enable_sanitizers(
    cxxtri_options
    ${cxxtri_ENABLE_SANITIZER_ADDRESS}
    ${cxxtri_ENABLE_SANITIZER_LEAK}
    ${cxxtri_ENABLE_SANITIZER_UNDEFINED}
    ${cxxtri_ENABLE_SANITIZER_THREAD}
    ${cxxtri_ENABLE_SANITIZER_MEMORY})

  set_target_properties(cxxtri_options PROPERTIES UNITY_BUILD ${cxxtri_ENABLE_UNITY_BUILD})

  if(cxxtri_ENABLE_PCH)
    target_precompile_headers(
      cxxtri_options
      INTERFACE
      <vector>
      <string>
      <utility>)
  endif()

  if(cxxtri_ENABLE_CACHE)
    include(cmake/Cache.cmake)
    cxxtri_enable_cache()
  endif()

  include(cmake/StaticAnalyzers.cmake)
  # if(cxxtri_ENABLE_CLANG_TIDY) cxxtri_enable_clang_tidy(cxxtri_options ${cxxtri_WARNINGS_AS_ERRORS}) endif()

  if(cxxtri_ENABLE_CPPCHECK)
    cxxtri_enable_cppcheck(${cxxtri_WARNINGS_AS_ERRORS} "" # override cppcheck options
    )
  endif()

  if(cxxtri_WARNINGS_AS_ERRORS)
    check_cxx_compiler_flag("-Wl,--fatal-warnings" LINKER_FATAL_WARNINGS)
    if(LINKER_FATAL_WARNINGS)
      # This is not working consistently, so disabling for now target_link_options(cxxtri_options INTERFACE
      # -Wl,--fatal-warnings)
    endif()
  endif()

  if(cxxtri_ENABLE_HARDENING AND NOT cxxtri_ENABLE_GLOBAL_HARDENING)
    include(cmake/Hardening.cmake)
    if(NOT SUPPORTS_UBSAN
       OR cxxtri_ENABLE_SANITIZER_UNDEFINED
       OR cxxtri_ENABLE_SANITIZER_ADDRESS
       OR cxxtri_ENABLE_SANITIZER_THREAD
       OR cxxtri_ENABLE_SANITIZER_LEAK)
      set(ENABLE_UBSAN_MINIMAL_RUNTIME FALSE)
    else()
      set(ENABLE_UBSAN_MINIMAL_RUNTIME TRUE)
    endif()
    cxxtri_enable_hardening(cxxtri_options OFF ${ENABLE_UBSAN_MINIMAL_RUNTIME})
  endif()

endmacro()
