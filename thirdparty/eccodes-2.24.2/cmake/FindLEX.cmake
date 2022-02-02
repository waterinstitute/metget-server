# - Find lex executable and provides a macro to generate custom build rules
#
# The module defines the following variables:
#  LEX_FOUND - true is lex executable is found
#  LEX_EXECUTABLE - the path to the lex executable
#  LEX_LIBRARIES - The lex libraries
#  LEX_INCLUDE_DIRS - The path to the lex headers
#
#
# If lex is found on the system, the module provides the macro:
#  LEX_TARGET(Name LexInput LexOutput [COMPILE_FLAGS <string>])
# which creates a custom command  to generate the <LexOutput> file from
# the <LexInput> file.  If  COMPILE_FLAGS option is specified, the next
# parameter is added to the lex  command line. Name is an alias used to
# get  details of  this custom  command.  Indeed the  macro defines  the
# following variables:
#  LEX_${Name}_DEFINED - true is the macro ran successfully
#  LEX_${Name}_OUTPUTS - the source file generated by the custom rule, an
#  alias for LexOutput
#  LEX_${Name}_INPUT - the lex source file, an alias for ${LexInput}
#
# Lex scanners oftenly use tokens  defined by Yacc: the code generated
# by Lex  depends of the header  generated by Yacc.   This module also
# defines a macro:
#  ADD_LEX_YACC_DEPENDENCY(LexTarget YaccTarget)
# which  adds the  required dependency  between a  scanner and  a parser
# where  <LexTarget>  and <YaccTarget>  are  the  first parameters  of
# respectively LEX_TARGET and YACC_TARGET macros.
#
#  ====================================================================

#=============================================================================
# Copyright 2009 Kitware, Inc.
# Copyright 2006 Tristan Carel
#
# Distributed under the OSI-approved BSD License (the "License");
# see accompanying file Copyright.txt for details.
#
# This software is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the License for more information.
#=============================================================================

# This file is based on the FindFLEX CMake macro, and adapted by ECMWF

#=============================================================================
# (C) Copyright 2011- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

FIND_PROGRAM(LEX_EXECUTABLE lex DOC "path to the lex executable")
MARK_AS_ADVANCED(LEX_EXECUTABLE)

FIND_LIBRARY(FL_LIBRARY NAMES fl
  DOC "Path to the fl library")

FIND_PATH(LEX_INCLUDE_DIR LexLexer.h
  DOC "Path to the lex headers")

MARK_AS_ADVANCED(FL_LIBRARY LEX_INCLUDE_DIR)

SET(LEX_INCLUDE_DIRS ${LEX_INCLUDE_DIR})
SET(LEX_LIBRARIES ${FL_LIBRARY})

IF(LEX_EXECUTABLE)

  #============================================================
  # LEX_TARGET (public macro)
  #============================================================
  #
  MACRO(LEX_TARGET Name Input Output)
    SET(LEX_TARGET_usage "LEX_TARGET(<Name> <Input> <Output> [COMPILE_FLAGS <string>]")
    IF(${ARGC} GREATER 3)
      IF(${ARGC} EQUAL 5)
        IF("${ARGV3}" STREQUAL "COMPILE_FLAGS")
          SET(LEX_EXECUTABLE_opts  "${ARGV4}")
          SEPARATE_ARGUMENTS(LEX_EXECUTABLE_opts)
        ELSE()
          MESSAGE(SEND_ERROR ${LEX_TARGET_usage})
        ENDIF()
      ELSE()
        MESSAGE(SEND_ERROR ${LEX_TARGET_usage})
      ENDIF()
    ENDIF()

    message( STATUS "${LEX_EXECUTABLE} ${LEX_EXECUTABLE_opts} -t ${Input} > ${Output}" )

    ADD_CUSTOM_COMMAND(OUTPUT ${Output}
      COMMAND ${LEX_EXECUTABLE} ${LEX_EXECUTABLE_opts} -t ${Input} > ${Output}
      DEPENDS ${Input}
      COMMENT "[LEX][${Name}] Building scanner with lex ${LEX_VERSION}"
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

    SET(LEX_${Name}_DEFINED TRUE)
    SET(LEX_${Name}_OUTPUTS ${Output})
    SET(LEX_${Name}_INPUT ${Input})
    SET(LEX_${Name}_COMPILE_FLAGS ${LEX_EXECUTABLE_opts})
  ENDMACRO(LEX_TARGET)
  #============================================================


  #============================================================
  # ADD_LEX_YACC_DEPENDENCY (public macro)
  #============================================================
  #
  MACRO(ADD_LEX_YACC_DEPENDENCY LexTarget YaccTarget)

    IF(NOT LEX_${LexTarget}_OUTPUTS)
      MESSAGE(SEND_ERROR "Lex target `${LexTarget}' does not exists.")
    ENDIF()

    IF(NOT YACC_${YaccTarget}_OUTPUT_HEADER)
      MESSAGE(SEND_ERROR "Yacc target `${YaccTarget}' does not exists.")
    ENDIF()

    SET_SOURCE_FILES_PROPERTIES(${LEX_${LexTarget}_OUTPUTS}
      PROPERTIES OBJECT_DEPENDS ${YACC_${YaccTarget}_OUTPUT_HEADER})
  ENDMACRO(ADD_LEX_YACC_DEPENDENCY)
  #============================================================

ENDIF(LEX_EXECUTABLE)

FIND_PACKAGE_HANDLE_STANDARD_ARGS(LEX REQUIRED_VARS LEX_EXECUTABLE)

# FindLEX.cmake ends here