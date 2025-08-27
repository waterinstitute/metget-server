include(${CMAKE_CURRENT_SOURCE_DIR}/../cmake/SystemLink.cmake)

add_library(cxxtri STATIC ${CMAKE_CURRENT_SOURCE_DIR}/Triangulation.cpp)

target_include_system_directories(cxxtri PRIVATE ${Boost_INCLUDE_DIRS})
target_link_libraries(cxxtri PRIVATE cxxtri::cxxtri_options cxxtri::cxxtri_warnings)
