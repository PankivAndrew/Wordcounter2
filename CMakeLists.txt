cmake_minimum_required(VERSION 3.7)
project(Wordcounter2)

set(CMAKE_CXX_STANDARD 11)
set(BOOST_ROOT "/home/paniv/Boost/Install")
FIND_PACKAGE(Boost 1.58.0)
FIND_PACKAGE( Boost COMPONENTS filesystem program_options system REQUIRED )
set(SOURCE_FILES main.cpp)
add_executable(Wordcounter2 ${SOURCE_FILES})
INCLUDE_DIRECTORIES( ${Boost_INCLUDE_DIR} )
target_link_libraries(Wordcounter2 ${Boost_LIBRARIES})
TARGET_LINK_LIBRARIES(Wordcounter2 pthread)
if(NOT Boost_FOUND)
    message(FATAL_ERROR "Could not find boost!")
endif()
include(PVS-Studio.cmake)

pvs_studio_add_target(TARGET analyze ALL
OUTPUT FORMAT errorfile
ANALYZE Wordcounter2
)

