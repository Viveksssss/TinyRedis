cmake_minimum_required(VERSION 3.13)
project(redis)


file(GLOB_RECURSE SOURCE_FILES src/*.cpp)

cmake_policy(SET CMP0167 NEW)
set(CMAKE_CXX_STANDARD 23) # Enable the C++23 standard
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_SOURCE_DIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_SOURCE_DIR})


if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_compile_options(-fcoroutines)
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    add_compile_options(-fcoroutines-ts)
endif()

find_package(Boost REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(spdlog REQUIRED)

add_executable(redis ${SOURCE_FILES})

target_include_directories(redis PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(redis PRIVATE Boost::headers)
target_link_libraries(redis PRIVATE nlohmann_json::nlohmann_json)
target_link_libraries(redis PRIVATE spdlog::spdlog)