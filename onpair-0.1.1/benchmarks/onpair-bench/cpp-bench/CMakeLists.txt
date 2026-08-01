cmake_minimum_required(VERSION 3.21)
project(cpp_bench CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

# Upstream OnPair C++ — pulled in as a git submodule. Exports target `onpair`
# (alias `OnPair::onpair`). Header-only hot paths instantiate in this binary,
# so the bench target gets the same opt-in perf flags upstream uses.
set(ONPAIR_NATIVE_ARCH ON CACHE BOOL "" FORCE)
set(ONPAIR_ENABLE_LTO  ON CACHE BOOL "" FORCE)
# Upstream's find_package(Boost ...) succeeds against partial system installs
# (e.g. homebrew BoostConfig without boost_unordered), but the Boost::unordered
# target is never created — link fails. Force-disable system Boost so upstream
# falls through to its FetchContent path. Override with -DCMAKE_DISABLE_FIND_PACKAGE_Boost=OFF.
set(CMAKE_DISABLE_FIND_PACKAGE_Boost ON CACHE BOOL "" FORCE)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/onpair_cpp onpair_cpp)

add_executable(cpp_bench main.cpp)
target_link_libraries(cpp_bench PRIVATE onpair)
target_compile_options(cpp_bench PRIVATE -O3 -DNDEBUG)
if(COMMAND onpair_apply_configured_optimizations)
  onpair_apply_configured_optimizations(cpp_bench)
endif()
