cmake_minimum_required(VERSION 3.15)

#if (NOT POLICY CMP0167)
#    cmake_policy(SET CMP0167 NEW)
#endif()

project(HFT_Server LANGUAGES CXX)

enable_testing()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

#set(Boost_NO_BOOST_CMAKE ON)
set(Boost_NO_BOOST_CMAKE OFF)

#find_package(Boost REQUIRED CONFIG COMPONENTS system thread asio format)

if(NOT WIN32)
    if(MSVC)
        message(FATAL_ERROR "Critical Error: MSVC is trying to build Linux target!")
    endif()
endif()

if (WIN32)
    # --- Windows (VCPKG) ---
	#set(Boost_NO_BOOST_CMAKE OFF)
    find_package(Boost REQUIRED CONFIG COMPONENTS system thread asio format)
    
else()
    # --- Linux / macOS (System Boost) ---
    #set(Boost_NO_BOOST_CMAKE OFF)
    
	find_package(Boost REQUIRED COMPONENTS system thread)
    
    if (NOT TARGET Boost::asio)
        add_library(Boost::asio INTERFACE IMPORTED)
        set_target_properties(Boost::asio PROPERTIES INTERFACE_LINK_LIBRARIES Boost::system)
    endif()
    
    if (NOT TARGET Boost::format)
        add_library(Boost::format INTERFACE IMPORTED)
    endif()
endif()



find_package(GTest REQUIRED)

add_library(ProjectInclude INTERFACE)
target_include_directories(
    ProjectInclude 
    INTERFACE 
        ${CMAKE_CURRENT_SOURCE_DIR}/Include 
)

add_subdirectory(Server)
add_subdirectory(Client)
add_subdirectory(Utils)
add_subdirectory(Tests)

