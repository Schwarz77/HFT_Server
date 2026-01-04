add_library(Utils STATIC Utils.h Utils.cpp)

target_include_directories(
    Utils
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
    Utils 
    PRIVATE 
        Boost::format
)