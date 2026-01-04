
add_library(ClientCore STATIC 
    Client.h Client.cpp
)

target_include_directories(
    ClientCore
    PUBLIC 
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
    ClientCore 
    PUBLIC 
        Utils 
        ProjectInclude 
        Boost::system 
        Boost::thread 
        Boost::asio
        #Boost::format
)

add_executable(Client main.cpp)


target_link_libraries(
    Client
    PRIVATE 
        ClientCore
)