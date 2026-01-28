
find_package(simdjson REQUIRED)
find_package(ZLIB REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ixwebsocket CONFIG REQUIRED)
add_definitions(-DSIMDJSON_USING_LIBRARY=1)


add_library(ServerCore STATIC 
    RingBuffer.h CoinRegistry.h Analytics.h
    Server.h Server.cpp 
    Session.h Session.cpp
)

target_include_directories(
    ServerCore
    PUBLIC 
        ${CMAKE_CURRENT_SOURCE_DIR}
		"${PROJECT_SOURCE_DIR}/Include"
)

target_link_libraries(
    ServerCore 
    PUBLIC 
        Utils 
        ProjectInclude 
        Boost::system 
        Boost::thread 
        Boost::asio
        Boost::format
    PRIVATE		
		simdjson::simdjson
		ixwebsocket::ixwebsocket
		OpenSSL::SSL
		OpenSSL::Crypto
)

if(WIN32)
    target_link_libraries(ServerCore PRIVATE bcrypt crypt32 ws2_32)
else()
    target_link_libraries(ServerCore PRIVATE pthread dl)
endif()

add_executable(Server main.cpp)

#set_target_properties(Server PROPERTIES VS_STARTUP_PROJECT 1)

target_link_libraries(
    Server
    PRIVATE 
        ServerCore
)

if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")

    set(ALL_TARGETS ServerCore Server) 

    foreach(target ${ALL_TARGETS})
        target_compile_options(${target} PRIVATE /W4 /EHsc /arch:AVX2 /Gy)

        target_compile_options(${target} PRIVATE $<$<CONFIG:Release>:/Ox /Ot /Oi /GL>)
		
		target_compile_options(${target} PRIVATE /fp:fast)

        if(target STREQUAL "ServerCore")
            set_target_properties(${target} PROPERTIES STATIC_LIBRARY_OPTIONS "/LTCG")
        else()
            set_target_properties(${target} PROPERTIES 
                LINK_FLAGS_RELEASE "/LTCG /INCREMENTAL:NO /OPT:REF /OPT:ICF"
                LINK_FLAGS_DEBUG "/DEBUG"
            )
        endif()
    endforeach()

else()
    #target_compile_options(ServerCore PRIVATE -Wall -O3 -march=native -flto)
    #target_compile_options(Server PRIVATE -Wall -O3 -march=native -flto)
	target_compile_options(ServerCore PRIVATE -Wall -O3 -g -march=native)
    target_compile_options(Server PRIVATE -Wall -O3 -g -march=native)
	target_link_libraries(Server PRIVATE pthread dl)
endif()


include(CheckIPOSupported)
check_ipo_supported(RESULT ipo_supported OUTPUT error)
if(ipo_supported)
    set_target_properties(ServerCore PROPERTIES INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE)
    set_target_properties(Server PROPERTIES INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE)
endif()