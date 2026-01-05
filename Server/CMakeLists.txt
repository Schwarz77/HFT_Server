
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
		ZLIB::ZLIB
		bcrypt
		crypt32
		ws2_32			
)

add_executable(Server main.cpp)

#set_target_properties(Server PROPERTIES VS_STARTUP_PROJECT 1)

target_link_libraries(
    Server
    PRIVATE 
        ServerCore
)

# 2. Флаги оптимизации — КРИТИЧЕСКИ ВАЖНО для Lock-Free и SIMD
if(MSVC)
    # 1. Глобальная настройка статического рантайма (/MT)
    # Это важно для переносимости на машины без vcpkg и чистого запуска
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")

    # Настраиваем список таргетов для массового применения флагов
    set(ALL_TARGETS ServerCore Server) 

    foreach(target ${ALL_TARGETS})
        # Базовые флаги + /Gy (Function-Level Linking) для LTCG
        target_compile_options(${target} PRIVATE /W4 /EHsc /arch:AVX2 /Gy)

        # Оптимизации Release
        target_compile_options(${target} PRIVATE $<$<CONFIG:Release>:/Ox /Ot /Oi /GL>)
		
		target_compile_options(${target} PRIVATE /fp:fast)

        # Настройки Линкера/Библиотекаря
        if(target STREQUAL "ServerCore")
            # Для статической либы настраиваем Librarian (аналог LTCG)
            set_target_properties(${target} PROPERTIES STATIC_LIBRARY_OPTIONS "/LTCG")
        else()
            # Для EXE: LTCG + отключаем инкрементальную линковку
            set_target_properties(${target} PROPERTIES 
                LINK_FLAGS_RELEASE "/LTCG /INCREMENTAL:NO /OPT:REF /OPT:ICF"
                LINK_FLAGS_DEBUG "/DEBUG"
            )
        endif()
    endforeach()

else()
    # Настройки для GCC/Clang (Linux)
    target_compile_options(ServerCore PRIVATE -Wall -O3 -march=native -flto)
    target_compile_options(Server PRIVATE -Wall -O3 -march=native -flto)
endif()

# Современный способ включения IPO/LTO в CMake
include(CheckIPOSupported)
check_ipo_supported(RESULT ipo_supported OUTPUT error)
if(ipo_supported)
    set_target_properties(ServerCore PROPERTIES INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE)
    set_target_properties(Server PROPERTIES INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE)
endif()