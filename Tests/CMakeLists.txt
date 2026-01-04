
add_executable(Tests utility_test.cpp server_test.cpp session_test.cpp integration_test.cpp perf_test.cpp stress_test.cpp)

target_include_directories(
    Tests
    PRIVATE 
        ${CMAKE_SOURCE_DIR}/Include
	    ${CMAKE_SOURCE_DIR}/Server
	    ${CMAKE_SOURCE_DIR}/Client		
)

target_link_libraries(
    Tests 
    PRIVATE 
        GTest::gtest_main
        ServerCore
		ClientCore
)

include(GoogleTest)
gtest_discover_tests(Tests)

gtest_discover_tests(Tests)
