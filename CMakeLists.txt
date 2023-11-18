cmake_minimum_required(VERSION 3.15)
project(io-fixed-throughput CXX)

set(CMAKE_NO_SYSTEM_FROM_IMPORTED TRUE)

find_package(rusty-cpp CONFIG REQUIRED)

aux_source_directory(src SRCS)
add_executable(${PROJECT_NAME} ${SRCS})

target_link_libraries(${PROJECT_NAME}
	PUBLIC
		rusty-cpp
		boost_program_options
)
