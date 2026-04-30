cmake_minimum_required(VERSION 2.8.12...3.29)
set(TARGET_NAME postgres_scanner)
project(${TARGET_NAME})

add_definitions(
    -DFRONTEND=1
    -D_GNU_SOURCE=1
    -DUSE_OPENSSL=1
    -DHAVE_BIO_GET_DATA=1
    -DHAVE_BIO_METH_NEW=1)

find_package(OpenSSL REQUIRED)
find_package(PostgreSQL REQUIRED)

if(NOT MSVC)
    add_compile_options(
        -Wno-pedantic
        -Wno-sign-compare
        -Wno-unused-variable)
endif()

include_directories(
    include
    database-connector/src/include
    ${OPENSSL_INCLUDE_DIR}
    ${PostgreSQL_INCLUDE_DIRS})

if(WIN32)
  include_directories(
      vcpkg_ports/libpq/include/port/win32
      vcpkg_ports/libpq/include/port/win32_msvc)
endif()

add_subdirectory(src)

set(PARAMETERS "-no-warnings")
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

build_static_extension(${TARGET_NAME} ${ALL_OBJECT_FILES})
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ALL_OBJECT_FILES})

# Fix for ELF symbol conflict when loaded inside PostgreSQL backend
# Use -Bsymbolic to force the extension to prefer its own symbols over global ones
# This fixes the pg_link_canary_is_frontend() check failure where calls from within
# this extension resolve to the backend's version (returns false) instead of ours (returns true)
if(NOT WIN32 AND NOT APPLE)
    target_link_options(${LOADABLE_EXTENSION_NAME} PRIVATE
        "-Wl,-Bsymbolic"
    )
endif()

target_link_libraries(${LOADABLE_EXTENSION_NAME}
    OpenSSL::SSL
    OpenSSL::Crypto
    PostgreSQL::PostgreSQL)
set_property(TARGET ${EXTENSION_NAME} PROPERTY C_STANDARD 99)
set_property(TARGET ${LOADABLE_EXTENSION_NAME} PROPERTY C_STANDARD 99)

if(WIN32)
  target_link_libraries(${LOADABLE_EXTENSION_NAME}
      wsock32
      ws2_32
      wldap32
      secur32
      crypt32)
endif()

install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
