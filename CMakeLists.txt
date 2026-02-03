cmake_minimum_required(VERSION 2.8.12...3.29)
set(TARGET_NAME postgres_scanner)
project(${TARGET_NAME})

add_definitions(-DFRONTEND=1 -D_GNU_SOURCE=1)
add_definitions(-DUSE_OPENSSL=1 -DHAVE_BIO_GET_DATA=1 -DHAVE_BIO_METH_NEW=1)
set(OPENSSL_USE_STATIC_LIBS TRUE)

find_package(OpenSSL REQUIRED)

if(NOT MSVC)
  set(POSTGRES_SCANNER_EXTRA_CFLAGS
      "-Wno-pedantic -Wno-sign-compare -Wno-unused-variable")
endif()

set(CMAKE_CXX_FLAGS_DEBUG
    "${CMAKE_CXX_FLAGS_DEBUG} ${POSTGRES_SCANNER_EXTRA_CFLAGS}")
set(CMAKE_CXX_FLAGS_RELEASE
    "${CMAKE_CXX_FLAGS_RELEASE} ${POSTGRES_SCANNER_EXTRA_CFLAGS}")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
    "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} ${POSTGRES_SCANNER_EXTRA_CFLAGS}")

set(CMAKE_C_FLAGS_DEBUG
    "${CMAKE_C_FLAGS_DEBUG} ${POSTGRES_SCANNER_EXTRA_CFLAGS} ${POSTGRES_SCANNER_EXTRA_CFLAGS}"
)
set(CMAKE_C_FLAGS_RELEASE
    "${CMAKE_C_FLAGS_RELEASE} ${POSTGRES_SCANNER_EXTRA_CFLAGS}")
set(CMAKE_C_FLAGS_RELWITHDEBINFO
    "${CMAKE_C_FLAGS_RELWITHDEBINFO} ${POSTGRES_SCANNER_EXTRA_CFLAGS}")

include_directories(include postgres/src/include postgres/src/backend
                    postgres/src/interfaces/libpq ${OPENSSL_INCLUDE_DIR})

if(WIN32)
  include_directories(postgres/src/include/port/win32 postgres/src/port
                      postgres/src/include/port/win32_msvc)
endif()

set(LIBPG_SOURCES
    postgres/src/common/base64.c
    postgres/src/common/cryptohash.c
    postgres/src/common/encnames.c
    postgres/src/common/hmac.c
    postgres/src/common/ip.c
    postgres/src/common/link-canary.c
    postgres/src/common/md5.c
    postgres/src/common/md5_common.c
    postgres/src/common/saslprep.c
    postgres/src/common/scram-common.c
    postgres/src/common/sha1.c
    postgres/src/common/sha2.c
    postgres/src/common/string.c
    postgres/src/common/unicode_norm.c
    postgres/src/common/wchar.c
    postgres/src/interfaces/libpq/fe-auth-scram.c
    postgres/src/interfaces/libpq/fe-auth.c
    postgres/src/interfaces/libpq/fe-connect.c
    postgres/src/interfaces/libpq/fe-exec.c
    postgres/src/interfaces/libpq/fe-lobj.c
    postgres/src/interfaces/libpq/fe-misc.c
    postgres/src/interfaces/libpq/fe-print.c
    postgres/src/interfaces/libpq/fe-protocol3.c
    postgres/src/interfaces/libpq/fe-secure.c
    postgres/src/interfaces/libpq/fe-trace.c
    postgres/src/interfaces/libpq/legacy-pqsignal.c
    postgres/src/interfaces/libpq/libpq-events.c
    postgres/src/interfaces/libpq/pqexpbuffer.c
    postgres/src/interfaces/libpq/fe-secure-openssl.c
    postgres/src/interfaces/libpq/fe-secure-common.c
    postgres/src/port/chklocale.c
    postgres/src/port/explicit_bzero.c
    postgres/src/port/inet_net_ntop.c
    postgres/src/port/noblock.c
    postgres/src/port/pg_strong_random.c
    postgres/src/port/pgstrcasecmp.c
    postgres/src/port/snprintf.c
    postgres/src/port/strerror.c
    postgres/src/port/thread.c)

if("${CMAKE_SYSTEM_NAME}" MATCHES "Linux" OR WIN32)
  set(LIBPG_SOURCES ${LIBPG_SOURCES} postgres/src/port/strlcpy.c
                    postgres/src/port/getpeereid.c)
endif()

if(WIN32)
  set(LIBPG_SOURCES
      ${LIBPG_SOURCES}
      postgres/src/interfaces/libpq/pthread-win32.c
      postgres/src/interfaces/libpq/win32.c
      postgres/src/port/getaddrinfo.c
      postgres/src/port/gettimeofday.c
      postgres/src/port/inet_aton.c
      postgres/src/port/open.c
      postgres/src/port/pgsleep.c
      postgres/src/port/system.c
      postgres/src/port/dirmod.c
      postgres/src/port/win32common.c
      postgres/src/port/win32error.c
      postgres/src/port/win32ntdll.c
      postgres/src/port/win32setlocale.c
      postgres/src/port/win32stat.c)

endif()

function(PREPEND var prefix)
  set(listVar "")
  foreach(f ${ARGN})
    list(APPEND listVar "${prefix}/${f}")
  endforeach(f)
  set(${var}
      "${listVar}"
      PARENT_SCOPE)
endfunction(PREPEND)

add_subdirectory(src)

prepend(LIBPG_SOURCES_FULLPATH ${CMAKE_CURRENT_SOURCE_DIR} ${LIBPG_SOURCES})

if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/postgres)

  # Download the PostgreSQL source code
  message(STATUS "Downloading PostgreSQL source code")
  file(
    DOWNLOAD
    "https://github.com/postgres/postgres/archive/refs/tags/REL_15_13.tar.gz"
    ${CMAKE_CURRENT_SOURCE_DIR}/pg.tar.gz
    SHOW_PROGRESS
    EXPECTED_MD5 106c54e53aca9395354a251eeea914c0
    STATUS PG_DOWNLOAD_RESULT)

  if(NOT PG_DOWNLOAD_RESULT EQUAL 0)
    file(REMOVE pg.tar.gz)
    message(FATAL_ERROR "Failed to download PostgreSQL source code")
  endif()

  # Extract the PostgreSQL source code
  message(STATUS "Extracting PostgreSQL source code")
  file(ARCHIVE_EXTRACT INPUT ${CMAKE_CURRENT_SOURCE_DIR}/pg.tar.gz DESTINATION
       ${CMAKE_CURRENT_SOURCE_DIR}/postgres_tmp)

  # Move out of root directory
  file(RENAME ${CMAKE_CURRENT_SOURCE_DIR}/postgres_tmp/postgres-REL_15_13
       ${CMAKE_CURRENT_SOURCE_DIR}/postgres)

  # Remove the tmp directory
  file(REMOVE_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/postgres_tmp)

  # Remove the downloaded tarball
  file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/pg.tar.gz)

  # Configure the PostgreSQL source code
  message(STATUS "Configuring PostgreSQL source code")
  if(WIN32)
    # On windows, use the mkvcbuild.pl script to configure the source code
    execute_process(
      COMMAND perl mkvcbuild.pl
      RESULT_VARIABLE PG_MKVCBUILD_RESULT
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/postgres/src/tools/msvc)

    # Check if configuration was successful
    if(NOT PG_MKVCBUILD_RESULT EQUAL 0)
      file(REMOVE_RECURSE postgres)
      message(
        FATAL_ERROR "Failed to configure PostgreSQL source code for windows")
    endif()
  else()
    # On other platforms, use the configure script to configure the source code
    set(ENV{CC} gcc)
    set(ENV{CXX} g++)
    execute_process(
      COMMAND
        ./configure --without-llvm --without-icu --without-tcl --without-perl
        --without-python --without-gssapi --without-pam --without-bsd-auth
        --without-ldap --without-bonjour --without-selinux --without-systemd
        --without-readline --without-libxml --without-libxslt --without-zlib
        --without-lz4 --without-openssl
      RESULT_VARIABLE PG_CONFIGURE_RESULT
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/postgres)
    # Check if configuration was successful
    if(NOT PG_CONFIGURE_RESULT EQUAL 0)
      file(REMOVE_RECURSE postgres)
      message(FATAL_ERROR "Failed to configure PostgreSQL source code")
    endif()
  endif()
  message(STATUS "Finished setting up PostgreSQL source code!")
endif()

set(PARAMETERS "-no-warnings")
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

build_static_extension(${TARGET_NAME} ${ALL_OBJECT_FILES}
                         ${LIBPG_SOURCES_FULLPATH})
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ALL_OBJECT_FILES}
                         ${LIBPG_SOURCES_FULLPATH})

# Fix for ELF symbol conflict when loaded inside PostgreSQL backend
# Use -Bsymbolic to force the extension to prefer its own symbols over global ones
# This fixes the pg_link_canary_is_frontend() check failure where calls from within
# this extension resolve to the backend's version (returns false) instead of ours (returns true)
if(NOT WIN32 AND NOT APPLE)
    target_link_options(${LOADABLE_EXTENSION_NAME} PRIVATE
        "-Wl,-Bsymbolic"
    )
endif()

# Loadable extension needs OpenSSL
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${OPENSSL_LIBRARIES})

if(WIN32)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} wsock32 ws2_32
                        wldap32 secur32 crypt32)
endif()


install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
