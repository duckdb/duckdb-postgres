include_directories(include)

add_subdirectory(storage)

add_library(
  postgres_ext_library OBJECT
  postgres_attach.cpp
  postgres_binary_copy.cpp
  postgres_binary_reader.cpp
  postgres_connection.cpp
  postgres_copy_from.cpp
  postgres_copy_to.cpp
  postgres_execute.cpp
  postgres_extension.cpp
  postgres_filter_pushdown.cpp
  postgres_query.cpp
  postgres_scanner.cpp
  postgres_storage.cpp
  postgres_text_reader.cpp
  postgres_utils.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:postgres_ext_library>
    PARENT_SCOPE)
