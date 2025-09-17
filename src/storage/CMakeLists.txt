add_library(
  postgres_ext_storage OBJECT
  postgres_catalog.cpp
  postgres_catalog_set.cpp
  postgres_connection_pool.cpp
  postgres_clear_cache.cpp
  postgres_delete.cpp
  postgres_index.cpp
  postgres_index_entry.cpp
  postgres_index_set.cpp
  postgres_insert.cpp
  postgres_merge_into.cpp
  postgres_optimizer.cpp
  postgres_schema_entry.cpp
  postgres_schema_set.cpp
  postgres_table_entry.cpp
  postgres_table_set.cpp
  postgres_transaction.cpp
  postgres_transaction_manager.cpp
  postgres_type_entry.cpp
  postgres_type_set.cpp
  postgres_update.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:postgres_ext_storage>
    PARENT_SCOPE)
