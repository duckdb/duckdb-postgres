set(POSTGRES_DBCONNECTOR_PATH
    "${CMAKE_CURRENT_LIST_DIR}/../../database-connector")

add_library(
  postgres_ext_dbconnector OBJECT
  ${POSTGRES_DBCONNECTOR_PATH}/src/query/query_writer.cpp
  # ${POSTGRES_DBCONNECTOR_PATH}/src/table_scan/filter_pushdown.cpp
  # ${POSTGRES_DBCONNECTOR_PATH}/src/table_scan/filter_util.cpp
  # ${POSTGRES_DBCONNECTOR_PATH}/src/optimizer/aggregate_optimizer.cpp
  # ${POSTGRES_DBCONNECTOR_PATH}/src/optimizer/optimizer_util.cpp
  # ${POSTGRES_DBCONNECTOR_PATH}/src/optimizer/order_by_and_limit_optimizer.cpp
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:postgres_ext_dbconnector>
    PARENT_SCOPE)
