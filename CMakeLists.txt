SET(MRUBY_SOURCES ha_mruby.cc)
SET(MRUBY_LIBRARIES -lmruby)
LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/storage/mruby/mruby/build/host/lib)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/storage/mruby/mruby/include)
MYSQL_ADD_PLUGIN(mruby ${MRUBY_SOURCES} STORAGE_ENGINE MODULE_ONLY LINK_LIBRARIES ${MRUBY_LIBRARIES})
