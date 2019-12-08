cmake_minimum_required(VERSION 3.15)
project(minidb)

set(CMAKE_CXX_STANDARD 17)

include_directories(./include ./test ./src)
add_executable(minidb main.cpp include/db.h src/slice.cpp include/slice.h src/db.cpp src/file_util.cpp src/file_util.h src/memtable.h src/skiplist.h src/iterator.h src/format.h src/config.h src/memtable_test.cpp)
add_executable(skip_list_test src/skip_list_test.cpp include/slice.h src/slice.cpp src/config.h src/format.h src/skiplist.h src/error.h)
add_executable(memtable_test src/memtable_test.cpp src/memtable.cpp src/memtable.h src/slice.cpp src/record.h src/comparator.h src src/record.cpp src/comparator.cpp src/log_writer.h src/log_writer.cpp src/db_impl.cpp src/db_impl.h)
add_executable(db_test src/db_test.cpp include/slice.h include/db.h src/record.h src/memtable.h src/memtable.cpp src/record.cpp src/slice.cpp src/skiplist.h
        src/db_impl.h src/db_impl.cpp src/db.cpp src/comparator.h src/comparator.cpp  src/file_util.h src/file_util.cpp src/log_writer.cpp src/log_writer.h src/version.h src/version.cpp src/sstable.h src/sstable.cpp src/sstable_builder.cpp)
add_executable(sstable_test src/sstable_test.cpp src/sstable_builder.cpp src/sstable_builder.h src/file_util.cpp src/file_util.h src/format.h src/config.h src/record.cpp src/record.h src/slice.cpp include/slice.h)