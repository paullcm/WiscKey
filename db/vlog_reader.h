// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VLOG_READER_H_
#define STORAGE_LEVELDB_DB_VLOG_READER_H_

#include <stdint.h>

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "port/port.h"
namespace leveldb {

class SequentialFile;

namespace log {
class VReader {
 public:
  class Reporter {
   public:
    virtual ~Reporter();

    // Some corruption was detected.  "size" is the approximate number
    // of bytes dropped due to the corruption.
    virtual void Corruption(size_t bytes, const Status& status) = 0;
  };

  VReader(SequentialFile* file, bool checksum,
         uint64_t initial_offset=0);
  VReader(SequentialFile* file, Reporter* reporter, bool checksum,
         uint64_t initial_offset=0);

  ~VReader();

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input.  May use
  // "*scratch" as temporary storage.  The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
//  bool ReadRecord(Slice* record, std::string* scratch);
  bool Read(char* val, size_t size, size_t pos);//从文件pos偏移读取size长的内容给val
//  Status BeginRecover(char* buf, size_t size, uint64_t pos);
//  void RecoverOk();
//  bool ReadRecord(Slice* record);
  //读取一条完整的日志记录到record，record的内容可能在scratch，也可能在backing_store_中
  bool ReadRecord(Slice* record, std::string* scratch);
  bool SkipToPos(size_t pos);//跳到文件指定偏移
  void SetCleanPos(uint64_t pos){cleanPos_ = pos;}
  uint64_t GetCleanPos(){return cleanPos_;}
  bool IsEnd();//是否读到尾
  bool DeallocateDiskSpace(uint64_t offset, size_t len);//释放offset偏移处len长的磁盘空间
 private:
  port::Mutex mutex_;
  SequentialFile* const file_;//要读的文件
  Reporter* const reporter_;//用于报告错误的
  bool const checksum_;//是否进行数据校验
  char* const backing_store_;//读缓冲区
  Slice buffer_;//读缓冲区的封装，便于表示当前读缓冲区待读部分
  bool eof_;   // Last Read() indicated EOF by returning < kBlockSize//是否读到文件尾了
  uint64_t cleanPos_;
  // Reports dropped bytes to the reporter.
  // buffer_ must be updated to remove the dropped bytes prior to invocation.
  void ReportCorruption(uint64_t bytes, const char* reason);
  void ReportDrop(uint64_t bytes, const Status& reason);
  // No copying allowed
  VReader(const VReader&);
  void operator=(const VReader&);
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_READER_H_
