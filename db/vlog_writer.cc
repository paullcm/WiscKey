// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/vlog_writer.h"

#include <stdint.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

VWriter::VWriter(WritableFile* dest)
    : dest_(dest){
}

VWriter::~VWriter() {
}

Status VWriter::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();
  char buf[kVHeaderSize];//日志记录头4个字节crc，后3个字节记录日志记录长度
  buf[4] = static_cast<char>(left & 0xff);
  buf[5] = static_cast<char>(left >> 8);
  buf[6]= static_cast<char>(left >> 16);//说明batch的长度不能超过2^24,即16M
  //db_impl.cc中BuildBatchGroup对batch的限制是1M
  uint32_t crc = crc32c::Extend(0, ptr, left);
  crc = crc32c::Mask(crc);                 // Adjust for storage
  EncodeFixed32(buf, crc);

  Status s = dest_->Append(Slice(buf, kVHeaderSize));
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, left));//写一条物理记录就刷一次
    if (s.ok()) {
      s = dest_->Flush();
    }
  }
  return s;
}

}  // namespace log
}  // namespace leveldb
