// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/vlog_reader.h"
#include "db/vlog_writer.h"
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/random.h"
#include "util/testharness.h"

namespace leveldb {
namespace log {

// Construct a string of the specified length made out of the supplied
// partial string.
static std::string BigString(const std::string& partial_string, size_t n) {
  std::string result;
  while (result.size() < n) {
    result.append(partial_string);
  }
  result.resize(n);
  return result;
}

// Construct a string from a number
static std::string NumberString(int n) {
  char buf[50];
  snprintf(buf, sizeof(buf), "%d.", n);
  return std::string(buf);
}

// Return a skewed potentially long string
static std::string RandomSkewedString(int i, Random* rnd) {
  return BigString(NumberString(i), rnd->Skewed(17));
}

class VlogTest {
 private:
  class StringDest : public WritableFile {
   public:
    std::string contents_;

    virtual Status Close() { return Status::OK(); }
    virtual Status Flush() { return Status::OK(); }
    virtual Status Sync() { return Status::OK(); }
    virtual Status Append(const Slice& slice) {
      contents_.append(slice.data(), slice.size());
      return Status::OK();
    }
  };

  class StringSource : public SequentialFile {
   public:
    Slice contents_;
    std::string* data;
    bool force_error_;
    bool returned_partial_;
    StringSource() : force_error_(false), returned_partial_(false) { }

    virtual Status Read(size_t n, Slice* result, char* scratch) {
      ASSERT_TRUE(!returned_partial_) << "must not Read() after eof/error";

      if (force_error_) {
        force_error_ = false;
        returned_partial_ = true;
        return Status::Corruption("read error");
      }

      if (contents_.size() < n) {
        n = contents_.size();
        returned_partial_ = true;
      }
      memcpy(scratch, contents_.data(), n);
      *result = Slice(scratch, n);
//      *result = Slice(contents_.data(), n);
      contents_.remove_prefix(n);
      return Status::OK();
    }

    virtual Status Skip(uint64_t n) {
      if (n > contents_.size()) {
        contents_.clear();
        return Status::NotFound("in-memory file skipped past end");
      }

      contents_.remove_prefix(n);

      return Status::OK();
    }

    virtual Status SkipFromHead(uint64_t n)
    {
      if (n > data->size()) {
        contents_.clear();
        return Status::NotFound("in-memory file SkipFromHead past end");
      }

        contents_ = Slice((char*)data->data() + n, data->size() - n);
        return Status::OK();
    }

  };

  class ReportCollector : public VReader::Reporter {
   public:
    size_t dropped_bytes_;
    std::string message_;

    ReportCollector() : dropped_bytes_(0) { }
    virtual void Corruption(size_t bytes, const Status& status) {
      dropped_bytes_ += bytes;
      message_.append(status.ToString());
    }
  };

  StringDest dest_;
  StringSource source_;
  ReportCollector report_;
  bool reading_;
  VWriter* writer_;
  VReader* reader_;

  // Record metadata for testing initial offset functionality
  static size_t initial_offset_record_sizes_[];
  static uint64_t initial_offset_last_record_offsets_[];
  static int num_initial_offset_records_;

 public:
  VlogTest() : reading_(false),
              writer_(new VWriter(&dest_)),
              reader_(new VReader(&source_, &report_, true/*checksum*/,
                      0/*initial_offset*/)) {
  }

  ~VlogTest() {
    delete writer_;
    delete reader_;
  }

  void ReopenForAppend() {
    delete writer_;
//    writer_ = new Writer(&dest_, dest_.contents_.size());
    writer_ = new VWriter(&dest_);//符合dest_的append语义
  }

  void Write(const std::string& msg) {
    ASSERT_TRUE(!reading_) << "Write() after starting to read";
    writer_->AddRecord(Slice(msg));
  }

  size_t WrittenBytes() const {
    return dest_.contents_.size();
  }

  std::string Read() {
    if (!reading_) {
      reading_ = true;
      source_.contents_ = Slice(dest_.contents_);
      source_.data = &dest_.contents_;
    }
    std::string scratch;
    Slice record;
    if (reader_->ReadRecord(&record, &scratch)) {
      return record.ToString();
    } else {
      return "EOF";
    }
  }

  void IncrementByte(int offset, int delta) {
    dest_.contents_[offset] += delta;
  }

  void SetByte(int offset, char new_byte) {
    dest_.contents_[offset] = new_byte;
  }

  void ShrinkSize(int bytes) {
    dest_.contents_.resize(dest_.contents_.size() - bytes);
  }

  void FixChecksum(int header_offset, int len) {
    // Compute crc of type/len/data
//    uint32_t crc = crc32c::Value(&dest_.contents_[header_offset+6], 1 + len);
    uint32_t crc = crc32c::Value(&dest_.contents_[header_offset+7], len);
    crc = crc32c::Mask(crc);
    EncodeFixed32(&dest_.contents_[header_offset], crc);
  }

  void ForceError() {
    source_.force_error_ = true;
  }

  size_t DroppedBytes() const {
    return report_.dropped_bytes_;
  }

  std::string ReportMessage() const {
    return report_.message_;
  }

  // Returns OK iff recorded error message contains "msg"
  std::string MatchError(const std::string& msg) const {
    if (report_.message_.find(msg) == std::string::npos) {
      return report_.message_;
    } else {
      return "OK";
    }
  }
//按照初始化好的长度写入num_initial_offset_records_条记录
  void WriteInitialOffsetLog() {
    for (int i = 0; i < num_initial_offset_records_; i++) {
      std::string record(initial_offset_record_sizes_[i],
                         static_cast<char>('a' + i));
      Write(record);
    }
  }
//从指定偏移处创建vlog_reader
  void StartReadingAt(uint64_t initial_offset) {
    delete reader_;
    reader_ = new VReader(&source_, &report_, true/*checksum*/, initial_offset);
  }
//从超过vlog文件大小的偏移处读记录会失败
  void CheckOffsetPastEndReturnsNoRecords(uint64_t offset_past_end) {
    WriteInitialOffsetLog();
    reading_ = true;
    source_.contents_ = Slice(dest_.contents_);
    VReader* offset_reader = new VReader(&source_, &report_, true/*checksum*/,
                                       WrittenBytes() + offset_past_end);
    Slice record;
    std::string scratch;
    ASSERT_TRUE(!offset_reader->ReadRecord(&record, &scratch));
    delete offset_reader;
  }
//从initial_offset偏移处创建vlog_reader,读取num_initial_offset_records_ - expected_record_offset条记录
//验证读取的每条记录就是初始化时写入的记录，expected_record_offset代表的是从第几条初始化记录开始验证
  void CheckInitialOffsetRecord(uint64_t initial_offset,
                                int expected_record_offset) {
    WriteInitialOffsetLog();
    reading_ = true;
    source_.contents_ = Slice(dest_.contents_);
    VReader* offset_reader = new VReader(&source_, &report_, true/*checksum*/,
                                       initial_offset);

    // Read all records from expected_record_offset through the last one.
    ASSERT_LT(expected_record_offset, num_initial_offset_records_);
    for (; expected_record_offset < num_initial_offset_records_;
         ++expected_record_offset) {
      Slice record;
      std::string scratch;
      ASSERT_TRUE(offset_reader->ReadRecord(&record, &scratch));
      ASSERT_EQ(initial_offset_record_sizes_[expected_record_offset],
                record.size());
      ASSERT_EQ((char)('a' + expected_record_offset), record.data()[0]);
    }
    delete offset_reader;
  }
//这个是测试vlog_read的read接口
  void CheckReadRecord() {
    WriteInitialOffsetLog();
    reading_ = true;
    source_.contents_ = Slice(dest_.contents_);
    VReader* offset_reader = new VReader(&source_, &report_, true/*checksum*/);

    // Read all records from expected_record_offset through the last one.
    int expected_record_offset = 0;
    ASSERT_LT(expected_record_offset, num_initial_offset_records_);
    size_t pos = 0;
    char buf[3*kBlockSize];
    for (; expected_record_offset < num_initial_offset_records_;
         ++expected_record_offset) {
      std::string scratch;
      pos += kHeaderSize;
      ASSERT_TRUE(offset_reader->Read(buf, initial_offset_record_sizes_[expected_record_offset], pos));
      ASSERT_EQ((char)('a' + expected_record_offset), buf[0]);
      ASSERT_EQ((char)('a' + expected_record_offset), buf[initial_offset_record_sizes_[expected_record_offset]-1]);
      pos += initial_offset_record_sizes_[expected_record_offset];
    }

      ASSERT_TRUE(offset_reader->Read(buf, initial_offset_record_sizes_[1], log::kBlockSize + kHeaderSize));
      ASSERT_EQ((char)('b'), buf[0]);
      ASSERT_EQ((char)('b'), buf[initial_offset_record_sizes_[1]-1]);

      ASSERT_TRUE(!offset_reader->Read(buf,log::kBlockSize, pos - 100));
    delete offset_reader;
  }
};

size_t VlogTest::initial_offset_record_sizes_[] =
    {log::kBlockSize - kHeaderSize,//刚好占满一个block
    log::kBlockSize - kHeaderSize - 2,//用于一个block的剩余空间不足kheadsize
     100,//slice会回退2，读取kBlockSize - 2个字节到读缓冲区中
     100,//record完全在刚读的读缓冲区中
     log::kBlockSize,//读完读缓冲区内容后， 该条记录剩余待读部分小于kblocksize/2
     2 * log::kBlockSize - 1000,  //读完读缓冲区内容后， 剩余部分大于kblocksize/2
     1
    };

// LogTest::initial_offset_last_record_offsets_ must be defined before this.
int VlogTest::num_initial_offset_records_ =
    sizeof(VlogTest::initial_offset_record_sizes_)/sizeof(size_t);

TEST(VlogTest, Empty) {
  ASSERT_EQ("EOF", Read());
}

TEST(VlogTest, ReadWrite) {
  Write("foo");
  Write("bar");
  Write("");
  Write("xxxx");
  ASSERT_EQ("foo", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("xxxx", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("EOF", Read());  // Make sure reads at eof work
}

TEST(VlogTest, ManyBlocks) {
  for (int i = 0; i < 100; i++) {
    Write(NumberString(i));
  }
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(NumberString(i), Read());
  }
  ASSERT_EQ("EOF", Read());
}

TEST(VlogTest, MarginalTrailer) {
  // Make a trailer that is exactly the same length as an empty record.
  const int n = kBlockSize - 2*kHeaderSize;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize, WrittenBytes());
  Write("");
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}


TEST(VlogTest, MarginalTrailer2) {
  // Make a trailer that is exactly the same length as an empty record.
  const int n = kBlockSize - 2*kHeaderSize;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize, WrittenBytes());
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST(VlogTest, ShortTrailer) {
  const int n = kBlockSize - 2*kHeaderSize + 4;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize + 4, WrittenBytes());//block剩下的空间不够容纳一个head
  Write("");
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

TEST(VlogTest, AlignedEof) {
  const int n = kBlockSize - 2*kHeaderSize + 4;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize + 4, WrittenBytes());
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("EOF", Read());
}

TEST(VlogTest, OpenForAppend) {
  Write("hello");
  ReopenForAppend();
  Write("world");
  ASSERT_EQ("hello", Read());
  ASSERT_EQ("world", Read());
  ASSERT_EQ("EOF", Read());
}

TEST(VlogTest, RandomRead) {
  const int N = 500;
  Random write_rnd(301);
  for (int i = 0; i < N; i++) {
    Write(RandomSkewedString(i, &write_rnd));
  }
  Random read_rnd(301);
  for (int i = 0; i < N; i++) {
    ASSERT_EQ(RandomSkewedString(i, &read_rnd), Read());
  }
  ASSERT_EQ("EOF", Read());
}

// Tests of all the error paths in log_reader.cc follow:

TEST(VlogTest, ReadError) {
  Write("foo");
  ForceError();
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(kBlockSize, DroppedBytes());
  ASSERT_EQ("OK", MatchError("read error"));
}


TEST(VlogTest, TruncatedTrailingRecordIsIgnored) {
  Write("foo");
  ShrinkSize(4);   // Drop all payload as well as a header byte
  ASSERT_EQ("EOF", Read());
  // Truncated last record is ignored, not treated as an error.
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}


TEST(VlogTest, BadLengthAtEndIsIgnored) {
  Write("foo");
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST(VlogTest, ChecksumMismatch) {
  Write("foo");
  IncrementByte(0, 10);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(10, DroppedBytes());
  ASSERT_EQ("OK", MatchError("checksum mismatch"));
}

TEST(VlogTest, PartialLastIsIgnored1) {
  Write(BigString("bar", kBlockSize));//没有超过blocksize/2
  // Cause a bad record length in the LAST block.
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0, DroppedBytes());
}
TEST(VlogTest, PartialLastIsIgnored2) {
  Write(BigString("bar", 2*kBlockSize));//超过blocksize/2
  // Cause a bad record length in the LAST block.
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0, DroppedBytes());
}

TEST(VlogTest, ReadStart) {
  CheckInitialOffsetRecord(0, 0);
}

TEST(VlogTest, ReadSecondOneOff) {
  CheckInitialOffsetRecord(kBlockSize, 1);
}

TEST(VlogTest, ReadThirdOneOff) {
  CheckInitialOffsetRecord(kBlockSize + kBlockSize - 2, 2);
}

TEST(VlogTest, ReadFourthOneOff) {
  CheckInitialOffsetRecord(kBlockSize + kBlockSize - 2 + 100 + kHeaderSize, 3);
}


TEST(VlogTest, ReadEnd) {
  CheckOffsetPastEndReturnsNoRecords(0);
}

TEST(VlogTest, ReadPastEnd) {
  CheckOffsetPastEndReturnsNoRecords(5);
}

TEST(VlogTest, Read) {
    CheckReadRecord();
}

}  // namespace log
}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
