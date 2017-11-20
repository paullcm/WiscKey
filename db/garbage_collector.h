
#ifndef STORAGE_LEVELDB_DB_GARBAGE_COLLECTOR_H_
#define STORAGE_LEVELDB_DB_GARBAGE_COLLECTOR_H_

#include "stdint.h"
#include "db/vlog_reader.h"

namespace leveldb{
class VReader;
class DBImpl;

class GarbageCollector
{
    public:
        GarbageCollector(DBImpl* db):db_(db){}
        ~GarbageCollector(){delete vlog_reader_;}
        //void SetVlog(log::VReader* vlog_reader, uint64_t vlog_number, uint64_t garbage_beg_pos=0);
        void SetVlog(uint64_t vlog_number, uint64_t garbage_beg_pos=0);
 //       void SetGarbageBeginPos(uint64_t garbage_beg_pos){garbage_pos_ = garbage_beg_pos;}
  //      void SetGarbageEndPos(uint64_t garbage_end_pos){garbage_end_pos_ = garbage_end_pos;}
        void BeginGarbageCollect();

    private:
        uint64_t vlog_number_;
        uint64_t garbage_pos_;//vlog文件起始垃圾回收的地方
    //    uint64_t garbage_end_pos_;//结束垃圾回收的偏移,暂时没用
//        uint64_t end_count_;//一共需要清除掉多少条记录才会终止
        log::VReader* vlog_reader_;
        DBImpl* db_;
};

}

#endif
