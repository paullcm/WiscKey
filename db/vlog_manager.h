#ifndef STORAGE_LEVELDB_DB_VLOG_MANAGER_H_
#define STORAGE_LEVELDB_DB_VLOG_MANAGER_H_

//#include <unordered_map>
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#include "db/vlog_reader.h"
#include <set>

namespace leveldb {
    class VlogManager
    {
        public:
            struct VlogInfo{
                log::VReader* vlog_;
                uint32_t count_;//代表该vlog文件垃圾kv的数量
              //  VlogInfo(log::VReader* vlog, uint32_t count):vlog_(vlog),count_(count)
              //  {}
            };

            VlogManager(uint64_t clean_threshold);
            ~VlogManager();

            void AddVlog(uint64_t vlog_numb, log::VReader* vlog);//必须包含头文件才行？
            void RemoveCleaningVlog();
            void RemoveCleaningVlog(uint64_t vlog_numb);
 //           bool Read(uint64_t vlog_numb, char* val, size_t size, size_t pos);
            log::VReader* GetVlog(uint64_t vlog_numb);
            void AddDropCount(uint64_t vlog_numb);
            bool HasVlogToClean();
            uint32_t GetDropCount(uint64_t vlog_numb){return manager_[vlog_numb].count_;}
 //           log::VReader* GetVlogToClean(uint64_t& vlog_numb);
            std::set<uint64_t> GetVlogsToClean(uint64_t clean_threshold);
            uint64_t GetVlogToClean();
            void SetNowVlog(uint64_t vlog_numb);
            bool Serialize(std::string& val);
            bool Deserialize(std::string& val);
            void Recover(uint64_t vlog_numb, uint64_t tail);
        private:
            std::tr1::unordered_map<uint64_t, VlogInfo> manager_;
            std::tr1::unordered_set<uint64_t> cleaning_vlog_set_;
            uint64_t clean_threshold_;
            uint64_t now_vlog_;
            uint64_t cleaning_vlog_;
    };
}

#endif
