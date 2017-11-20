#include "db/vlog_reader.h"
#include "db/vlog_manager.h"
#include "util/coding.h"
#include <iostream>

namespace leveldb {

    VlogManager::VlogManager(uint64_t clean_threshold):clean_threshold_(clean_threshold),now_vlog_(0),cleaning_vlog_(0)
    {}

    VlogManager::~VlogManager()
    {
        std::tr1::unordered_map<uint64_t, VlogInfo>::iterator iter = manager_.begin();
        for(;iter != manager_.end();iter++)
        {
            delete iter->second.vlog_;
        }
    }

    void VlogManager::AddVlog(uint64_t vlog_numb, log::VReader* vlog)
    {
        VlogInfo v;
        v.vlog_ = vlog;
        v.count_ = 0;
 //       bool b = manager_.insert(std::make_pair(vlog_numb, VlogInfo(vlog, 0))).second;
        bool b = manager_.insert(std::make_pair(vlog_numb, v)).second;
        assert(b);
        now_vlog_ = vlog_numb;
    }
//得在单独加一个set nowlog接口,因为dbimpl->recover时最后addDropCount不一定就是now_vlog_
    void VlogManager::SetNowVlog(uint64_t vlog_numb)
    {
        now_vlog_ = vlog_numb;
    }

    void VlogManager::RemoveCleaningVlog()
    {
        assert(cleaning_vlog_>0);
        std::tr1::unordered_map<uint64_t, VlogInfo>::const_iterator iter = manager_.find (cleaning_vlog_);
        delete iter->second.vlog_;
        manager_.erase(iter);
        cleaning_vlog_set_.erase(cleaning_vlog_);
   //     std::cout<<cleaning_vlog_<<" clear "<<std::endl;
        cleaning_vlog_=0;
    }
    void VlogManager::RemoveCleaningVlog(uint64_t vlog_numb)
    {
        std::tr1::unordered_map<uint64_t, VlogInfo>::const_iterator iter = manager_.find (vlog_numb);
        delete iter->second.vlog_;
        manager_.erase(iter);
        cleaning_vlog_set_.erase(vlog_numb);
    }

    void VlogManager::AddDropCount(uint64_t vlog_numb)
    {
         std::tr1::unordered_map<uint64_t, VlogInfo>::iterator iter = manager_.find (vlog_numb);
         if(iter != manager_.end())
         {
            iter->second.count_++;
            if(iter->second.count_ >= clean_threshold_ && vlog_numb != now_vlog_)
            {
 //               std::cout<<vlog_numb<<" clear "<<iter->second.count_<<std::endl;
                cleaning_vlog_set_.insert(vlog_numb);
            }
         }//否则说明该vlog已经clean过了
    }

    std::set<uint64_t> VlogManager::GetVlogsToClean(uint64_t clean_threshold)
    {
        std::set<uint64_t> res;
        std::tr1::unordered_map<uint64_t, VlogInfo>::iterator iter = manager_.begin();
        for(;iter != manager_.end();iter++)
        {
            if(iter->second.count_ >= clean_threshold && iter->first != now_vlog_)
                res.insert(iter->first);
 //           std::cout<<iter->first<< " "<<iter->second.count_<<std::endl;
        }
        return res;
    }

    log::VReader* VlogManager::GetVlog(uint64_t vlog_numb)
    {
        std::tr1::unordered_map<uint64_t, VlogInfo>::const_iterator iter = manager_.find (vlog_numb);
        if(iter == manager_.end())
            return NULL;
        else
            return iter->second.vlog_;
    }

    bool VlogManager::HasVlogToClean()
    {
        return !cleaning_vlog_set_.empty();
    }

 //   log::VReader* VlogManager::GetVlogToClean(uint64_t& vlog_numb)
    uint64_t VlogManager::GetVlogToClean()
    {
        if(cleaning_vlog_ == 0)
        {
            std::tr1::unordered_set<uint64_t>::iterator iter = cleaning_vlog_set_.begin();
            assert(iter != cleaning_vlog_set_.end());
            cleaning_vlog_ = *iter;
        }
        return cleaning_vlog_;
    }
    bool VlogManager::Serialize(std::string& val)
    {
        val.clear();
        uint64_t size = manager_.size();
        if(size == 0)
            return false;

        std::tr1::unordered_map<uint64_t, VlogInfo>::iterator iter = manager_.begin();
        for(;iter != manager_.end();iter++)
        {
            char buf[8];
            EncodeFixed64(buf, (iter->second.count_ << 16) | iter->first);
            val.append(buf, 8);
        }
        return true;
    }

    bool VlogManager::Deserialize(std::string& val)
    {
        Slice input(val);
        while(!input.empty())
        {
            uint64_t code = DecodeFixed64(input.data());
            uint64_t file_numb = code & 0xffff;
            size_t count = code>>16;
            if(manager_.count(file_numb) > 0)//检查manager_现在是否还有该vlog，因为有可能已经删除了
            {
                manager_[file_numb].count_ = count;
                if(count >= clean_threshold_ && file_numb != now_vlog_)
                {
                    cleaning_vlog_set_.insert(file_numb);
                }
            }
            input.remove_prefix(8);
        }
        return true;
    }

    void VlogManager::Recover(uint64_t vlog_numb, uint64_t tail)
    {
        std::tr1::unordered_map<uint64_t, VlogInfo>::iterator iter = manager_.find(vlog_numb);
        if(iter != manager_.end())
        {
            assert(iter->second.count_ >= clean_threshold_);
            iter->second.vlog_->SetCleanPos(tail);
        cleaning_vlog_ = vlog_numb;
        }
    }

}
