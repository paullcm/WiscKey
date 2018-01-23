#include "db/garbage_collector.h"
#include "leveldb/slice.h"
#include "db/write_batch_internal.h"
#include "db/db_impl.h"
#include "fcntl.h"
#include "db/filename.h"
#include <iostream>
namespace leveldb{

void GarbageCollector::SetVlog(uint64_t vlog_number, uint64_t garbage_beg_pos)
{
    SequentialFile* vlr_file;
    db_->options_.env->NewSequentialFile(VLogFileName(db_->dbname_, vlog_number), &vlr_file);
    vlog_reader_ = new log::VReader(vlr_file, true,0);
    vlog_number_ = vlog_number;
    garbage_pos_ = garbage_beg_pos;
}

//void GarbageCollector::BeginGarbageCollect(bool isCleanAll)
void GarbageCollector::BeginGarbageCollect()
{
//    garbage_pos_ = garbage_pos;//garbage_pos_代表从vlog文件的何处开始进行垃圾回收
/*    if(garbage_pos_ >= garbage_end_pos_)*/
    //{
//#ifndef NDEBUG
        //std::cout<<"already clean"<<std::endl;
//#endif
        //return;
    //}

    uint64_t garbage_pos = garbage_pos_;
    Slice record;
    std::string str;
    WriteOptions write_options;
    if(garbage_pos_ > 0)
        assert(vlog_reader_->SkipToPos(garbage_pos_));//从指定位置开始回收

#ifndef NDEBUG
    std::cout<<"begin clean in "<<garbage_pos_<<std::endl;
#endif

    Slice key,value;
    WriteBatch batch, clean_valid_batch;
    std::string val;
    bool isEndOfFile = false;
    while(!db_->IsShutDown())//db关了
    {
        if(!vlog_reader_->ReadRecord(&record, &str))//读日志记录读取失败了
        {
            isEndOfFile = true;
            break;
        }

        garbage_pos_ += log::kVHeaderSize;//因为在readrecord过程中会读到日志头
        WriteBatchInternal::SetContents(&batch, record);//会把record的内容拷贝到batch中去
        ReadOptions read_options;
        uint64_t size = record.size();//size是整个batch的长度，包括batch头
        uint64_t pos = 0;//是相对batch起始位置的偏移
        uint64_t old_garbage_pos = garbage_pos_;
        while(pos < size)//遍历batch看哪些kv有效
        {
            bool isDel = false;
            Status s =WriteBatchInternal::ParseRecord(&batch, pos, key, value, isDel);//解析完一条kv后pos是下一条kv的pos
            assert(s.ok());
            garbage_pos_ = old_garbage_pos + pos;
            if(isDel)//log文件里的delete记录可以直接丢掉，因为sst文件会记录
            {
                val = "del";
            }
            else if(db_->GetPtr(read_options, key, &val).ok())
            {
                uint64_t code = DecodeFixed64(val.data());
                size_t size = code & 0xffffff;
                code = code>>24;
                uint64_t file_numb = code & 0xff;
                uint64_t item_pos = code>>8;
              //  std::string val1;
              //  db_->RealValue(val,&val1);
             //   if(val1 == value.ToString())
                if(item_pos + size == garbage_pos_ && file_numb == vlog_number_ )
                {
                /*    WriteBatch batch;*/
                    //batch.Put(key, value);
                    //Status s = db_->Write(write_options, &batch);
                    /*assert(s.ok());*/
                    clean_valid_batch.Put(key, value);
                }
            }
        }
        assert(pos == size);
        if(WriteBatchInternal::ByteSize(&clean_valid_batch) > db_->options_.clean_write_buffer_size)
        {
            Status s = db_->Write(write_options, &clean_valid_batch);
            assert(s.ok());
            clean_valid_batch.Clear();
        }
//        //一般是读完一条record，即一个batch后更新一下tail，tail位置就是下一条record起始的位置。它包含日志头。
//        db_->Put(write_options, "tail", std::to_string(garbage_pos_));//head不会出现在vlog中，但tail会
    }

#ifndef NDEBUG
 //   std::cout<<"tail is "<<garbage_pos_<<", last key is "<<key.ToString()<<", last v is "<<value.ToString()<<std::endl;
 //   std::cout<<"tail is "<<garbage_pos_<<" last v is "<<value.ToString()<<std::endl;
    if(db_->IsShutDown())
        std::cout<<" tail by shutdown"<<std::endl;
    else
        std::cout<<" tail by read0"<<std::endl;
#endif
        if(WriteBatchInternal::Count(&clean_valid_batch) > 0)
        {
            Status s = db_->Write(write_options, &clean_valid_batch);
            assert(s.ok());
            clean_valid_batch.Clear();
        }

    if(garbage_pos_ - garbage_pos > 0)
    {
        if(isEndOfFile)
        {
            std::string file_name = VLogFileName(db_->dbname_, vlog_number_);
            db_->env_->DeleteFile(file_name);
            std::cout<<"delete vlog "<<vlog_number_<<std::endl;
        }
        else
        {
            assert(vlog_reader_->DeallocateDiskSpace(garbage_pos, garbage_pos_ - garbage_pos));
        char buf[8];
        Slice v(buf, 8);
        EncodeFixed64(buf, (garbage_pos_ << 24) | vlog_number_ );
           std::cout<<"clear vlog "<<vlog_number_<<" in "<<garbage_pos_<<std::endl;
        assert(db_->Put(write_options, "tail", v).ok());//head不会出现在vlog中，但tail会
     //这里有bug，put不一定成功如果是因为数据库正在关闭而退出上述循环，这时候插入tail会失败
     //因为makeroom会返回失败，因为合并操作会将bg_error_设置为io error
        }
    }
}
}
