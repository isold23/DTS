#ifndef _BINLOG_WORKER_H_
#define _BINLOG_WORKER_H_

#include "rpl_slave.h"
#include "record_process.h"

namespace dts
{

    enum publish_state
    {
        ERROR_STATE,
        PUBLISHING_STATE,
        STOPPED_STATE
    };

    class binlog_worker
    {
    public:
        binlog_worker() {}
        ~binlog_worker()
        {
            destroy();
        }
        int init(std::string &conf_file);
        int run();
        int destroy();

    private:
        int process();
        int set_progress(uint64_t progress);
        int set_current_progress(uint64_t progress);
        int set_current_master_by_location(mysql_master_info &info);
        int process_record();

    public:
        dts_conf m_conf;
        std::vector<binlog_processor *> m_binlog_processor_vect;
        binlog_processor *m_current_processor;
        int m_publish_state;
        record_process m_record_process;
        std::list<dts_record::Record *> m_event_list;
    };
} // end of namespace dts

#endif // end of _BINLOG_WORKER_H_
