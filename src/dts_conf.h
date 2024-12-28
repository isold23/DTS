#ifndef _dts_CONF_H_
#define _dts_CONF_H_

#include <libxml2/libxml/parser.h>
#include <libxml2/libxml/xpath.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/socket.h>
#include <fstream>
#include <arpa/inet.h>

// #include "include.h"
#include "dts_common.h"

namespace dts
{
    class dts_conf
    {
    public:
        dts_conf() {}
        int load(const std::string &conf_file);
        std::string tostring() const;
        int set_progress_to_file(const uint64_t progress);

    private:
        int calculate_server_id();
        uint64_t get_progress_from_file();

    public:
        int m_server_id;
        std::vector<dts::table_t> m_table_list;
        std::vector<dts::mysql_master_info> m_master_list;
        uint64_t m_binlog_progress;

    private:
        std::string m_progress_file_name;
    };
}; // end of namespace dts

#endif //_dts_CONF_H_
