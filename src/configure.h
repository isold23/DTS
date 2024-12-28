#ifndef _RDT_CONF_H_
#define _RDT_CONF_H_

#include <libxml2/libxml/parser.h>
#include <libxml2/libxml/xpath.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/socket.h>
#include <fstream>
#include <arpa/inet.h>

// #include "include.h"
#include "rdt_common.h"

namespace rdt
{
    class rdt_conf
    {
    public:
        rdt_conf() {}
        int load(const std::string &conf_file);
        std::string tostring() const;
        int set_progress_to_file(const uint64_t progress);

    private:
        int calculate_server_id();
        uint64_t get_progress_from_file();

    public:
        int m_server_id;
        std::vector<rdt::table_t> m_table_list;
        std::vector<rdt::mysql_master_info> m_master_list;
        uint64_t m_binlog_progress;

    private:
        std::string m_progress_file_name;
    };
}; // end of namespace rdt

#endif //_RDT_CONF_H_
