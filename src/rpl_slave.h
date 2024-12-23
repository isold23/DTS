#ifndef _RDT_PROCESSOR_H_
#define _RDT_PROCESSOR_H_

// #include "libnet.h"
#include "mysql.h"
#include "rdt_common.h"
#include "mysql_command.h"
#include "binlog_parser.h"
#include "binlog_printer.h"

namespace rdt
{
    enum read_status_code_t
    {
        READ_EVENT_SUCC = 0,
        READ_EVENT_FAIL,
        READ_EVENT_TIMEOUT
    };

    class db_processor
    {
    public:
        db_processor();
        ~db_processor();
        int init(mysql_master_info &info,
                 const int server_id,
                 const rdt_conf *ptr_conf);
        int get_db_table_schema(
            const table_t &table,
            std::vector<field_info_t> &field_info_vect, MYSQL *in_mysql = NULL) const;
        int get_charset_map(std::vector<std::string> &id_charsets);
        int read_event();
        int read_event_ex();
        inline uint64_t get_event_ex(char **event)
        {
            int64_t event_len = 0;
            char *packet = m_big_buf ? m_big_buf : m_recv_buf;

            // 4+1+9+4
            if (m_recv_buf_len - m_buf_readed_len > 18)
            {
                if (packet == NULL)
                {
                    *event = NULL;
                }
                else
                {
                    *event = packet + 5 + m_buf_readed_len;
                    event_len = get_event_len(*event);

                    if (m_recv_buf_len >= (event_len + m_buf_readed_len + 5))
                    {
                        m_buf_readed_len += 5;
                        m_buf_readed_len += event_len;
                    }
                    else
                    {
                        event_len = 0;
                        *event = NULL;
                    }
                }
            }
            else
            {
                *event = NULL;
            }

            return event_len;
        }

        int send_binlog_dump(const char *binlog, uint64_t offset);
        int show_master_status(std::string &binlog_name, uint64_t &offset) const;
        MYSQL *get_new_connect(MYSQL *new_mysql) const;
        void disconnect();

        int query_master(MYSQL *mysql_instance, const char *command, MYSQL_ROW &row, MYSQL_RES *&res)
        {
            if (mysql_instance == NULL)
            {
                return -1;
            }

            if (mysql_query(mysql_instance, command) != 0)
            {
                LOG_FATAL("mysql_query failed. "
                          "[host='%s:%d'] sql='%s' err='%s']",
                          m_master_info.host.c_str(), m_master_info.port, command, mysql_error(mysql_instance));
                disconnect();
                return -1;
            }

            res = mysql_use_result(mysql_instance);

            if (NULL == res)
            {
                LOG_FATAL("mysql_use_result failed."
                          "[host='%s:%d']",
                          m_master_info.host.c_str(), m_master_info.port);
                disconnect();
                return -1;
            }

            row = mysql_fetch_row(res);

            if (NULL == row)
            {
                // if(NULL == row || '\0' == row[0]) {
                LOG_FATAL("mysql fetch row failed."
                          "[host='%s:%d']",
                          m_master_info.host.c_str(), m_master_info.port);
                mysql_free_result(res);
                disconnect();
                return -1;
            }

            return 0;
        }

    private:
        static uint64_t get_event_len(const char *event);
        int check_version();
        int set_checksum();
        int connect();
        int send_dump_command(const std::string &binlog_name, const uint64_t binlog_offset) const;

    public:
        char *m_recv_buf;
        char *m_big_buf;
        int m_socket;
        int m_server_id;
        uint64_t m_db_version;
        int64_t m_recv_buf_len;
        int64_t m_buf_readed_len;
        rdt_conf *m_conf;
        MYSQL *m_ptr_mysql;
        mysql_master_info m_master_info;
    };

    class binlog_processor
    {
    public:
        binlog_processor();
        ~binlog_processor() {}

        int init(mysql_master_info &info, const int server_id, const rdt_conf *conf);
        int subscribe(const table_t &table);
        int get_all_subscribed_tables(std::vector<table_t> &table_vect) const;
        int set_start_pos(const char *binlog, uint64_t offset);
        int get_latest_pos(std::string &file, uint64_t &offset) const;
        int get_rows_event(std::list<rdt_record::Record *> &event_list);
        int get_rows(std::list<rdt_record::Record *> &event_list);
        int send_binlog_dump();
        int update_table_schema(const std::string &db_name, const std::string &table_name, std::vector<field_info_t> &field_info_vect);
        inline const std::string &get_host() const
        {
            return m_db_proc.m_master_info.host;
        }

        inline int get_port() const
        {
            return m_db_proc.m_master_info.port;
        }

        std::string get_master_info_str() const
        {
            char buffer[400] = {0};
            snprintf(buffer, 400, "host='%s:%d' binlog='%s:%lu'",
                     get_host().c_str(), get_port(),
                     m_binlog_name.c_str(), m_binlog_offset);
            return buffer;
        }

        static uint32_t get_binlog_index(const std::string &binlog, std::string &prefix)
        {
            std::string::size_type index = binlog.rfind('.');
            uint32_t ret_val = strtoul(binlog.c_str() + index + 1, NULL, 10);
            prefix = binlog.substr(0, index);
            return ret_val;
        }

        static int make_binlog_name(uint32_t binlog_index,
                                    std::string &binlog_name,
                                    const std::string &binlog_prefix)
        {
            binlog_name = binlog_prefix;
            binlog_name += '.';
            char index_str[30] = {0};
            snprintf(index_str, sizeof(index_str), "%06d", binlog_index);
            binlog_name += index_str;
            return 0;
        }

        int make_binlog_name(uint32_t binlog_index, std::string &binlog_name) const
        {
            return make_binlog_name(binlog_index, binlog_name, m_binlog_prefix);
        }

        static uint64_t make_progress(
            uint32_t binlog_index,
            uint32_t binlog_offset,
            uint32_t server_index)
        {
            uint64_t progress = binlog_offset;
            progress |= (((uint64_t)binlog_index) << 32);
            progress |= (((uint64_t)server_index) << 63);
            return progress;
        }
        uint64_t make_progress() const
        {
            return make_progress(m_binlog_index, m_safe_offset, m_server_index);
        }

        static int parse_progress(
            uint64_t progress,
            uint32_t &binlog_index,
            uint32_t &binlog_offset,
            uint32_t &server_index)
        {
            binlog_offset = progress & (uint64_t)(uint32_t)(-1);
            progress >>= 32;
            binlog_index = progress & 0x7fffffff;
            progress >>= 31;
            server_index = progress;
            return 0;
        }
        int init_server_index(int server_index)
        {
            if (server_index < 0 || server_index > 1)
            {
                return -1;
            }

            m_server_index = server_index;
            return 0;
        }
        int reconnect();

    private:
        int read_binlog_event();
        int check_query_log_event(
            const query_log_event &query_log_event, std::list<rdt_record::Record *> &event_list);

    public:
        db_processor m_db_proc;
        binlog_parser m_parser;
        pb_printer m_printer;
        std::vector<table_t> m_table_list;
        std::string m_binlog_name;
        std::string m_binlog_prefix;
        uint32_t m_binlog_index;
        uint64_t m_binlog_offset;
        uint64_t m_safe_offset;
        uint32_t m_safe_timestamp;
        uint32_t m_schema_change_timestamp;
        uint32_t m_server_index;
        mysql_master_info m_master_info;
        // 0 begin 1 commit 2 redo
        uint32_t m_transaction_status;
        std::list<rdt_record::Record *> m_event_list;
        uint64_t m_current_xid;
        uint64_t m_last_xid;
    };

}; // end of namespace rdt
#endif //_RDT_PROCESSOR_H_
