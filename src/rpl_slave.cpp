#include "rpl_slave.h"

namespace rdt
{
    db_processor::db_processor() : m_ptr_mysql(NULL),
                                   m_recv_buf(NULL),
                                   m_big_buf(NULL)
    {
        m_db_version = 0;
        m_recv_buf_len = 0;
        m_buf_readed_len = 0;
    }

    db_processor::~db_processor()
    {
        if (m_ptr_mysql)
        {
            mysql_close(m_ptr_mysql);
            m_ptr_mysql = NULL;
        }

        if (NULL != m_recv_buf)
        {
            free(m_recv_buf);
            m_recv_buf = NULL;
        }

        if (NULL != m_big_buf)
        {
            free(m_big_buf);
            m_big_buf = NULL;
        }
    }

    uint64_t db_processor::get_event_len(const char *event)
    {
        if (event == NULL)
        {
            return 0;
        }

        uint64_t len = (uint64_t)get_uint64_value(event + 9, 4);
        return len;
    }

    int db_processor::init(mysql_master_info &info,
                           const int server_id,
                           const rdt_conf *ptr_conf)
    {
        if (ptr_conf == NULL)
        {
            LOG_FATAL("db init failed.");
            return -1;
        }

        m_conf = (rdt_conf *)ptr_conf;
        m_master_info = info;
        m_server_id = server_id;
        m_recv_buf = (char *)calloc(MYSQL_MAX_PACKET_LEN * 2, sizeof(char));

        if (NULL == m_recv_buf)
        {
            LOG_FATAL("init master failed, malloc m_recv_buf failed. "
                      "[host='%s:%d' malloc_len=%u]",
                      m_master_info.host.c_str(), m_master_info.port, MYSQL_MAX_PACKET_LEN);
            return -1;
        }

        if (check_version() != 0)
        {
            LOG_FATAL("init master failed, _checkm_db_version failed. [host='%s:%d']",
                      m_master_info.host.c_str(), m_master_info.port);
            return -1;
        }

        LOG_NOTICE("master init succ. master: '%s:%d', ver: %lu, server_id: %d", m_master_info.host.c_str(), m_master_info.port, m_db_version, m_server_id);
        return 0;
    }

    int db_processor::set_checksum()
    {
        char command[1024] = {0};

        if (connect() != 0)
        {
            LOG_WARNING("set checksum failed, [host'%s:%d']. connect failed.",
                        m_master_info.host.c_str(), m_master_info.port);
            return -1;
        }

        snprintf(command, sizeof(command), "%s", "SET @master_binlog_checksum= @@global.binlog_checksum");

        if (mysql_query(m_ptr_mysql, command) != 0)
        {
            LOG_FATAL("set checksum failed, mysql_query failed. "
                      "[host='%s:%d'] sql='%s' err='%s']",
                      m_master_info.host.c_str(), m_master_info.port, command, mysql_error(m_ptr_mysql));
            disconnect();
            return -1;
        }

        return 0;
    }
    MYSQL *db_processor::get_new_connect(MYSQL *newm_ptr_mysql) const
    {
        if (newm_ptr_mysql != NULL)
            return newm_ptr_mysql;
        newm_ptr_mysql = mysql_init(newm_ptr_mysql);

        if (NULL == newm_ptr_mysql)
        {
            return NULL;
        }

        // timeout 10s
        uint32_t timeout = 10;
        int ret = 0;
        ret = mysql_options(newm_ptr_mysql, MYSQL_OPT_WRITE_TIMEOUT, &timeout);

        if (ret != 0)
        {
            LOG_WARNING("set write timeout failed.");
        }

        ret = mysql_options(newm_ptr_mysql, MYSQL_OPT_READ_TIMEOUT, &timeout);

        if (ret != 0)
        {
            LOG_WARNING("set read timeout faild.");
        }

        ret = mysql_options(newm_ptr_mysql, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);

        if (ret != 0)
        {
            LOG_WARNING("set connect timeout faild.");
        }

        bool ml_false = false;
        ret = mysql_options(newm_ptr_mysql, MYSQL_OPT_RECONNECT, &ml_false);

        if (ret != 0)
        {
            LOG_WARNING("set connection not reconnect failed.");
        }

        if (NULL == mysql_real_connect(newm_ptr_mysql, m_master_info.host.c_str(), m_master_info.username.c_str(),
                                       m_master_info.password.c_str(), NULL, m_master_info.port, NULL, 0))
        {
            LOG_WARNING("_connect to mysql failed. [host='%s' user='%s' pass='%s' port=%d], error[%s]",
                        m_master_info.host.c_str(), m_master_info.username.c_str(), m_master_info.password.c_str(), m_master_info.port, mysql_error(newm_ptr_mysql));
            mysql_close(newm_ptr_mysql);
            return NULL;
        }

        return newm_ptr_mysql;
    }
    int db_processor::connect()
    {
        if (m_ptr_mysql != NULL)
        {
            disconnect();
        }

        m_ptr_mysql = mysql_init(m_ptr_mysql);

        if (NULL == m_ptr_mysql)
        {
            LOG_WARNING("mysql_init failed. [host='%s' user='%s' pass='%s' port=%d]",
                        m_master_info.host.c_str(), m_master_info.username.c_str(), m_master_info.password.c_str(), m_master_info.port);
            return -1;
        }

        // timeout 10s
        uint32_t timeout = 10;
        int ret = 0;
        ret = mysql_options(m_ptr_mysql, MYSQL_OPT_WRITE_TIMEOUT, &timeout);

        if (ret != 0)
        {
            LOG_WARNING("set write timeout failed.");
        }

        ret = mysql_options(m_ptr_mysql, MYSQL_OPT_READ_TIMEOUT, &timeout);

        if (ret != 0)
        {
            LOG_WARNING("set read timeout faild.");
        }

        ret = mysql_options(m_ptr_mysql, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);

        if (ret != 0)
        {
            LOG_WARNING("set connect timeout faild.");
        }

        bool ml_false = false;
        ret = mysql_options(m_ptr_mysql, MYSQL_OPT_RECONNECT, &ml_false);

        if (ret != 0)
        {
            LOG_WARNING("set connection not reconnect failed.");
        }

        if (NULL == mysql_real_connect(m_ptr_mysql, m_master_info.host.c_str(), m_master_info.username.c_str(),
                                       m_master_info.password.c_str(), NULL, m_master_info.port, NULL, 0))
        {
            LOG_FATAL("_connect to mysql failed. [host='%s' user='%s' pass='%s' port=%d] error:[%s]",
                      m_master_info.host.c_str(), m_master_info.username.c_str(), m_master_info.password.c_str(), m_master_info.port, mysql_error(m_ptr_mysql));
            mysql_close(m_ptr_mysql);
            m_ptr_mysql = NULL;
            m_socket = -1;
            return -1;
        }

        m_socket = m_ptr_mysql->net.fd;
        return 0;
    }
    void db_processor::disconnect()
    {
        if (m_ptr_mysql)
        {
            mysql_close(m_ptr_mysql);
            m_ptr_mysql = NULL;
            m_socket = -1;
        }

        m_recv_buf_len = 0;
        m_buf_readed_len = 0;
        memset(m_recv_buf, 0, MYSQL_MAX_PACKET_LEN * 2);
    }
    int db_processor::get_db_table_schema(
        const table_t &table,
        std::vector<field_info_t> &field_info_vect, MYSQL *ptr_mysql) const
    {
        // Use Another MYSQL instance
        // Because may update schema when parsing binlog.
        MYSQL *mysql = ptr_mysql;
        MYSQL_RES *res = NULL;
        char command[1024];
        MYSQL_ROW row;
        int status = 0;
        uint32_t field_cnt;
        std::map<std::string, uint32_t> field_is_find;

        if (ptr_mysql == NULL)
        {
            if (NULL == (mysql = get_new_connect(mysql)))
            {
                LOG_FATAL("get_db_table_schema failed, get_new_connect failed.");
                return -1;
            }
        }

        field_info_vect.clear();
        snprintf(command, sizeof(command),
                 "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_TYPE, COLUMN_KEY, EXTRA, PRIVILEGES, COLUMN_COMMENT FROM information_schema.columns WHERE table_schema='%s' AND table_name='%s'",
                 table.db.c_str(), table.table.c_str());

        if (mysql_query(mysql, command) != 0)
        {
            LOG_FATAL("mysql exec sql failed. [host='%s:%d' err='%s' sql='%s']",
                      m_master_info.host.c_str(), m_master_info.port, mysql_error(mysql), command);
            status = -1;
            goto end;
        }

        res = mysql_store_result(mysql);

        if (NULL == res)
        {
            LOG_FATAL("mysql get result failed or result is empty. [host='%s:%d' err='%s' sql='%s']",
                      m_master_info.host.c_str(), m_master_info.port, mysql_error(mysql), command);
            status = -1;
            goto end;
        }

        field_cnt = mysql_num_rows(res);

        if (0 == field_cnt)
        {
            LOG_FATAL("table not found[%s.%s]", table.db.c_str(), table.table.c_str());
            status = 0;
            goto end;
        }

        for (std::set<std::string>::const_iterator ite = table.columns.begin();
             ite != table.columns.end(); ++ite)
        {
            field_is_find[*ite] = 0;
        }

        while (NULL != (row = mysql_fetch_row(res)))
        {
            field_info_t cur_field;
            cur_field.is_proc = false;
            cur_field.id = (uint32_t)strtoul(row[4], NULL, 10);
            cur_field.name = row[3];
            int res = str2type(row[14], &(cur_field.mysql_type),
                               &(cur_field.binloglib_type), &(cur_field.is_sign));

            if (0 != res)
            {
                LOG_FATAL("mysql str2type failed. [host='%s:%d' err='%s' sql='%s']",
                          m_master_info.host.c_str(), m_master_info.port, mysql_error(mysql), command);
                status = -1;
                goto end;
            }

            // field can be null?
            cur_field.can_null = ((row[6] != NULL) &&
                                  (0 == strncasecmp(row[6], "YES", 3)));
            // field is primary key?
            cur_field.is_pk = ((row[15] != NULL) &&
                               (0 == strncasecmp(row[15], "PRI", 3)));
            std::map<std::string, uint32_t>::iterator it_found = field_is_find.find(cur_field.name);
            cur_field.is_proc = it_found != field_is_find.end();

            if (it_found != field_is_find.end())
            {
                it_found->second = 1;
            }

            cur_field.charset = (row[12] != NULL) ? row[12] : "";
            std::set<std::string>::iterator ite = table.monitor_columns.find(cur_field.name);
            cur_field.is_monitor_change = ite != table.monitor_columns.end();
            field_info_vect.push_back(cur_field);
            ++field_cnt;
        }

        for (std::map<std::string, uint32_t>::const_iterator ite = field_is_find.begin();
             ite != field_is_find.end(); ++ite)
        {
            if (0 == ite->second)
            {
                LOG_FATAL("No [field:%s] in [table:%s:%s] in table schema from master.",
                          ite->first.c_str(), table.db.c_str(), table.table.c_str());
            }
        }

    end:
        mysql_free_result(res);

        if (ptr_mysql == NULL)
        {
            mysql_close(mysql);
        }

        return status;
    }

    int db_processor::get_charset_map(std::vector<std::string> &id_charsets)
    {
        MYSQL_RES *res = NULL;
        char command[1024] = {0};
        MYSQL_ROW row;
        id_charsets.assign(0x10000, "");

        if (connect() != 0)
        {
            LOG_WARNING("connect failed, [host'%s:%d']. connect failed.",
                        m_master_info.host.c_str(), m_master_info.port);
            return -1;
        }

        snprintf(command, sizeof(command), "%s", "SHOW COLLATION");

        if (query_master(m_ptr_mysql, command, row, res) != 0)
        {
            LOG_WARNING("get charset map failed.");
            return -1;
        }

        do
        {
            uint32_t charset_id = 0;
            char *end_ptr = NULL;

            if (NULL == row[1] || NULL == row[2])
            {
                continue;
            }

            charset_id = strtol(row[2], &end_ptr, 10);

            if (end_ptr != NULL && *end_ptr != '\0')
            {
                LOG_FATAL("make_charset_map failed, bad mysql charset id. "
                          "[host='%s:%d' charset='%s' err='%s']",
                          m_master_info.host.c_str(), m_master_info.port, row[1], end_ptr);
                goto err;
            }

            id_charsets[charset_id] = row[1];
        } while ((row = mysql_fetch_row(res)) != NULL);

        mysql_free_result(res);
        disconnect();
        return 0;
    err:
        id_charsets.clear();
        mysql_free_result(res);
        disconnect();
        return -1;
    }

    int db_processor::check_version()
    {
        char *ptr = NULL;
        char command[1024] = {0};
        uint64_t version = 0;
        const char *host_name = NULL;
        MYSQL_RES *res = NULL;
        MYSQL_ROW row;

        if (connect() != 0)
        {
            LOG_WARNING("master check version failed, [host'%s:%d']. connect failed.",
                        m_master_info.host.c_str(), m_master_info.port);
            return -1;
        }

        version = mysql_get_server_version(m_ptr_mysql);
        LOG_DEBUG("[master version:%lu]", version);

        if (version < 50100)
        {
            LOG_WARNING("master check version failed, "
                        "[host='%s:%d' err='master version < 5.1']",
                        m_master_info.host.c_str(), m_master_info.port);
            disconnect();
            return -1;
        }

        m_db_version = version;
        host_name = mysql_get_host_info(m_ptr_mysql);

        if ((ptr = (char *)strchr(host_name, '.')) != NULL)
        {
            *ptr = '\0';
        }

        strcpy(command, "SHOW GLOBAL VARIABLES LIKE 'binlog_format'");

        if (query_master(m_ptr_mysql, command, row, res) != 0)
        {
            LOG_WARNING("check version failed.");
            return -1;
        }

        if ((memcmp(row[1], "ROW", strlen("ROW")) != 0) && (memcmp(row[1], "row", strlen("row")) != 0))
        {
            LOG_WARNING("MySQL master replication must be set to 'ROW'. "
                        " [host='%s:%d']",
                        m_master_info.host.c_str(), m_master_info.port);
            mysql_free_result(res);
            disconnect();
            return -1;
        }

        mysql_free_result(res);
        disconnect();
        return 0;
    }

    int db_processor::show_master_status(std::string &binlog_name, uint64_t &offset) const
    {
        MYSQL *mysql = NULL;
        MYSQL_RES *res = NULL;
        MYSQL_ROW row;
        char command[1024] = "SHOW MASTER STATUS";
        int status = 0;

        if (NULL == (mysql = get_new_connect(mysql)))
        {
            LOG_WARNING("show_master_status failed, [host'%s:%d']. get_new_connect failed.",
                        m_master_info.host.c_str(), m_master_info.port);
            return -1;
        }

        if (mysql_query(mysql, command) != 0)
        {
            LOG_WARNING("SHOW MASTER STATUS failed, mysql_query failed. "
                        "[host='%s:%d'] sql='%s' err='%s'",
                        m_master_info.host.c_str(), m_master_info.port, command, mysql_error(mysql));
            status = -1;
            goto end;
        }

        res = mysql_use_result(mysql);

        if (NULL == res)
        {
            LOG_WARNING("SHOW MASTER STATUS failed, mysql_use_result failed."
                        "[host='%s:%d']",
                        m_master_info.host.c_str(), m_master_info.port);
            status = -1;
            goto end;
        }

        row = mysql_fetch_row(res);
        if (NULL == row)
        {
            // if(NULL == row || '\0' == row[0]) {
            LOG_WARNING("SHOW MASTER STATUS failed, mysql fetch row failed."
                        "[host='%s:%d']",
                        m_master_info.host.c_str(), m_master_info.port);
            mysql_free_result(res);
            status = -1;
            goto end;
        }

        binlog_name = row[0];
        offset = (uint64_t)strtoul(row[1], NULL, 10);
        mysql_free_result(res);

        if (offset >= (1UL << 32))
        {
            // offset exceed uint32_t range. Consider show master status failed.
            // because this value can not be used to send binlog dump command.
            // offset in binlog dump command has only 4 bytes!!!
            LOG_WARNING("SHOW MASTER STATUS failed, offset[%lu] exceeds uint32_t range", offset);
            status = -1;
        }

    end:
        mysql_close(mysql);
        return status;
    }

    int db_processor::read_event_ex()
    {
        if (NULL == m_ptr_mysql || NULL == m_recv_buf)
        {
            return READ_EVENT_FAIL;
        }

        if (m_big_buf)
        {
            free(m_big_buf);
            m_big_buf = NULL;
        }

        int ret = 0;
        int sockfd = -1;
        sockfd = m_socket;
        m_recv_buf_len -= m_buf_readed_len;

        if (m_recv_buf_len > 0)
        {
            memmove(m_recv_buf, m_recv_buf + m_buf_readed_len, m_recv_buf_len);
            memset(m_recv_buf + m_recv_buf_len, 0, MYSQL_MAX_PACKET_LEN * 2 - m_recv_buf_len);
        }
        else if (m_recv_buf_len < 0)
        {
            m_recv_buf_len = 0;
            m_buf_readed_len = 0;
            memset(m_recv_buf, 0, MYSQL_MAX_PACKET_LEN * 2);
            return READ_EVENT_FAIL;
        }

        m_buf_readed_len = 0;
        ret = recv(sockfd, m_recv_buf + m_recv_buf_len, MYSQL_MAX_PACKET_LEN, 0);

        if (ret == 0)
        {
            LOG_NOTICE("recv error, errno: %d", errno);
            return READ_EVENT_FAIL;
        }
        else if (ret < 0 && errno == ECONNRESET)
        {
            LOG_NOTICE("recv error, socket connect reset.");
            return READ_EVENT_FAIL;
        }
        else if (ret > 0)
        {
            m_recv_buf_len += ret;
        }

        return READ_EVENT_SUCC;
    }

    int db_processor::read_event()
    {
        int ret = 0;
        int sockfd = -1;
        char header[MYSQL_PACKET_HEADER_LEN] = {0};
        int64_t m_recv_buf_len = 0;
        int64_t event_len = 0;
        uint8_t packet_type;

        if (NULL == m_ptr_mysql || NULL == m_recv_buf)
        {
            return READ_EVENT_FAIL;
        }

        if (m_big_buf)
        {
            free(m_big_buf);
            m_big_buf = NULL;
        }

        sockfd = m_socket;
        ret = recv(sockfd, header, MYSQL_PACKET_HEADER_LEN, 0);

        if (ret != MYSQL_PACKET_HEADER_LEN)
        {
            LOG_WARNING("master read_event failed, get packet header failed. "
                        "[host='%s:%d' get_len=%ld, expect_len=%d]",
                        m_master_info.host.c_str(), m_master_info.port, ret, MYSQL_PACKET_HEADER_LEN);
            return READ_EVENT_FAIL;
        }

        m_recv_buf_len = get_uint64_value(header, 3);
        ret = recv(sockfd, m_recv_buf, m_recv_buf_len, 0);

        if (m_recv_buf_len != ret || 0 == m_recv_buf_len)
        {
            LOG_WARNING("master read_event failed, get packet body failed. "
                        "[host='%s:%d' get_len=%ld, expect_len=%lu]",
                        m_master_info.host.c_str(), m_master_info.port, ret, m_recv_buf_len);
            return READ_EVENT_FAIL;
        }

        packet_type = (uint8_t)m_recv_buf[0];

        if (0 != packet_type)
        {
            if (MYSQL_PACKET_TYPE_ERROR == packet_type)
            {
                m_recv_buf[m_recv_buf_len] = '\0';
                int err_no = get_uint64_value(m_recv_buf + 1, 2);
                LOG_WARNING("receive error packet. [host='%s:%d' "
                            "packet_type=%d errno=%u error_msg='%s']",
                            m_master_info.host.c_str(), m_master_info.port,
                            packet_type, err_no, m_recv_buf + 9);
                return READ_EVENT_FAIL;
            }
            else if (m_recv_buf_len < 8 && MYSQL_PACKET_TYPE_EOF == packet_type)
            {
                LOG_WARNING("receive eof packet, master shutdown. "
                            "[host='%s:%d']",
                            m_master_info.host.c_str(), m_master_info.port);
                return READ_EVENT_FAIL;
            }
            else
            {
                LOG_WARNING("receive unknown type packet. "
                            "[host='%s:%d' packet_type=%d]",
                            m_master_info.host.c_str(), m_master_info.port, packet_type);
                return READ_EVENT_FAIL;
            }
        }

        char *tmp = m_recv_buf;
        event_len = get_event_len(m_recv_buf + 1);

        if (event_len > MYSQL_MAX_PACKET_LEN)
        {
            m_big_buf = (char *)malloc((event_len + 2) * sizeof(char));

            if (!m_big_buf)
            {
                LOG_FATAL("malloc bigm_recv_buf failed. "
                          "[host='%s:%d' size=%lu",
                          m_master_info.host.c_str(), m_master_info.port, event_len + 2);
                return READ_EVENT_FAIL;
            }
            else
            {
                LOG_NOTICE("malloc bigm_recv_buf success. "
                           "[host='%s:%d' size=%lu",
                           m_master_info.host.c_str(), m_master_info.port, event_len + 2);
            }

            memcpy(m_big_buf, m_recv_buf, m_recv_buf_len);
            tmp = m_big_buf;
        }

        if (MYSQL_MAX_PACKET_LEN == (uint32_t)m_recv_buf_len)
        {
            char *cur_pos = tmp + m_recv_buf_len;

            // more packet to be read
            do
            {
                ret = recv(sockfd, header, MYSQL_PACKET_HEADER_LEN, 0);

                if (MYSQL_PACKET_HEADER_LEN != ret)
                {
                    LOG_WARNING("master read_event failed, get packet header failed. "
                                "[host='%s:%d' get_len=%ld, expect_len=%d]",
                                m_master_info.host.c_str(), m_master_info.port,
                                ret, MYSQL_PACKET_HEADER_LEN);
                    return READ_EVENT_FAIL;
                }

                m_recv_buf_len = get_uint64_value(header, 3);
                ret = recv(sockfd, cur_pos, m_recv_buf_len, 0);

                if (m_recv_buf_len != ret || m_recv_buf_len == 0)
                {
                    LOG_WARNING("master read_event failed, get packet body failed. "
                                "[host='%s:%d' get_len=%ld, expect_len=%lu]",
                                m_master_info.host.c_str(), m_master_info.port, ret, m_recv_buf_len);
                    return READ_EVENT_FAIL;
                }

                cur_pos += m_recv_buf_len;
            } while (MYSQL_MAX_PACKET_LEN == (uint32_t)m_recv_buf_len);
        }

        return READ_EVENT_SUCC;
    }

    int db_processor::send_dump_command(
        const std::string &binlog_name,
        const uint64_t binlog_offset) const
    {
        if (m_db_version > 50602)
        {
            db_processor *db = const_cast<db_processor *>(this);

            if (db->set_checksum() != 0)
            {
                LOG_FATAL("set checksum failed.");
                return -1;
            }
        }

        char packet[1024];
        memset(packet, 0, sizeof(packet));
        uint32_t packet_len = 1024;
        com_binlog_dump dump;
        dump.m_binlog_pos = binlog_offset;
        dump.m_server_id = m_server_id;
        dump.m_binlog_filename = binlog_name;
        dump.packet(packet, packet_len);
        int ret = send(m_socket, packet, packet_len, 0);

        if (packet_len != ret)
        {
            LOG_WARNING("send COM_BINLOG_DUMP failed. sended len: %d, must send len: %d", ret, MYSQL_PACKET_HEADER_LEN);
            return -1;
        }

        return 0;
    }

    int db_processor::send_binlog_dump(
        const char *binlog_name,
        uint64_t binlog_offset)
    {
        if (0 != connect())
        {
            LOG_WARNING("send_binlog_dump failed, [host='%s:%d' binlog='%s:%lu']. connect failed.",
                        m_master_info.host.c_str(), m_master_info.port, binlog_name, binlog_offset);
            return -1;
        }

        if (0 != send_dump_command(
                     binlog_name,
                     binlog_offset))
        {
            LOG_WARNING("_send_dump_command failed, "
                        "[host='%s:%d'].",
                        m_master_info.host.c_str(), m_master_info.port);
            return -1;
        }

        // get ok packet, see cli_safe_read
        if (read_event() != 0)
        {
            LOG_WARNING("send binlog dump failed, master binlog information when problem happened. "
                        "[host='%s:%d' binlog='%s:%lu' repl_id=%d]",
                        m_master_info.host.c_str(), m_master_info.port, binlog_name, binlog_offset, m_server_id);
            return -1;
        }
        else
        {
            LOG_NOTICE("send binlog dump succeed, master binlog information. "
                       "[host='%s:%d' binlog='%s:%lu' repl_id=%d]",
                       m_master_info.host.c_str(), m_master_info.port, binlog_name, binlog_offset, m_server_id);
        }

        return 0;
    }
    binlog_processor::binlog_processor() : m_binlog_index(0),
                                           m_binlog_offset(0),
                                           m_safe_offset(BINLOG_START_POS),
                                           m_safe_timestamp(0),
                                           m_schema_change_timestamp(0),
                                           m_server_index(0),
                                           m_transaction_status(-1)
    {
    }

    int binlog_processor::init(mysql_master_info &info, const int server_id, const rdt_conf *conf)
    {
        m_table_list.clear();
        int res = 0;
        res = m_db_proc.init(info, server_id, conf);

        if (0 != res)
        {
            LOG_WARNING("binlog_processor init failed. m_db_processor init failed. res: %d", res);
            return -1;
        }

        if (0 != m_db_proc.show_master_status(m_binlog_name, m_binlog_offset))
        {
            LOG_WARNING("binlog_processor init failed, show_master_status failed."
                        " [host='%s:%d']",
                        info.host, info.port);
            return -1;
        }

        m_binlog_index = get_binlog_index(m_binlog_name, m_binlog_prefix);

        if (0 != m_db_proc.get_charset_map(m_parser.m_parser_info.m_id_charsets))
        {
            LOG_WARNING("binlog_processor init failed. get charset map failed.");
            return -1;
        }

        m_server_index = 0;
        return 0;
    }

    int binlog_processor::subscribe(const table_t &table)
    {
        std::vector<std::string> union_result;
        set_union(table.columns.begin(), table.columns.end(),
                  table.monitor_columns.begin(), table.monitor_columns.end(),
                  std::back_inserter(union_result));

        if (union_result.size() > table.columns.size())
        {
            return -1;
        }

        m_table_list.push_back(table);
        return 0;
    }

    int binlog_processor::get_all_subscribed_tables(
        std::vector<table_t> &table_vect) const
    {
        table_vect = m_table_list;
        return 0;
    }

    int binlog_processor::set_start_pos(
        const char *binlog,
        uint64_t offset)
    {
        m_binlog_name = binlog;
        m_binlog_index = get_binlog_index(std::string(m_binlog_name), m_binlog_prefix);
        m_binlog_offset = offset;
        m_safe_offset = BINLOG_START_POS;
        // set m_safe_timestamp to 0
        m_safe_timestamp = 0;
        m_schema_change_timestamp = 0;
        return 0;
    }

    int binlog_processor::get_latest_pos(
        std::string &binlog,
        uint64_t &offset) const
    {
        binlog = m_binlog_name;
        offset = m_binlog_offset;
        return 0;
    }

    int binlog_processor::update_table_schema(
        const std::string &db_name,
        const std::string &table_name,
        std::vector<field_info_t> &field_info_vect)
    {
        MYSQL *mysql = NULL;

        if (NULL == (mysql = m_db_proc.get_new_connect(mysql)))
        {
            LOG_FATAL("get_db_table_schema failed, get_new_connect failed.");
            return -1;
        }

        for (uint32_t i = 0; i < m_table_list.size(); ++i)
        {
            if (db_name == m_table_list[i].db && table_name == m_table_list[i].table)
            {
                // using a tmp variable for check compatibilty of old and new schema.
                std::vector<field_info_t> tmp_field_info_vect;

                if (0 != m_db_proc.get_db_table_schema(m_table_list[i], tmp_field_info_vect, mysql))
                {
                    LOG_WARNING("get db table schema failed. [db:%s table:%s]",
                                m_table_list[i].db.c_str(), m_table_list[i].table.c_str());

                    if (mysql != NULL)
                    {
                        mysql_close(mysql);
                        mysql = NULL;
                    }

                    return -1;
                }

                bool b_is_compatible = field_info_t::is_compatible(
                    field_info_vect, tmp_field_info_vect);
                field_info_vect.swap(tmp_field_info_vect);

                if (mysql != NULL)
                {
                    mysql_close(mysql);
                    mysql = NULL;
                }

                return b_is_compatible ? 0 : 1;
            }
        }

        if (mysql != NULL)
        {
            mysql_close(mysql);
            mysql = NULL;
        }

        return 0;
    }

    int binlog_processor::read_binlog_event()
    {
        int fd = m_db_proc.m_socket;

        if (-1 == fd)
        {
            LOG_NOTICE("mysql fd is invalid.");
            return READ_EVENT_FAIL;
        }

        fd_set rset;
        fd_set except_set;
        timeval wait_time;
        FD_ZERO(&rset);
        FD_ZERO(&except_set);
        FD_SET(fd, &rset);
        FD_SET(fd, &except_set);
        wait_time.tv_sec = 10;
        int ret = select(fd + 1, &rset, NULL, &except_set, &wait_time);

        if (-1 == ret)
        {
            LOG_WARNING("select failed. %s", get_master_info_str().c_str());
        }
        else if (0 == ret)
        {
            LOG_NOTICE("select timeout. %s", get_master_info_str().c_str());
            return READ_EVENT_TIMEOUT;
        }
        else if (FD_ISSET(fd, &except_set))
        {
            LOG_WARNING("select failed. %s", get_master_info_str().c_str());
        }
        else if (FD_ISSET(fd, &rset))
        {
            return READ_EVENT_SUCC;
        }
        else
        {
            LOG_WARNING("select failed. %s", get_master_info_str().c_str());
        }

        return READ_EVENT_FAIL;
    }

    int binlog_processor::reconnect()
    {
        bool reconnected = false;

        for (int index = 0; index < 5; ++index)
        {
            if (0 != send_binlog_dump())
            {
                LOG_WARNING("send_binlog_dump failed! try_times[%u] master info: [%s]", index, get_master_info_str().c_str());
                continue;
            }

            reconnected = true;
            LOG_NOTICE("reconnect succeed, %s", get_master_info_str().c_str());
            break;
        }

        if (!reconnected)
        {
            LOG_WARNING("_read_binlog_event fail and reconnect failed, %s.",
                        get_master_info_str().c_str());
            return -1;
        }

        return 0;
    }

    int binlog_processor::get_rows(std::list<rdt_record::Record *> &event_list)
    {
        int ret = m_db_proc.read_event_ex();

        if (READ_EVENT_FAIL == ret)
        {
            LOG_NOTICE("read event faild.");
            return READ_EVENT_FAIL;
        }

        char *event = NULL;
        binary_log_event *binlog_event = NULL;
        log_event_header header;

        while (true)
        {
            uint64_t event_len = m_db_proc.get_event_ex(&event);

            if (event == NULL)
            {
                return READ_EVENT_SUCC;
            }

            int parse_res = m_parser.parse_log_event(event, event_len, binlog_event, &header);

            if (0 != parse_res)
            {
                LOG_WARNING("parse event failed. %s", get_master_info_str().c_str());
                return READ_EVENT_FAIL;
            }

            if (ROTATE_EVENT == header.type_code)
            {
                rotate_log_event *rotate_event = (rotate_log_event *)binlog_event;
                m_binlog_name = rotate_event->m_next_binlog_name;
                m_binlog_index = get_binlog_index(m_binlog_name, m_binlog_prefix);
                m_binlog_offset = rotate_event->m_next_binlog_offset;

                if (-1 == m_db_proc.send_binlog_dump(m_binlog_name.c_str(), m_binlog_offset))
                {
                    LOG_WARNING("failed when send_binlog_dump after ROTATE_EVENT."
                                "[%s:%lu]",
                                m_binlog_name.c_str(), BINLOG_START_POS);
                    return READ_EVENT_FAIL;
                }

                LOG_NOTICE("update binlog_parser status after ROTATE_EVENT."
                           "[%s:%lu]",
                           m_binlog_name.c_str(), BINLOG_START_POS);
            }
            else if (0 == header.next_pos)
            {
                // Master will send a format description event after send rotate event.
                // When the position of binlog is not 4, it will send a format description event
                // which next_position value is 0.
                // Do Not update db status now!
                if (FORMAT_DESCRIPTION_EVENT != header.type_code)
                {
                    // Master send an event has next_position equals 0.
                    LOG_WARNING("NOT FORMAT_DESCRIPTION_EVENT has next_position equals 0.%s",
                                header.tostring().c_str());
                }
            }
            else
            {
                // update binlog status, only need update offset.
                m_binlog_offset = header.next_pos;

                if (QUERY_EVENT == header.type_code)
                {
                    query_log_event *query_event = (query_log_event *)binlog_event;

                    if (0 != check_query_log_event(*query_event, event_list))
                    {
                        LOG_WARNING("check_query_log_event failed.");
                        return READ_EVENT_FAIL;
                    }
                }
                else if (XID_EVENT == header.type_code)
                {
                    if (m_transaction_status == 0)
                    {
                        m_transaction_status = 1;
                        xid_log_event *xid_event = (xid_log_event *)binlog_event;

                        if (xid_event != NULL)
                        {
                            m_current_xid = xid_event->m_xid;
                        }
                    }
                    else
                    {
                        LOG_WARNING("transaction dont begin.");
                        std::list<rdt_record::Record *>::iterator iter = m_event_list.begin();

                        for (; iter != m_event_list.end(); ++iter)
                        {
                            rdt_record::Record *ptrevent = NULL;
                            ptrevent = (rdt_record::Record *)*iter;

                            if (ptrevent != NULL)
                            {
                                delete ptrevent;
                                ptrevent = NULL;
                            }
                        }

                        m_event_list.clear();
                    }
                }
                else if (WRITE_ROWS_EVENT == header.type_code || UPDATE_ROWS_EVENT == header.type_code || DELETE_ROWS_EVENT == header.type_code || WRITE_ROWS_EVENT_V2 == header.type_code || UPDATE_ROWS_EVENT_V2 == header.type_code || DELETE_ROWS_EVENT_V2 == header.type_code)
                {
                    rows_log_event *rows_event = (rows_log_event *)binlog_event;

                    if (rows_event->m_row_data_vect.size() != 0)
                    {
                        m_printer.print(this, header, rows_event, m_event_list);
                    }
                }
            }
        }

        return READ_EVENT_SUCC;
    }

    int binlog_processor::get_rows_event(
        std::list<rdt_record::Record *> &event_list)
    {
        int ret = 0;

        for (int read_event_count = 0; read_event_count < 1; ++read_event_count)
        {
            ret = read_binlog_event();

            if (READ_EVENT_SUCC != ret)
            {
                ret = reconnect();
                return ret;
            }

            if (m_event_list.size() > 0 && m_current_xid != m_last_xid)
            {
                m_last_xid = m_current_xid;
                event_list.insert(event_list.end(), m_event_list.begin(), m_event_list.end());
                m_event_list.clear();
            }

            ret = get_rows(event_list);

            if (READ_EVENT_FAIL == ret)
            {
                ret = reconnect();
                return ret;
            }
        }

        return ret;
    }
    int binlog_processor::send_binlog_dump()
    {
        m_parser.m_parser_info.m_table_info_vect.clear();
        m_parser.m_parser_info.m_table_name_index_map.clear();
        table_info_t table_info;
        std::vector<field_info_t> field_info_vect;
        MYSQL *mysql = NULL;

        if (NULL == (mysql = m_db_proc.get_new_connect(mysql)))
        {
            LOG_FATAL("get_db_table_schema failed, get_new_connect failed.");
            return -1;
        }

        for (uint32_t i = 0; i < m_table_list.size(); ++i)
        {
            if (0 != m_db_proc.get_db_table_schema(m_table_list[i],
                                                   table_info.field_info_vect, mysql))
            {
                LOG_FATAL("get db table schema failed. [db:%s table:%s]",
                          m_table_list[i].db.c_str(), m_table_list[i].table.c_str());
                m_db_proc.disconnect();

                if (mysql != NULL)
                {
                    mysql_close(mysql);
                    mysql = NULL;
                }

                return -1;
            }

            table_info.db_name = m_table_list[i].db;
            table_info.table_name = m_table_list[i].table;
            m_parser.m_parser_info.m_table_info_vect.push_back(table_info);
            std::string table_name = table_info.db_name;
            table_name += '.';
            table_name += table_info.table_name;
            m_parser.m_parser_info.m_table_name_index_map[table_name] = i;
        }

        if (mysql != NULL)
        {
            mysql_close(mysql);
            mysql = NULL;
        }

        if (0 != m_db_proc.send_binlog_dump(m_binlog_name.c_str(), m_binlog_offset))
        {
            LOG_WARNING("send_binlog_dump failed. %s", get_master_info_str().c_str());
            return -1;
        }

        return 0;
    }

    int binlog_processor::check_query_log_event(
        const query_log_event &query_event, std::list<rdt_record::Record *> &event_list)
    {
        const char *exec_sql = query_event.m_real_sql.c_str();
        int sql_len = query_event.m_real_sql.length();

        /* filter sql, begin, alter table, create table */
        if (strncasecmp(exec_sql, "BEGIN", 5) == 0)
        {
            if (m_transaction_status == 0)
            {
                std::list<rdt_record::Record *>::iterator iter = m_event_list.begin();

                for (; iter != m_event_list.end(); ++iter)
                {
                    rdt_record::Record *ptrevent = NULL;
                    ptrevent = (rdt_record::Record *)*iter;

                    if (ptrevent != NULL)
                    {
                        delete ptrevent;
                        ptrevent = NULL;
                    }
                }

                m_event_list.clear();
                LOG_WARNING("twice or more begin.");
            }
            else if (m_transaction_status == 1)
            {
                m_last_xid = m_current_xid;
                event_list.insert(event_list.end(), m_event_list.begin(), m_event_list.end());
                m_event_list.clear();
            }

            m_transaction_status = 0;
            m_parser.m_has_table_map = false;
            m_safe_offset = query_event.m_header.next_pos - query_event.m_header.event_length;
            m_safe_timestamp = query_event.m_header.timestamp;
            m_parser.m_parser_info.m_cur_info_vect.clear();
        }
        else if (is_sql_starts_with(exec_sql, "ALTER", "TABLE"))
        {
            /*
                if replicated table is 'ALTER TABLE', which will change our processed field,
                such as 'DROP COLUMN', 'CHANGE COLUMN', so these tables will be reloaded metadata.
                other commands such as 'ADD COLUMN', 'ADD INDEX' ..., will not affect,
                but there is no need to distinguish these cases.
            */
            const char *table_name = exec_sql + 12;

            for (uint32_t i = 0; i < m_parser.m_parser_info.m_table_info_vect.size(); ++i)
            {
                table_info_t &tableinfo = m_parser.m_parser_info.m_table_info_vect[i];

                if (strstr(table_name, tableinfo.table_name.c_str()) != NULL)
                {
                    LOG_NOTICE("'ALTER TABLE' maybe change database schema info.[reload_table='%s'] "
                               "sql='%.*s']",
                               tableinfo.table_name.c_str(), sql_len, exec_sql);
                    int res = update_table_schema(tableinfo.db_name,
                                                  tableinfo.table_name, tableinfo.field_info_vect);

                    if (-1 == res)
                    {
                        return -1;
                    }
                    else if (1 == res)
                    {
                        LOG_WARNING("not compatiable schema change happened.[%s]",
                                    query_event.m_header.tostring().c_str());
                        m_schema_change_timestamp = query_event.m_header.timestamp;
                    }
                }
            }
        }
        else if (is_sql_starts_with(exec_sql, "DROP", "TABLE") ||
                 strncasecmp(exec_sql, "TRUNCATE ", 9) == 0)
        {
            for (uint32_t i = 0; i < m_parser.m_parser_info.m_table_info_vect.size(); ++i)
            {
                const table_info_t &tableinfo = m_parser.m_parser_info.m_table_info_vect[i];

                if (strstr(exec_sql, tableinfo.table_name.c_str()) != NULL)
                {
                    LOG_NOTICE("'DROP|TRUNCATE TABLE' maybe change database schema info."
                               "[reload_table='%s'] [sql='%.*s']",
                               tableinfo.table_name.c_str(), sql_len, exec_sql);
                }
            }
        }
        else if (is_sql_starts_with(exec_sql, "CREATE", "TABLE"))
        {
            for (uint32_t i = 0; i < m_parser.m_parser_info.m_table_info_vect.size(); ++i)
            {
                table_info_t &tableinfo = m_parser.m_parser_info.m_table_info_vect[i];

                if (strstr(exec_sql, tableinfo.table_name.c_str()) != NULL)
                {
                    LOG_WARNING("'CREATE TABLE' maybe change database schema info.[reload_table='%s'] "
                                "sql='%.*s']",
                                tableinfo.table_name.c_str(), sql_len, exec_sql);
                    int res = update_table_schema(tableinfo.db_name,
                                                  tableinfo.table_name, tableinfo.field_info_vect);

                    if (-1 == res)
                    {
                        LOG_NOTICE("update table schema failed.");
                        return -1;
                    }
                    else if (1 == res)
                    {
                        LOG_WARNING("not compatiable schema change happened.[%s]",
                                    query_event.m_header.tostring().c_str());
                        m_schema_change_timestamp = query_event.m_header.timestamp;
                    }
                }
            }
        }
        else if (is_sql_starts_with(exec_sql, "RENAME", "TABLE"))
        {
            for (uint32_t i = 0; i < m_parser.m_parser_info.m_table_info_vect.size(); ++i)
            {
                table_info_t &tableinfo = m_parser.m_parser_info.m_table_info_vect[i];

                if (strstr(exec_sql, tableinfo.table_name.c_str()) != NULL)
                {
                    LOG_WARNING("'RENAME TABLE' maybe change database schema info.[reload_table='%s'] "
                                "sql='%.*s']",
                                tableinfo.table_name.c_str(), sql_len, exec_sql);
                    int res = update_table_schema(tableinfo.db_name,
                                                  tableinfo.table_name, tableinfo.field_info_vect);

                    if (-1 == res)
                    {
                        LOG_NOTICE("update table schema failed.");
                        return -1;
                    }
                    else if (1 == res)
                    {
                        LOG_WARNING("not compatiable schema change happened.[%s]",
                                    query_event.m_header.tostring().c_str());
                        m_schema_change_timestamp = query_event.m_header.timestamp;
                    }
                }
            }
        }
        else if (0 == strncasecmp(exec_sql, "COMMIT", 6))
        {
            if (m_transaction_status == 0)
            {
                m_transaction_status = 1;
            }

            LOG_NOTICE("transaction status: %d, sql:%s", m_transaction_status, exec_sql);
        }
        else
        {
            LOG_NOTICE("unexpected sql in row_event replication. [sql='%.*s']",
                       sql_len, exec_sql);
        }

        return 0;
    }
}