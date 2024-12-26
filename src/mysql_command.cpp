#include "mysql_command.h"

namespace rdt
{
    int com_binlog_dump::packet(char *buf, uint32_t &len)
    {
        if (buf == NULL)
        {
            return -1;
        }

        buf[MYSQL_PACKET_HEADER_LEN] = COM_BINLOG_DUMP;
        intstore(buf + 5, m_binlog_pos, 4);
        intstore(buf + 11, m_server_id, 4);
        memcpy(buf + 15, m_binlog_filename.c_str(), m_binlog_filename.length());
        int payload_len = 11 + m_binlog_filename.length() + 1;
        intstore(buf, payload_len, 3);
        len = payload_len + MYSQL_PACKET_HEADER_LEN;
        return 0;
    }

}; // end of namespace rdt
