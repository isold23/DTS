#ifndef _MYSQL_COMMOND_H_
#define _MYSQL_COMMOND_H_

#include "dts_common.h"

namespace dts
{

    const static uint32_t MYSQL_PACKET_HEADER_LEN = 4;
    const static uint32_t MYSQL_MAX_PACKET_LEN = 256L * 256L * 256L - 1;
    const static uint32_t MYSQL_PACKET_TYPE_ERROR = 255;
    const static uint32_t MYSQL_PACKET_TYPE_EOF = 254;

    enum enum_server_command
    {
        COM_SLEEP,
        COM_QUIT,
        COM_INIT_DB,
        COM_QUERY,
        COM_FIELD_LIST,
        COM_CREATE_DB,
        COM_DROP_DB,
        COM_REFRESH,
        COM_SHUTDOWN,
        COM_STATISTICS,
        COM_PROCESS_INFO,
        COM_CONNECT,
        COM_PROCESS_KILL,
        COM_DEBUG,
        COM_PING,
        COM_TIME,
        COM_DELAYED_INSERT,
        COM_CHANGE_USER,
        COM_BINLOG_DUMP,
        COM_TABLE_DUMP,
        COM_CONNECT_OUT,
        COM_REGISTER_SLAVE,
        COM_STMT_PREPARE,
        COM_STMT_EXECUTE,
        COM_STMT_SEND_LONG_DATA,
        COM_STMT_CLOSE,
        COM_STMT_RESET,
        COM_SET_OPTION,
        COM_STMT_FETCH,
        COM_DAEMON,
        COM_BINLOG_DUMP_GTID,
        // Must be last
        COM_END
    };

    class mysql_command
    {
    public:
        explicit mysql_command(enum_server_command type) : m_type(type) {}
        virtual ~mysql_command() {}
        virtual int packet(char *buf, uint32_t &len)
        {
            return 0;
        }

    protected:
        enum_server_command m_type;
    };

    class com_binlog_dump : public mysql_command
    {
    public:
        com_binlog_dump() : mysql_command(COM_BINLOG_DUMP) {}
        ~com_binlog_dump() {}
        int packet(char *buf, uint32_t &len);

    public:
        // position in the binlog-file to start the stream with
        uint32_t m_binlog_pos;
        // 0x01 BINLOG_DUMP_NON_BLOCK(if there is no more event to send, send a EOF_Packet instead of blocking the connection)
        short m_flags;
        // server id of this slave
        uint32_t m_server_id;
        // filename of the binlog on the master
        std::string m_binlog_filename;
    };
}; // end of namespace rdt

#endif //_MYSQL_COMMOND_H_
