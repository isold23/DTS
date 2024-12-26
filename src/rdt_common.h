#ifndef _MYSQL_BINLOG_COMMON_H_
#define _MYSQL_BINLOG_COMMON_H_

#include <glog/logging.h>
#include <iostream>
#include <cstdarg>
#include <string>
#include <sstream>
#include <cassert>
#include <list>
#include <vector>
// #include "common.h"
// #include "time_base.h"
#include "record.pb.h"

namespace rdt
{

    inline void LogFormat(google::LogSeverity severity, const char *format, ...)
    {
        va_list args;
        va_start(args, format);
        std::ostringstream oss;
        char buffer[1024];
        vsnprintf(buffer, sizeof(buffer), format, args);
        oss << buffer;
        switch (severity)
        {
        case google::INFO:
            LOG(INFO) << oss.str();
            break;
        case google::WARNING:
            LOG(WARNING) << oss.str();
            break;
        case google::ERROR:
            LOG(ERROR) << oss.str();
            break;
        case google::FATAL:
            LOG(FATAL) << oss.str();
            break;
        default:
            LOG(INFO) << oss.str();
        }
        va_end(args);
    }

#define LOG_FATAL(format, ...) rdt::LogFormat(google::FATAL, format, ##__VA_ARGS__)
#define LOG_ERROR(format, ...) rdt::LogFormat(google::ERROR, format, ##__VA_ARGS__)
#define LOG_WARNING(format, ...) rdt::LogFormat(google::WARNING, format, ##__VA_ARGS__)
#define LOG_DEBUG(format, ...) rdt::LogFormat(google::INFO, format, ##__VA_ARGS__)
#define LOG_NOTICE(format, ...) rdt::LogFormat(google::INFO, format, ##__VA_ARGS__)
#define LOG_TRACE(format, ...) rdt::LogFormat(google::INFO, format, ##__VA_ARGS__)

#define MAX_PATH_LEN 256
#define MAX_CHAR_IN_LINE 1024
#define FIELD_REX_DELIMITER ","
#define ASSERT assert

    template <typename T>
    std::string print_vect(const std::vector<T> &vect)
    {
        std::ostringstream os;
        os << "[";
        typedef typename std::vector<T>::const_iterator iter;

        for (iter it = vect.begin();
             it != vect.end(); ++it)
        {
            os << *it << " ";
        }

        os << "]";
        return os.str();
    }

    inline char *ltrim(char *str)
    {
        char *p = str;

        if (str == NULL || str[0] == '\0')
        {
            return NULL;
        }

        while (*p != '\0')
        {
            if (*p == ' ' || *p == '\n' || *p == '\r' || *p == '\t')
            {
                p++;
                continue;
            }

            break;
        }

        return p;
    }

    inline char *rtrim(char *str)
    {
        char *p = NULL;

        if (str == NULL || str[0] == '\0')
        {
            return NULL;
        }

        p = str + strlen(str) - 1;

        while (p != str)
        {
            if (*p == ' ' || *p == '\n' || *p == '\r' || *p == '\t')
            {
                p--;
                continue;
            }

            break;
        }

        p[1] = '\0';
        return str;
    }

    struct table_t
    {
        std::string db;
        std::string table;
        std::set<std::string> columns;
        std::set<std::string> monitor_columns;
    };

    class mysql_master_info
    {
    public:
        mysql_master_info()
        {
            username = "";
            password = "";
            host = "";
            port = 0;
            prefix = "";
            binlog_name = "";
            offset = 0;
            progress = 0;
        }
        mysql_master_info(const mysql_master_info &info)
        {
            username = info.username;
            password = info.password;
            host = info.host;
            port = info.port;
            prefix = info.prefix;
            binlog_name = info.binlog_name;
            offset = info.offset;
            progress = info.progress;
        }
        ~mysql_master_info() {}
        std::string print() const
        {
            std::ostringstream os;
            os << "username:" << username << std::endl;
            os << "password:" << password << std::endl;
            os << "host:" << host << std::endl;
            os << "port:" << port << std::endl;
            os << "binlog name: " << binlog_name << std::endl;
            os << "offset: " << offset << std::endl;
            return os.str();
        }
        mysql_master_info &operator=(const mysql_master_info &info)
        {
            if (this == &info)
            {
                return *this;
            }

            username = info.username;
            password = info.password;
            host = info.host;
            port = info.port;
            prefix = info.prefix;
            binlog_name = info.binlog_name;
            offset = info.offset;
            progress = info.progress;
            return *this;
        }

    public:
        std::string username;
        std::string password;
        std::string host;
        short port;
        std::string prefix;
        std::string binlog_name;
        uint64_t offset;
        uint64_t progress;
    };

    // 0:not signed,1:signed,2:no signed attribute
    int get_sign_type(
        uint32_t mysql_type,
        uint32_t binloglib_type);

    struct field_info_t
    {
        // field id
        uint32_t id;
        // field name
        std::string name;
        // mysql type
        uint32_t mysql_type;
        // field type in rdt
        uint32_t binloglib_type;
        // is signed
        bool is_sign;
        // whether the field would be null
        bool can_null;
        // whether the field is primary key
        bool is_pk;
        // whether the field will be processed
        bool is_proc;
        // whether this field change will cause increment
        bool is_monitor_change;
        // field charset
        std::string charset;
        const static field_info_t S_DEFAULT_FIELD_INFO;
        // check whether the following two schema is compatible.
        static bool is_compatible(
            const std::vector<field_info_t> &old_fields,
            const std::vector<field_info_t> &new_fields);
    };

    struct table_info_t
    {
        // table id in mysql
        uint64_t map_id;
        // database name
        std::string db_name;
        // table name
        std::string table_name;
        // field metadata
        std::vector<uint16_t> field_metadata_vect;
        // field types
        std::vector<uint8_t> field_type_vect;
        // field count
        uint32_t field_cnt;
        // the number of field that need be processed
        uint32_t proc_field_cnt;
        // field description
        std::vector<field_info_t> field_info_vect;
    };

    inline void intstore(char *buf, uint64_t num, int len)
    {
        int pos = 0;

        if (buf == NULL || len <= 0)
        {
            return;
        }

        while (len--)
        {
            buf[pos++] = (char)num;
            num >>= 8;
        }

        return;
    }

#ifndef IS_BIG_ENDIAN

    inline short get_sint16_value(const char *buf)
    {
        return *((short *)(buf));
    }

    inline int get_sint24_value(const char *buf)
    {
        uint32_t result = ((uint8_t)buf[2] & 128) ? 0xFF000000 : 0;
        result |= (uint32_t)(uint8_t)buf[0];
        result |= ((uint32_t)(uint8_t)buf[1]) << 8;
        result |= ((uint32_t)(uint8_t)buf[2]) << 16;
        return (int)result;
    }

    inline int get_sint32_value(const char *buf)
    {
        return *((int *)(buf));
    }

    inline uint16_t get_uint16_value(const char *buf)
    {
        return *((uint16_t *)(buf));
    }

    inline uint32_t get_uint24_value(const char *buf)
    {
        uint32_t result = 0;
        result |= (uint32_t)(uint8_t)buf[0];
        result |= ((uint32_t)(uint8_t)buf[1]) << 8;
        result |= ((uint32_t)(uint8_t)buf[2]) << 16;
        return result;
    }

    inline uint32_t get_uint32_value(const char *buf)
    {
        return *((uint32_t *)(buf));
    }

    inline uint64_t get_uint64_value(const char *buf, int len)
    {
        int i = 0;
        uint64_t res = 0;

        if (!buf)
        {
            return 0;
        }

        while (i < len)
        {
            res |= ((uint64_t)(uint8_t)buf[i] << (i * 8));
            ++i;
        }

        return res;
    }
#else
#error "This Program Can Not Compile On Big Endian Machine."
#endif

    inline char get_be_sint8_value(const char *buf)
    {
        char result = (uint8_t)buf[0];
        return result;
    }

    inline short get_be_sint16_value(const char *buf)
    {
        short result = 0;
        result |= (short)(uint8_t)buf[1];
        result |= ((short)(uint8_t)buf[0]) << 8;
        return result;
    }

    inline int get_be_sint24_value(const char *buf)
    {
        uint32_t result = ((uint8_t)buf[0] & 128) ? 0xFF000000 : 0;
        result |= (uint32_t)(uint8_t)buf[2];
        result |= ((uint32_t)(uint8_t)buf[1]) << 8;
        result |= ((uint32_t)(uint8_t)buf[0]) << 16;
        return (int)result;
    }

    inline int get_be_sint32_value(const char *buf)
    {
        int result = 0;
        result |= (int)(uint8_t)buf[3];
        result |= ((int)(uint8_t)buf[2]) << 8;
        result |= ((int)(uint8_t)buf[1]) << 16;
        result |= ((int)(uint8_t)buf[0]) << 24;
        return result;
    }
    inline uint64_t get_packed_length(char **packet)
    {
        uint8_t type = *(uint8_t *)*packet;
        register char *pos = (char *)*packet;

        /* 1 byte length */
        if (type < 251)
        {
            (*packet)++;
            return (uint64_t)*(uint8_t *)pos;
        }

        /* 251 stands for NULL */
        if (type == 251)
        {
            (*packet)++;
            return ((uint64_t)~0);
        }

        /* 2 byte length */
        if (type == 252)
        {
            (*packet) += 3;
            return (uint64_t)get_uint16_value(pos + 1);
        }

        /* 3 byte length */
        if (type == 253)
        {
            (*packet) += 4;
            return (uint64_t)get_uint24_value(pos + 1);
        }

        /* 4 byte length */
        (*packet) += 9; /* Must be 254 when here */
        return (uint64_t)get_uint32_value(pos + 1);
    }

    // all mysql field type
    enum mysql_field_type_t
    {
        MYSQL_TYPE_DECIMAL,
        MYSQL_TYPE_TINY,
        MYSQL_TYPE_SHORT,
        MYSQL_TYPE_LONG,
        MYSQL_TYPE_FLOAT,
        MYSQL_TYPE_DOUBLE,
        MYSQL_TYPE_NULL,
        MYSQL_TYPE_TIMESTAMP,
        MYSQL_TYPE_LONGLONG,
        MYSQL_TYPE_INT24,
        MYSQL_TYPE_DATE,
        MYSQL_TYPE_TIME,
        MYSQL_TYPE_DATETIME,
        MYSQL_TYPE_YEAR,
        MYSQL_TYPE_NEWDATE,
        MYSQL_TYPE_VARCHAR,
        MYSQL_TYPE_BIT,
        MYSQL_TYPE_NEWDECIMAL = 246,
        MYSQL_TYPE_ENUM = 247,
        MYSQL_TYPE_SET = 248,
        MYSQL_TYPE_TINY_BLOB = 249,
        MYSQL_TYPE_MEDIUM_BLOB = 250,
        MYSQL_TYPE_LONG_BLOB = 251,
        MYSQL_TYPE_BLOB = 252,
        MYSQL_TYPE_VAR_STRING = 253,
        MYSQL_TYPE_STRING = 254,
        MYSQL_TYPE_GEOMETRY = 255,
        MYSQL_TYPE_UNKNOW
    };

    enum binloglib_type_t
    {
        BINLOGLIB_TYPE_UINT8 = 1,
        BINLOGLIB_TYPE_INT8 = 2,
        BINLOGLIB_TYPE_UINT16 = 3,
        BINLOGLIB_TYPE_INT16 = 4,
        BINLOGLIB_TYPE_UINT32 = 5,
        BINLOGLIB_TYPE_INT32 = 6,
        BINLOGLIB_TYPE_UINT64 = 7,
        BINLOGLIB_TYPE_INT64 = 8,
        BINLOGLIB_TYPE_STRING = 9,
        BINLOGLIB_TYPE_BINARY = 10,
        BINLOGLIB_TYPE_TIMESTAMP,
        BINLOGLIB_TYPE_DECIMAL,
        BINLOGLIB_TYPE_FLOAT,
        BINLOGLIB_TYPE_DOUBLE,
        BINLOGLIB_TYPE_UNKNOW
    };

    const uint32_t DECIMAL_BUFF_LENGTH = 9;
    const uint32_t DIG_PER_DEC1 = 9;

#define ROUND_UP(X) (((X) + DIG_PER_DEC1 - 1) / DIG_PER_DEC1)

    // decimal
    typedef struct st_decimal_t
    {
        int intg;
        int frac;
        int len;
        bool sign;
        int buf[DECIMAL_BUFF_LENGTH];
    } decimal_t;

    const int dig2bytes[DIG_PER_DEC1 + 1] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
    const int powers10[DIG_PER_DEC1 + 1] = {
        1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

    const uint64_t DIG_MAX = 1000000000 - 1;

    int bin2decimal(const uint8_t *from, decimal_t *to, int precision, int scale);

    inline int decimal_bin_size(int precision, int scale)
    {
        int intg = precision - scale;
        int intg0 = intg / DIG_PER_DEC1;
        int frac0 = scale / DIG_PER_DEC1;
        int intg0x = intg - intg0 * DIG_PER_DEC1;
        int frac0x = scale - frac0 * DIG_PER_DEC1;
        return intg0 * sizeof(int) + dig2bytes[intg0x] +
               frac0 * sizeof(int) + dig2bytes[frac0x];
    }

    uint32_t get_quoted_length_buf(const uint8_t *ptr, uint8_t **ptr_res, uint32_t *res_len);

    int str2type(const char *type_str, uint32_t *mysql_type, uint32_t *binloglib_type, bool *is_signed);

    int mysql_type2binloglib_type(uint32_t *binloglib_type, uint32_t mysql_type, bool is_signed);

    int make_field_metadata(
        std::vector<uint16_t> &metadata_vect,
        const uint8_t *meta_buffer,
        uint32_t meta_len,
        const std::vector<uint8_t> &field_types_vect);
    bool is_sql_starts_with(const char *sql, const char *first_pattern, const char *second_pattern);

}
#endif //_MYSQL_BINLOG_COMMON_H_
