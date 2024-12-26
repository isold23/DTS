#ifndef _BINLOG_PARSER_H_
#define _BINLOG_PARSER_H_

#include "rdt_conf.h"
#include "rdt_common.h"
#include "binlog_event.h"

namespace rdt
{

#define CHECKSUM_CRC32_SIGNATURE_LEN 4
// defined statically while there is just one alg implemented
#define BINLOG_CHECKSUM_LEN CHECKSUM_CRC32_SIGNATURE_LEN
// 1 byte checksum alg descriptor
#define BINLOG_CHECKSUM_ALG_DESC_LEN 1

#define BINLOG_CHECKSUM_ALG_OFF 0
#define BINLOG_CHECKSUM_ALG_CRC32 1
#define BINLOG_CHECKSUM_ALG_ENUM_END 2
#define BINLOG_CHECKSUM_ALG_UNDEF 255

    const uint8_t checksum_version_split[3] = {5, 6, 1};
    const uint64_t checksum_version_product =
        (checksum_version_split[0] * 256 +
         checksum_version_split[1]) *
            256 +
        checksum_version_split[2];

    // store db schema infomation
    class parser_info
    {
    public:
        parser_info() {}
        bool find_table_by_mapid(uint64_t mapid, table_info_t *&p_table_info);
        bool find_table(
            uint64_t mapid,
            const std::string &dbname,
            const std::string &tablename,
            table_info_t *&p_table_info);

    private:
        bool find_table_by_name(
            const std::string &dbname,
            const std::string &tablename,
            table_info_t *&p_table_info);

    public:
        std::vector<table_info_t> m_table_info_vect;
        std::map<std::string, size_t> m_table_name_index_map;
        std::vector<table_info_t> m_cur_info_vect;
        std::vector<std::string> m_id_charsets;
        std::string m_current_charset;
    };

    // This is the class for parse mysql binary logs.
    class binlog_parser
    {
    public:
        binlog_parser()
        {
            m_has_table_map = false;
            checksum_alg = BINLOG_CHECKSUM_ALG_UNDEF;
            m_table_map_event.init(&m_parser_info);
        }
        binlog_parser(const binlog_parser &);
        ~binlog_parser() {}
        binlog_parser &operator=(const binlog_parser &);
        int parse_log_event(
            const char *event_pack,
            uint64_t event_len,
            binary_log_event *&event,
            log_event_header *header);

    private:
        // parse ROWS_LOG_EVENT
        int _parse_row_event(
            const char *event,
            const log_event_header &event_header,
            rows_log_event &);

        // parse one row event in ROWS_LOG_EVENT
        int _parse_row_event_one_row(
            std::vector<field_data_t> &field_data_vect,
            uint64_t *length,
            const char *value,
            uint64_t column_cnt,
            const table_info_t &table_info);

        // parse one field data in ROWS_LOG_EVENT
        // !!! WARNING !!!
        // field_data will not correct value when this field is not subscribed(means false == is_proc).
        int _parse_row_event_field(
            field_data_t &field_data,
            uint64_t *length,
            const char *ptr,
            uint32_t ftype,
            bool is_null,
            uint32_t metadata,
            const field_info_t &field_info);
        /**
         * Splits server 'version' string into three numeric pieces stored
         * into 'split_versions':
         * X.Y.Zabc (X,Y,Z numbers, a not a digit) -> {X,Y,Z}
         * X.Yabc -> {X,Y,0}
         * */
        inline void do_server_version_split(char *version, uint8_t split_versions[3])
        {
            char *p = version, *r;
            ulong number;

            for (uint i = 0; i <= 2; i++)
            {
                number = strtoul(p, &r, 10);

                /*
                 * It is an invalid version if any version number greater than 255 or
                 * first number is not followed by '.'.
                 * */
                if (number < 256 && (*r == '.' || i != 0))
                    split_versions[i] = (uint8_t)number;
                else
                {
                    split_versions[0] = 0;
                    split_versions[1] = 0;
                    split_versions[2] = 0;
                    break;
                }

                p = r;

                if (*r == '.')
                    p++; // skip the dot
            }
        }
        inline uint64_t version_product(const uint8_t *version_split)
        {
            return ((version_split[0] * 256 + version_split[1]) * 256 + version_split[2]);
        }

        inline int get_checksum_alg(const char *event_body, const char *event_pack, const uint32_t event_len)
        {
            char mysql_server_version[50];
            memset(mysql_server_version, 0, 50);
            memcpy(mysql_server_version, event_body + 2, 49);
            uint8_t version_split[3];
            do_server_version_split(mysql_server_version, version_split);
            checksum_alg = (version_product(version_split) < checksum_version_product) ? (uint8_t)BINLOG_CHECKSUM_ALG_UNDEF : *(uint8_t *)(event_pack + event_len - BINLOG_CHECKSUM_LEN - BINLOG_CHECKSUM_ALG_DESC_LEN);
            return 0;
        }

    public:
        // store the parser info
        parser_info m_parser_info;
        //
        bool m_has_table_map;
        // format description log event
        format_description_log_event m_desc_event;
        // tables map log event
        table_map_log_event m_table_map_event;
        // rows log event
        rows_log_event m_rows_event;
        // query log event
        query_log_event m_query_event;
        // rotate log event
        rotate_log_event m_rotate_event;
        // xid log event
        xid_log_event m_xid_event;

    private:
        uint8_t checksum_alg;
    };

} // end of namespace rdt
#endif //_BINLOG_PARSER_H_
