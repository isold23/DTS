#include "binlog_parser.h"

namespace dts
{
    bool parser_info::find_table(
        uint64_t mapid,
        const std::string &dbname,
        const std::string &tablename,
        table_info_t *&p_table_info)
    {
        if (find_table_by_mapid(mapid, p_table_info))
        {
            return true;
        }

        if (find_table_by_name(dbname, tablename, p_table_info))
        {
            p_table_info->map_id = mapid;
            return true;
        }

        return false;
    }
    bool parser_info::find_table_by_mapid(
        uint64_t mapid,
        table_info_t *&p_table_info)
    {
        for (uint32_t i = 0; i < m_cur_info_vect.size(); ++i)
        {
            if (mapid == m_cur_info_vect[i].map_id)
            {
                p_table_info = &m_cur_info_vect[i];
                return true;
            }
        }

        return false;
    }

    bool parser_info::find_table_by_name(
        const std::string &dbname,
        const std::string &tablename,
        table_info_t *&p_table_info)
    {
        std::string table_name = dbname;
        table_name += '.';
        table_name += tablename;
        std::map<std::string, size_t>::iterator it;
        it = m_table_name_index_map.find(table_name);

        if (it != m_table_name_index_map.end())
        {
            m_cur_info_vect.push_back(m_table_info_vect[it->second]);
            p_table_info = &m_cur_info_vect.back();
            return true;
        }

        return false;
    }

    int binlog_parser::parse_log_event(
        const char *event_pack,
        uint64_t event_len,
        binary_log_event *&event,
        log_event_header *header)
    {
        log_event_header event_header;
        const char *event_body = NULL;
        binary_log_event *result = NULL;
        event = NULL;
        int ignore = 0;

        if (!event_pack)
        {
            LOG_WARNING("'%s' event_pack is null. ",
                        __PRETTY_FUNCTION__);
            return -1;
        }

        if (event_header.parser(event_pack, event_len) != 0)
        {
            LOG_WARNING("parse log event header failed");
            return -1;
        }

        if (NULL != header)
        {
            *header = event_header;
        }

        event_body = event_pack + BINLOG_EVENT_HEADER_LEN;

        if (event_header.type_code == FORMAT_DESCRIPTION_EVENT)
        {
            get_checksum_alg(event_body, event_pack, event_len);
        }

        if (checksum_alg != BINLOG_CHECKSUM_ALG_OFF &&
            (event_header.type_code == FORMAT_DESCRIPTION_EVENT ||
             checksum_alg != BINLOG_CHECKSUM_ALG_OFF))
        {
            event_header.event_length = event_header.event_length - 4;
        }

        switch (event_header.type_code)
        {
        case FORMAT_DESCRIPTION_EVENT:
        {
            m_desc_event.m_header = *header;

            if (m_desc_event.unpack(event_pack, event_len) != 0)
            {
                LOG_WARNING("parser format description log event failed.");
                return -1;
            }

            break;
        }

        case TABLE_MAP_EVENT:
        {
            int ret = table_map_log_event::unpack(event_pack, event_header.event_length,
                                                  m_parser_info, m_desc_event);

            if (0 == ret)
            {
                m_has_table_map = true;
            }
            else
            {
                LOG_WARNING("_parse_table_map_event failed.");
                return -1;
            }

            break;
        }

        case UPDATE_ROWS_EVENT:
        case WRITE_ROWS_EVENT:
        case DELETE_ROWS_EVENT:
        case UPDATE_ROWS_EVENT_V2:
        case WRITE_ROWS_EVENT_V2:
        case DELETE_ROWS_EVENT_V2:
        {
            m_rows_event.m_header = *header;

            if (!m_has_table_map)
            {
                LOG_WARNING("parse row_event failed. [err='%s']",
                            "no table_map event found");
                return -1;
            }

            if (-1 == m_rows_event.unpack(event_pack, event_len, m_desc_event, m_parser_info))
            {
                LOG_WARNING("parse rows event failed.");
                return -1;
            }

            result = &m_rows_event;
            break;
        }

        case QUERY_EVENT:
        {
            m_query_event.m_header = *header;

            if (0 != m_query_event.unpack(event_pack, event_len, m_desc_event))
            {
                LOG_WARNING("parse query log event failed.");
                return -1;
            }

            result = &m_query_event;
            break;
        }

        case XID_EVENT:
        {
            m_xid_event.m_header = *header;

            if (0 != m_xid_event.unpack(event_pack, event_len))
            {
                LOG_WARNING("parse xid log event failed.");
                return -1;
            }

            result = &m_xid_event;
            break;
        }

        case ROTATE_EVENT:
        {
            m_rotate_event.m_header = *header;

            if (0 != m_rotate_event.unpack(event_pack, event_len))
            {
                LOG_WARNING("parse xid log event failed.");
                return -1;
            }

            result = &m_rotate_event;
            break;
        }

        case INCIDENT_EVENT:
        {
            LOG_WARNING("master incident error, ");
            return -1;
        }

        default:
        {
            ignore = 1;
            LOG_WARNING("unknow event. %d", event_header.type_code);
            break;
        }
        }

        event = result;
        return 0;
    }
}
