#ifndef  _BINLOG_EVENT_H_
#define  _BINLOG_EVENT_H_

#include <openssl/evp.h>
#include "rdt_common.h"

namespace rdt
{

//binlog start position
#define BINLOG_START_POS 4
#define R_POS_OFFSET 0
//Max length of full path-name
#define FN_REFLEN 512

enum log_event_type {
    UNKNOWN_EVENT = 0,
    START_EVENT_V3 = 1,
    QUERY_EVENT = 2,
    STOP_EVENT = 3,
    ROTATE_EVENT = 4,
    INTVAR_EVENT = 5,
    LOAD_EVENT = 6,
    SLAVE_EVENT = 7,
    CREATE_FILE_EVENT = 8,
    APPEND_BLOCK_EVENT = 9,
    EXEC_LOAD_EVENT = 10,
    DELETE_FILE_EVENT = 11,
    NEW_LOAD_EVENT = 12,
    RAND_EVENT = 13,
    USER_VAR_EVENT = 14,
    FORMAT_DESCRIPTION_EVENT = 15,
    XID_EVENT = 16,
    BEGIN_LOAD_QUERY_EVENT = 17,
    EXECUTE_LOAD_QUERY_EVENT = 18,
    TABLE_MAP_EVENT = 19,
    //These events were used for 5.1.0 to 5.1.15 and are therefore obsolete.
    PRE_GA_WRITE_ROWS_EVENT = 20,
    PRE_GA_UPDATE_ROWS_EVENT = 21,
    PRE_GA_DELETE_ROWS_EVENT = 22,
    //These event numbers are used from 5.1.16 and 5.6.0
    WRITE_ROWS_EVENT = 23,
    UPDATE_ROWS_EVENT = 24,
    DELETE_ROWS_EVENT = 25,
    //Something out of the ordinary happened on the master
    INCIDENT_EVENT = 26,
    /*
     * heartbeat event will be added in MySQL6.0,
     * if master upgrades to MySQL6.0, you will need to deal it
     * */
    HEARTBEAT_LOG_EVENT = 27,
    /*
     * In some situations, it is necessary to send over ignorable
     * data to the slave: data that a slave can handle in case there
     * is code for handling it, but which can be ignored if it is not
     * recognized.
     * */
    IGNORABLE_LOG_EVENT = 28,
    ROWS_QUERY_LOG_EVENT = 29,
    //Version 2 of the Row events mysql 5.6.0 and forword
    WRITE_ROWS_EVENT_V2 = 30,
    UPDATE_ROWS_EVENT_V2 = 31,
    DELETE_ROWS_EVENT_V2 = 32,
    
    GTID_LOG_EVENT = 33,
    ANONYMOUS_GTID_LOG_EVENT = 34,
    PREVIOUS_GTIDS_LOG_EVENT = 35,
    
    TRANSACTION_CONTEXT_EVENT = 36,
    
    VIEW_CHANGE_EVENT = 37,
    
    //Prepared XA transaction terminal event similar to Xid
    XA_PREPARE_LOG_EVENT = 38,
    
    /*
     * Add new events here - right above this comment!
     * Existing events (except ENUM_END_EVENT) should never change their numbers
     * */
    ENUM_END_EVENT /* end marker */
};

static const char* event_type2str(log_event_type type)
{
    switch(type) {
    case START_EVENT_V3:
        return "Start_v3";
        
    case STOP_EVENT:
        return "Stop";
        
    case QUERY_EVENT:
        return "Query";
        
    case ROTATE_EVENT:
        return "Rotate";
        
    case INTVAR_EVENT:
        return "Intvar";
        
    case LOAD_EVENT:
        return "Load";
        
    case NEW_LOAD_EVENT:
        return "New_load";
        
    case SLAVE_EVENT:
        return "Slave";
        
    case CREATE_FILE_EVENT:
        return "Create_file";
        
    case APPEND_BLOCK_EVENT:
        return "Append_block";
        
    case DELETE_FILE_EVENT:
        return "Delete_file";
        
    case EXEC_LOAD_EVENT:
        return "Exec_load";
        
    case RAND_EVENT:
        return "RAND";
        
    case XID_EVENT:
        return "Xid";
        
    case USER_VAR_EVENT:
        return "User var";
        
    case FORMAT_DESCRIPTION_EVENT:
        return "Format_desc";
        
    case TABLE_MAP_EVENT:
        return "Table_map";
        
    case PRE_GA_WRITE_ROWS_EVENT:
        return "Write_rows_event_old";
        
    case PRE_GA_UPDATE_ROWS_EVENT:
        return "Update_rows_event_old";
        
    case PRE_GA_DELETE_ROWS_EVENT:
        return "Delete_rows_event_old";
        
    case WRITE_ROWS_EVENT:
        return "Write_rows";
        
    case UPDATE_ROWS_EVENT:
        return "Update_rows";
        
    case DELETE_ROWS_EVENT:
        return "Delete_rows";
        
    case WRITE_ROWS_EVENT_V2:
        return "Write_rows_v2";
        
    case UPDATE_ROWS_EVENT_V2:
        return "Update_rows_v2";
        
    case DELETE_ROWS_EVENT_V2:
        return "Delete_rows_v2";
        
    case BEGIN_LOAD_QUERY_EVENT:
        return "Begin_load_query";
        
    case EXECUTE_LOAD_QUERY_EVENT:
        return "Execute_load_query";
        
    case INCIDENT_EVENT:
        return "Incident";
        
    case GTID_LOG_EVENT:
        return "GTID_log_event";
        
    default:
        return "Unknown";
    }
}

//These are codes, not offsets; not more than 256 values (1 byte).
enum query_event_status_vars {
    Q_FLAGS2_CODE = 0,
    Q_SQL_MODE_CODE,
    /*
     *Q_CATALOG_CODE is catalog with end zero stored; it is used only by MySQL
     *5.0.x where 0<=x<=3. We have to keep it to be able to replicate these
     *old masters.
     **/
    Q_CATALOG_CODE,
    Q_AUTO_INCREMENT,
    Q_CHARSET_CODE,
    Q_TIME_ZONE_CODE,
    /*
     *Q_CATALOG_NZ_CODE is catalog withOUT end zero stored; it is used by MySQL
     *5.0.x where x>=4. Saves one byte in every Query_event in binlog,
     *compared to Q_CATALOG_CODE. The reason we didn't simply re-use
     *Q_CATALOG_CODE is that then a 5.0.3 slave of this 5.0.x (x>=4)
     *master would crash (segfault etc) because it would expect a 0 when there
     *is none.
     **/
    Q_CATALOG_NZ_CODE,
    Q_LC_TIME_NAMES_CODE,
    Q_CHARSET_DATABASE_CODE,
    Q_TABLE_MAP_FOR_UPDATE_CODE,
    Q_MASTER_DATA_WRITTEN_CODE,
    Q_INVOKER,
    /*
     *Q_UPDATED_DB_NAMES status variable collects information of accessed
     *databases i.e. the total number and the names to be propagated to the
     *slave in order to facilitate the parallel applying of the Query events.
     **/
    Q_UPDATED_DB_NAMES,
    Q_MICROSECONDS,
    //A old (unused now) code for Query_log_event status similar to G_COMMIT_TS.
    Q_COMMIT_TS,
    //A code for Query_log_event status, similar to G_COMMIT_TS2.
    Q_COMMIT_TS2,
    /*
     * The master connection @@session.explicit_defaults_for_timestamp which
     * is recorded for queries, CREATE and ALTER table that is defined with
     * a TIMESTAMP column, that are dependent on that feature.
     * For pre-WL6292 master's the associated with this code value is zero.
     * */
    Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP
};

/**
 *The length of the array server_version, which is used to store the version
 *of MySQL server.
 *We could have used SERVER_VERSION_LENGTH, but this introduces an
 *obscure dependency - if somebody decided to change SERVER_VERSION_LENGTH
 *this would break the replication protocol
 *both of these are used to initialize the array server_version
 *SERVER_VERSION_LENGTH is used for global array server_version
 *and ST_SERVER_VER_LEN for the Start_event_v3 member server_version
**/

#define ST_SERVER_VER_LEN 50
const static int LOG_EVENT_TYPES = (ENUM_END_EVENT - 1);

//Event type offset
#define EVENT_TYPE_OFFSET 4

/**
 * The lengths for the fixed data part of each event.
 * This is an enum that provides post-header lengths for all events.
 * */
enum enum_post_header_length {
    //where 3.23, 4.x and 5.0 agree
    QUERY_HEADER_MINIMAL_LEN = (4 + 4 + 1 + 2),
    //where 5.0 differs: 2 for length of N-bytes vars.
    QUERY_HEADER_LEN = (QUERY_HEADER_MINIMAL_LEN + 2),
    STOP_HEADER_LEN = 0,
    LOAD_HEADER_LEN = (4 + 4 + 4 + 1 + 1 + 4),
    START_V3_HEADER_LEN = (2 + ST_SERVER_VER_LEN + 4),
    //this is FROZEN (the Rotate post-header is frozen)
    ROTATE_HEADER_LEN = 8,
    INTVAR_HEADER_LEN = 0,
    CREATE_FILE_HEADER_LEN = 4,
    APPEND_BLOCK_HEADER_LEN = 4,
    EXEC_LOAD_HEADER_LEN = 4,
    DELETE_FILE_HEADER_LEN = 4,
    NEW_LOAD_HEADER_LEN = LOAD_HEADER_LEN,
    RAND_HEADER_LEN = 0,
    USER_VAR_HEADER_LEN = 0,
    FORMAT_DESCRIPTION_HEADER_LEN = (START_V3_HEADER_LEN + 1 + LOG_EVENT_TYPES),
    XID_HEADER_LEN = 0,
    BEGIN_LOAD_QUERY_HEADER_LEN = APPEND_BLOCK_HEADER_LEN,
    ROWS_HEADER_LEN_V1 = 8,
    TABLE_MAP_HEADER_LEN = 8,
    EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN = (4 + 4 + 4 + 1),
    EXECUTE_LOAD_QUERY_HEADER_LEN = (QUERY_HEADER_LEN + EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN),
    INCIDENT_HEADER_LEN = 2,
    HEARTBEAT_HEADER_LEN = 0,
    IGNORABLE_HEADER_LEN = 0,
    ROWS_HEADER_LEN_V2 = 10,
    TRANSACTION_CONTEXT_HEADER_LEN = 18,
    VIEW_CHANGE_HEADER_LEN = 52,
    XA_PREPARE_HEADER_LEN = 0
};//end enum_post_header_length

#define BINLOG_EVENT_HEADER_LEN 19

//MySQL(V4) event header
class log_event_header
{
public:

    log_event_header() {}
    ~log_event_header() {}
    std::string tostring() const
    {
        std::ostringstream os;
        os << "[timestamp:" << timestamp << "]";
        os << "[type_code" << (uint32_t)type_code << " ";
        os << event_type2str((log_event_type)type_code) << "]";
        os << "[server_id:" << server_id << "]";
        os << "[event_length:" << event_length << "]";
        os << "[next_position:" << next_pos << "]";
        os << "[flags:" << (uint32_t)flags << "]";
        return os.str();
    }
    
    int parser(const char* buffer, const uint32_t length)
    {
        if(buffer == NULL) {
            LOG_WARNING("parser log event header faild, buffer is null.");
            return -1;
        }
        
        if(length < BINLOG_EVENT_HEADER_LEN) {
            LOG_WARNING("parser log event header faild. length: %d", length);
            return -1;
        }
        
#ifndef IS_BIG_ENDIAN
        memcpy(this, buffer, BINLOG_EVENT_HEADER_LEN);
#else
        const char* curr_pos = buffer;
        timestamp = get_uint32_value(curr_pos);
        curr_pos += 4;
        type_code = (uint8_t) * curr_pos;
        curr_pos += 1;
        server_id = get_uint32_value(curr_pos);
        curr_pos += 4;
        event_length = get_uint32_value(curr_pos);
        curr_pos += 4;
        next_position = get_uint32_value(curr_pos);
        curr_pos += 4;
        flags = get_uint16_value(curr_pos);
#endif
        
        if(event_length != length) {
            LOG_WARNING("parser log event header faild. event length: %d, buffer length: %d", event_length, length);
            return -1;
        }
        
        LOG_DEBUG("timestamp: %d, type code: %s, server id: %d, event len: %d, next position: %d, flags: %d", timestamp, event_type2str((log_event_type)type_code), server_id, event_length, next_pos, flags);
        return 0;
    }
public:
    /**
     * This is the time at which the statement began executing.
     * It is represented as the number of seconds since 1970 (UTC),
     * like the TIMESTAMP SQL data type.
     * */
    uint32_t timestamp;
    
    //The type of event. These numbers are defined in the log_event_type.
    uint8_t type_code;
    
    //The ID of the mysqld server that originally created the event.
    uint32_t server_id;
    
    //The total size of this event. This includes both the header and data parts.
    uint32_t event_length;
    
    //The position of the next event in the master's binary log.
    uint32_t next_pos;
    
    /**
     * until MySQL5.1, flags are:
     * LOG_EVENT_BINLOG_IN_USE_F = 0x1 (New in 5.0.3)
     * LOG_EVENT_THREAD_SPECIFIC_F = 0x4 (New in 4.1.0)
     * LOG_EVENT_SUPPRESS_USE_F = 0x8 (New in 4.1.7)
     * LOG_EVENT_UPDATE_TABLE_MAP_VERSION_F = 0x10 (New in 5.1.4)
     * */
    uint16_t flags;
} __attribute__((packed));

//This is the abstract base class for binary log events.
class binary_log_event
{
public:
    explicit binary_log_event(log_event_type type): m_type(type) {}
    virtual ~binary_log_event() {}
    virtual log_event_type get_type()
    {
        return m_type;
    }
    
    virtual int unpack(const char* buffer, const uint32_t length)
    {
        return 0;
    }
    
public:
    log_event_header m_header;
protected:
    log_event_type m_type;
};

class format_description_log_event : public binary_log_event
{
public:
    format_description_log_event(): binary_log_event(FORMAT_DESCRIPTION_EVENT) {}
    ~format_description_log_event() {}
    
    int unpack(const char* buffer, const uint32_t length)
    {
        buffer += BINLOG_EVENT_HEADER_LEN;
        m_binlog_version = *(short*)buffer;
        memcpy(m_mysql_server_version, buffer + 2, 50);
        m_create_timestamp = *(uint32_t*)(buffer + 52);
        m_event_header_len = *(uint8_t*)(buffer + 56);
        LOG_DEBUG("binlog version: %d, mysql server version: %s, create timestamp: %d, event header len: %d", m_binlog_version, m_mysql_server_version, m_create_timestamp, m_event_header_len);
        return 0;
    }
public:
    short m_binlog_version;
    char m_mysql_server_version[50];
    uint32_t m_create_timestamp;
    uint32_t m_event_header_len;
    std::string m_event_type_header_length;
};


class parser_info;

class table_map_log_event : public binary_log_event
{
public:
    table_map_log_event(): binary_log_event(TABLE_MAP_EVENT)
    {
        m_table_id = 0;
        m_colcnt = 0;
        m_flags = 0;
        m_dblen = 0;
        m_tbllen = 0;
        m_parser_info = NULL;
    }
    
    int init(parser_info* p)
    {
        if(p != NULL) {
            m_parser_info = p;
        }
        
        return 0;
    }
    static int unpack(const char* buf, uint32_t event_len, parser_info& parserinfo,
                      format_description_log_event& desc_event);
    std::string tostring() const;
public:
    uint64_t m_table_id;
    uint64_t m_colcnt;
    uint16_t m_flags;
    size_t m_dblen;
    size_t m_tbllen;
    std::string m_dbname;
    std::string m_tblname;
    std::vector<uint8_t> m_coltype_vect;
    std::vector<uint16_t> m_metadata_vect;
    parser_info* m_parser_info;
};

class field_data_t
{
public:
    field_data_t() {}
    ~field_data_t() {}
    void set_value(const void* ptr, uint32_t size)
    {
        const uint8_t* data = (const uint8_t*)ptr;
        value.assign(data, data + size);
    }
    
    bool value_equal(const field_data_t& data) const
    {
        if(is_null != data.is_null) {
            return false;
        }
        
        return value == data.value;
    }
    int get_value_string(std::string& value_string, bool b_is_encode) const ;
public:
    //field name
    std::string name;
    //field type
    uint32_t binloglib_type;
    //field type
    uint32_t mysql_type;
    //is primary key
    uint32_t is_pk;
    //is NULL
    uint32_t is_null;
    //field charset
    std::string charset;
    //whether monitor field change
    uint32_t is_monitor_change;
private:
    //Field value, this field is NULL when value.size() is zero.
    std::vector<uint8_t> value;
};

struct row_data_t {
    std::vector<field_data_t> old_data_vect;
    std::vector<field_data_t> new_data_vect;
};

class rows_log_event : public binary_log_event
{
public:
    rows_log_event(): binary_log_event(UNKNOWN_EVENT), m_row_type(OP_UNKNOWN) {}
    ~rows_log_event() {}
    enum row_type_t {
        OP_UPDATE = 0,
        OP_INSERT = 1,
        OP_DELETE = 2,
        OP_UNKNOWN
    };
    int unpack(const char* buffer, const uint32_t length, format_description_log_event& desc_event, parser_info& info);
    
private:
    int parse_row(
        vector<field_data_t>& field_data_vect,
        uint64_t* length,
        const char* value,
        uint64_t column_cnt,
        const table_info_t& table_info);
    int parse_field(
        field_data_t& field_data,
        uint64_t* length,
        const char* ptr,
        uint32_t ftype,
        bool is_null,
        uint32_t meta,
        const field_info_t& field_info);
        
public:
    uint32_t m_row_type;
    std::string m_dbname;
    std::string m_tblname;
    std::string m_charset;
    std::vector<row_data_t> m_row_data_vect;
};

struct query_post_header {
    //Thread id which executed the event in master
    uint32_t slave_proxy_id;
    uint32_t exec_time;
    uint8_t db_name_len;
    uint16_t error_code;
    uint16_t status_vars_len;
} __attribute__((packed));

class query_log_event : public binary_log_event
{
public:
    query_log_event() : binary_log_event(QUERY_EVENT) {}
    ~query_log_event() {}
    
    int unpack(const char* buffer, const uint32_t length, format_description_log_event& desc_event);
private:
    int parse_query_event_variable(uint16_t* charset_id, const uint8_t* var_begin, const uint8_t* var_end);
public:
    std::string m_default_db;
    std::string m_real_sql;
};

class xid_log_event : public binary_log_event
{
public:
    xid_log_event() : binary_log_event(XID_EVENT) {}
    int unpack(const char* buf, const int len)
    {
        buf += BINLOG_EVENT_HEADER_LEN;
        m_xid = *(uint64_t*)buf;
        LOG_DEBUG("xid: %d", m_xid);
        return 0;
    }
public:
    uint64_t m_xid;
};

class rotate_log_event : public binary_log_event
{
public:
    rotate_log_event() : binary_log_event(ROTATE_EVENT), m_next_binlog_offset(4UL) {}
    int unpack(const char* buf, const int len)
    {
        uint32_t event_len = m_header.event_length;
        uint32_t post_header_len = ROTATE_HEADER_LEN;
        uint32_t file_name_len = 0;
        
        if(event_len < BINLOG_EVENT_HEADER_LEN + post_header_len) {
            LOG_WARNING("get rotate event length error, maybe network problem. ");
            return -1;
        }
        
        file_name_len = event_len - BINLOG_EVENT_HEADER_LEN - post_header_len - 4;
        
        if(file_name_len > FN_REFLEN - 1) {
            file_name_len = FN_REFLEN - 1;
        }
        
        m_next_binlog_name.assign(buf + BINLOG_EVENT_HEADER_LEN + post_header_len, file_name_len);
        m_next_binlog_offset = * (uint64_t*)(buf + BINLOG_EVENT_HEADER_LEN);
        LOG_DEBUG("rotate event next_binlog_name: %s, next_binlog_offset: %lu]",
                  m_next_binlog_name.c_str(), m_next_binlog_offset);
        return 0;
    }
public:
    std::string m_next_binlog_name;
    uint64_t m_next_binlog_offset;
};

} // end namespace
#endif  //_BINLOG_EVENT_H_

