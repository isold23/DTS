#include "binlog_event.h"
#include "binlog_parser.h"

namespace rdt
{
int field_data_t::get_value_string(std::string& ret_res, bool b_is_encode) const
{
    char buffer[30] = {0};
    int write_len = 0;
    
    if(is_null) {
        ret_res.assign("NULL");
        return -1;
    }
    
    switch(binloglib_type) {
    case BINLOGLIB_TYPE_UINT8:
        write_len = snprintf(buffer, 30, "%u", * (uint8_t*)&value[0]);
        ret_res.assign(buffer, write_len);
        break;
        
    case BINLOGLIB_TYPE_INT8:
        write_len = snprintf(buffer, 30, "%d", * (char*)&value[0]);
        ret_res.assign(buffer, write_len);
        break;
        
    case BINLOGLIB_TYPE_UINT16:
        write_len = snprintf(buffer, 30, "%u", * (uint16_t*)&value[0]);
        ret_res.assign(buffer, write_len);
        break;
        
    case BINLOGLIB_TYPE_INT16:
        write_len = snprintf(buffer, 30, "%d", * (short*)&value[0]);
        ret_res.assign(buffer, write_len);
        break;
        
    case BINLOGLIB_TYPE_UINT32:
        write_len = snprintf(buffer, 30, "%u", * (uint32_t*)&value[0]);
        ret_res.assign(buffer, write_len);
        break;
        
    case BINLOGLIB_TYPE_INT32:
        write_len = snprintf(buffer, 30, "%d", * (int*)&value[0]);
        ret_res.assign(buffer, write_len);
        break;
        
    case BINLOGLIB_TYPE_UINT64:
        write_len = snprintf(buffer, 30, "%lu", * (uint64_t*)&value[0]);
        ret_res.assign(buffer, write_len);
        break;
        
    case BINLOGLIB_TYPE_INT64:
        write_len = snprintf(buffer, 30, "%ld", * (int64_t*)&value[0]);
        ret_res.assign(buffer, write_len);
        break;
        
    case BINLOGLIB_TYPE_STRING:
    case BINLOGLIB_TYPE_BINARY:
        if(b_is_encode) {
            uint32_t need_len = (value.size() + 2) / 3 * 4;
            // need an extra location to store '\0' at the end when EVP_EncodeBlock.
            vector<uint8_t> data_vect(need_len + 1);
            int ret = EVP_EncodeBlock(&data_vect[0], &value[0], value.size());
            
            if(ret == (int)need_len) {
                ret_res.assign((const char*)&data_vect[0], (uint)data_vect.size());
            } else {
                return -1;
            }
        } else {
            ret_res.assign((const char*)&value[0], value.size());
        }
        
        break;
        
    case BINLOGLIB_TYPE_TIMESTAMP:
        write_len = snprintf(buffer, 30, "%u", * (uint32_t*)&value[0]);
        ret_res.assign(buffer, write_len);
        break;
        
    case BINLOGLIB_TYPE_DECIMAL:
        ret_res.assign((char*)&value[0], (uint32_t)value.size());
        break;
        
    case BINLOGLIB_TYPE_FLOAT:
        write_len = snprintf(buffer, 30, "%-.20g", * (float*)&value[0]);
        ret_res.assign(buffer, write_len);
        break;
        
    case BINLOGLIB_TYPE_DOUBLE:
        write_len = snprintf(buffer, 30, "%-.20g", * (double*)&value[0]);
        ret_res.assign(buffer, write_len);
        break;
        
    default:
        LOG_WARNING("UNKNOW BINLOGLIB TYPE[%u]", binloglib_type);
        return -1;
    }
    
    return 0;
}

std::string table_map_log_event::tostring() const
{
    std::ostringstream os;
    os << "_table_id:" << m_table_id;
    os << " _flags:" << m_flags;
    os << " _dblen:" << m_dblen;
    os << " _dbname:" << m_dbname;
    os << " _tbllen:" << m_tbllen;
    os << " _tblname:" << m_tblname;
    os << " _colcnt:" << m_colcnt;
    std::vector<uint16_t> tmp_vect(m_coltype_vect.size());
    std::copy(m_coltype_vect.begin(), m_coltype_vect.end(), tmp_vect.begin());
    os << " _coltype_vect.size():" << tmp_vect.size();
    os << print_vect(tmp_vect);
    os << " _metadata_vect.size():" << m_metadata_vect.size();
    os << print_vect(m_metadata_vect);
    return os.str();
}

int table_map_log_event::unpack(
    const char* buf,
    uint32_t event_len,
    parser_info& parser_info,
    format_description_log_event& desc_event)
{
    table_map_log_event event;
    const char* p = NULL;
    const char* event_body = NULL;
    p = event_body = buf + desc_event.m_event_header_len;
    event.m_table_id = get_uint64_value(p, 6);
    p += 6;
    event.m_flags = get_uint16_value(p);
    p += 2;
    //Read the variable part of the event
    const char* const vpart = p;
    // Extract the length of the various parts from the buffer
    uint8_t const* const ptr_dblen = (uint8_t const*)vpart + 0;
    event.m_dblen = * (uint8_t*) ptr_dblen;
    event.m_dbname.assign((char*)ptr_dblen + 1, event.m_dblen);
    // Length of database name + counter + terminating null
    uint8_t const* const ptr_tbllen = ptr_dblen + event.m_dblen + 2;
    event.m_tbllen = * (uint8_t*) ptr_tbllen;
    event.m_tblname.assign((char*)ptr_tbllen + 1, event.m_tbllen);
    table_info_t* p_table_info = NULL;
    bool b_is_table_found = parser_info.find_table(event.m_table_id,
                            event.m_dbname, event.m_tblname, p_table_info);
                            
    if(!b_is_table_found) {
        LOG_TRACE("Not found map in parser_info. Skip table map event."
                  "[table_map_id:%lu][db_name:%s][tbl_name:%s]",
                  event.m_table_id, event.m_dbname.c_str(), event.m_tblname.c_str());
        return 0;
    }
    
    // Length of table name + counter + terminating null
    uint8_t const* const ptr_colcnt = ptr_tbllen + event.m_tbllen + 2;
    char* ptr_after_colcnt = (char*) ptr_colcnt;
    event.m_colcnt = get_packed_length(&ptr_after_colcnt);
    
    for(uint32_t i = 0; i < event.m_colcnt; ++i) {
        event.m_coltype_vect.push_back((uint8_t)ptr_after_colcnt[i]);
    }
    
    ptr_after_colcnt += event.m_colcnt;
    uint32_t field_metadata_size = get_packed_length(&ptr_after_colcnt);
    
    if(0 != make_field_metadata(event.m_metadata_vect,
                                (uint8_t*)ptr_after_colcnt, field_metadata_size, event.m_coltype_vect)) {
        LOG_WARNING("make_field_metadata failed."
                    "[table_map_id:%lu][db_name:%s][tbl_name:%s]",
                    event.m_table_id, event.m_dbname.c_str(), event.m_tblname.c_str());
        return -1;
    }
    
    ptr_after_colcnt += field_metadata_size;
    ASSERT(field_metadata_size <= (event.m_colcnt * 2));
    uint32_t can_be_null_bytes = (event.m_colcnt + 7) / 8;
    
    // skip can_be_null_bytes data.
    if(ptr_after_colcnt + can_be_null_bytes - buf != event_len) {
        LOG_WARNING("event len equal check failed. [event_len=%u check_len=%lu], tablename: [%s]",
                    event_len, ptr_after_colcnt + can_be_null_bytes - buf, event.m_tblname.c_str());
        return -1;
    }
    
    // update table schema info.
    p_table_info->field_cnt = event.m_colcnt;
    p_table_info->field_type_vect = event.m_coltype_vect;
    p_table_info->field_metadata_vect = event.m_metadata_vect;
    LOG_TRACE("Found map of table map event in parser_info. update info."
              "[table_map_id:%lu][db_name:%s][tbl_name:%s]",
              event.m_table_id, event.m_dbname.c_str(), event.m_tblname.c_str());
    LOG_DEBUG("Table map event contents:%s", event.tostring().c_str());
    return 0;
}

int rows_log_event::unpack(const char* buf, const uint32_t len, format_description_log_event& desc_event, parser_info& info)
{
    m_row_data_vect.clear();
    const char* p = NULL;
    uint64_t table_id = 0;
    p = buf + desc_event.m_event_header_len;
    table_id = get_uint64_value(p, 6);
    
    //uint16_t flag = get_uint16_value(p + 6);
    if(m_header.type_code ==  WRITE_ROWS_EVENT_V2 || m_header.type_code == UPDATE_ROWS_EVENT_V2 || m_header.type_code == DELETE_ROWS_EVENT_V2) {
        uint16_t extra_data_length = get_uint16_value(p + 8);
        p += 8;
        //Have variable length header, check length,
        //which includes length bytes
        p += extra_data_length;
    } else {
        p += 8;
    }
    
    table_info_t* cur_table = NULL;
    bool b_found = info.find_table_by_mapid(table_id, cur_table);
    
    if(!b_found) {
        LOG_DEBUG("not found corresponding table_info_t skip.[map_id:%lu].", table_id);
        return 1;
    }
    
    uint64_t column_cnt = get_packed_length((char**)&p);
    
    if((uint64_t)cur_table->field_cnt != column_cnt) {
        LOG_WARNING("field_cnt of TABLE_MAP is not the same as ROW_EVENT. table_map: %d, row_event: %lu", cur_table->field_cnt, column_cnt);
        return -1;
    }
    
    int field_bit_len = (int)((column_cnt + 7) / 8);
    p += field_bit_len;
    
    if(m_header.type_code == UPDATE_ROWS_EVENT ||
            m_header.type_code == UPDATE_ROWS_EVENT_V2) {
        int field_bit_update_len = (int)((column_cnt + 7) / 8);
        p += field_bit_update_len;
        m_row_type = rows_log_event::OP_UPDATE;
    } else {
        if(m_header.type_code == WRITE_ROWS_EVENT ||
                m_header.type_code == WRITE_ROWS_EVENT_V2) {
            m_row_type = rows_log_event::OP_INSERT;
        } else {
            m_row_type = rows_log_event::OP_DELETE;
        }
    }
    
    m_dbname = cur_table->db_name;
    m_tblname = cur_table->table_name;
    m_charset = info.m_current_charset;
    const char* row_data = p;
    uint64_t data_len = m_header.event_length - (p - buf);
    uint32_t row_cnt = 0;
    uint32_t event_type = m_header.type_code;
    
    for(const char* value = row_data; value < row_data + data_len;) {
        ++row_cnt;
        uint64_t length = 0;
        bool all_proc_value_same = false;
        row_data_t row_data;
        vector<field_data_t>& data_vect = ((event_type == WRITE_ROWS_EVENT) || (event_type == WRITE_ROWS_EVENT_V2)) ? row_data.new_data_vect : row_data.old_data_vect;
        
        if(parse_row(data_vect, &length, value, column_cnt, *cur_table) != 0) {
            LOG_WARNING("parse row event row failed. row_cnt=%u, db_name:%s, table_name:%s",
                        row_cnt, m_dbname.c_str(), m_tblname.c_str());
            return -1;
        }
        
        value += length;
        
        // for update, have second image
        if(event_type == UPDATE_ROWS_EVENT ||
                event_type == UPDATE_ROWS_EVENT_V2) {
            /*
               For UPDATE_ROWS_LOG_EVENT, a row matching the first row-image is removed,
               and the row described by the second row-image is inserted.
            */
            if(parse_row(row_data.new_data_vect, &length,
                         value, column_cnt, *cur_table) != 0) {
                LOG_WARNING("parse row event row failed. row_cnt=%u", row_cnt);
                return -1;
            }
            
            value += length;
            all_proc_value_same = true;
            
            for(uint32_t i = 0; i < row_data.new_data_vect.size(); i++) {
                if(row_data.old_data_vect[i].is_monitor_change
                        && !row_data.old_data_vect[i].value_equal(row_data.new_data_vect[i])) {
                    all_proc_value_same = false;
                    break;
                }
            }
        }
        
        if(!all_proc_value_same) {
            m_row_data_vect.push_back(row_data);
        }
    }
    
    return 0;
}

int rows_log_event::parse_row(
    vector<field_data_t>& field_data_vect,
    uint64_t* length,
    const char* value,
    uint64_t column_cnt,
    const table_info_t& table_info)
{
    const char* p = value;
    uint8_t* null_bits = (uint8_t*)value;
    
    // Not check column_cnt and field_info_vect.size(). because the two values can be not equal.
    // schema has changed and subscribed not changed and binlog source changed.
    if(column_cnt != table_info.field_metadata_vect.size()
            || column_cnt != table_info.field_type_vect.size()) {
        LOG_WARNING("column_cnt(%lu) is not equal to field_meta_data_vect size(%zu) or field_type_vect size(%zu)]", column_cnt,
                    table_info.field_metadata_vect.size(), table_info.field_type_vect.size());
        return -1;
    }
    
    p += (column_cnt + 7) / 8;
    
    for(uint64_t i = 0; i < column_cnt; i++) {
        field_data_t field_data;
        bool is_null = (null_bits[i / 8] >> (i % 8)) & 0x01;
        // Give a default not proc field if i is out range of table_info.field_info_vect.
        const field_info_t& field_info = i < table_info.field_info_vect.size()
                                         ? table_info.field_info_vect[i]
                                         : field_info_t::S_DEFAULT_FIELD_INFO;
        uint64_t len = 0;
        
        if(0 != parse_field(field_data, &len, p,
                            table_info.field_type_vect[i], is_null,
                            table_info.field_metadata_vect[i], field_info)) {
            LOG_WARNING("parse field failed, field_index:%lu", i);
            return -1;
        }
        
        if(field_info.is_proc) {
            field_data_vect.push_back(field_data);
        }
        
        p += len;
    }
    
    *length = p - value;
    return 0;
}
int rows_log_event::parse_field(field_data_t& field_data, uint64_t* length, const char* ptr, uint32_t ftype, bool is_null, uint32_t meta, const field_info_t& field_info)
{
    uint32_t actual_field_type = 0;
    uint32_t str_field_size = 0;
    
    if(ftype != MYSQL_TYPE_STRING) {
        actual_field_type = ftype;
    } else {
        if(meta >= 256) {
            uint32_t byte0 = meta >> 8;
            uint32_t byte1 = meta && 0xff;
            
            if((byte0 & 0x30) != 0x30) {
                str_field_size = byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
                actual_field_type = byte0 | 0x30;
            } else {
                switch(byte0) {
                case MYSQL_TYPE_SET:
                case MYSQL_TYPE_ENUM:
                case MYSQL_TYPE_STRING:
                    actual_field_type = byte0;
                    str_field_size = byte1;
                    break;
                    
                default:
                    char tmp[5];
                    snprintf(tmp, sizeof(tmp), "%04X", meta);
                    LOG_WARNING("unknown column type. col_type=%d,  meta=%d(%s)",
                                ftype, meta, tmp);
                    return -1;
                }
            }
        } else {
            str_field_size = meta;
        }
    }
    
    // !!! Only check need proc (in another word, subscribed) field !!!
    // The binlog type can be not the same as schema type if the field is not processed
    //
    // In this case we need to go on process.
    // This case may happen when the following two condition both matches.
    // 1. compatibility schema change happened.
    //    <<< When schema change, we have checked to whether the index and type of
    //    subscribed column changed. >>>
    //    T1 : C1(int) C2(char)
    //    change to
    //    T2 : C1(bigint) C2(char)
    //    and we only care T2 column.
    // 2. Database source of binlog changed and need to parse an event before schema change
    //    so the binlog is not exactly match the current schema.
    // If not subscribed column type not agree with type in table_map, we use NOTICE log to show this case.
    if(field_info.mysql_type != actual_field_type) {
        if(field_info.is_proc) {
            LOG_WARNING("subscribed field type(%u) does not match schema field type(%u) field_name(%s)", actual_field_type, field_info.mysql_type, field_info.name.c_str());
            return -1;
        } else {
            LOG_NOTICE("not subscribed field type(%u) does not match schema field type(%u) field_name(%s)", actual_field_type, field_info.mysql_type,
                       field_info.name.c_str());
        }
    }
    
    if(is_null)
        goto end;
        
    // Need process not subscribed field to skip these fields data.
    // In these cases, no need to read data, only need get the length of these data and skip them.
    switch(actual_field_type) {
    case MYSQL_TYPE_LONG: {
            int si = get_sint32_value(ptr);
            *length = sizeof(int);
            
            if(!field_info.is_proc)
                break;
                
            field_data.set_value(&si, sizeof(int));
            break;
        }
        
    case MYSQL_TYPE_TINY: {
            char si = * (char*)ptr;
            *length = sizeof(char);
            
            if(!field_info.is_proc)
                break;
                
            field_data.set_value(&si, sizeof(char));
            break;
        }
        
    case MYSQL_TYPE_SHORT: {
            short si = get_sint16_value(ptr);
            *length = sizeof(short);
            
            if(!field_info.is_proc)
                break;
                
            field_data.set_value(&si, sizeof(short));
            break;
        }
        
    case MYSQL_TYPE_INT24: {
            int si = 0;
            *length = 3;
            
            if(!field_info.is_proc)
                break;
                
            if(field_info.binloglib_type == BINLOGLIB_TYPE_UINT32) {
                si = get_uint24_value(ptr);
            } else if(field_info.binloglib_type == BINLOGLIB_TYPE_INT32) {
                si = get_sint24_value(ptr);
            } else {
                LOG_WARNING("uint24 data mapping error. error_type: %u",
                            field_info.binloglib_type);
                return -1;
            }
            
            field_data.set_value(&si, sizeof(int));
            break;
        }
        
    case MYSQL_TYPE_LONGLONG: {
            uint64_t si = 0;
            *length = sizeof(uint64_t);
            
            if(!field_info.is_proc)
                break;
                
            si = get_uint64_value(ptr, 8);
            field_data.set_value(&si, sizeof(uint64_t));
            break;
        }
        
    case MYSQL_TYPE_NEWDECIMAL: {
            int i = 0;
            int end = 0;
            char buff[512];
            char* pos = buff;
            uint32_t precision = meta >> 8;
            uint32_t decimals = meta & 0xFF;
            uint32_t bin_size = decimal_bin_size((int)precision, (int)decimals);
            *length = bin_size;
            
            if(!field_info.is_proc)
                break;
                
            decimal_t dec;
            
            if(bin2decimal((uint8_t*) ptr, &dec, precision, decimals) != 0) {
                LOG_WARNING("bin2decimal failed");
                break;
            }
            
            if(dec.sign) {
                *pos++ = '-';
            }
            
            int intgx = ROUND_UP(dec.intg);
            end = ROUND_UP(dec.frac) + intgx;
            
            if(intgx == 0) {
                *pos++ = '0';
            } else {
                pos += snprintf(pos, sizeof(buff) - (pos - buff), "%d", dec.buf[0]);
                
                for(i = 1; i < intgx; i++) {
                    pos += snprintf(pos, sizeof(buff) - (pos - buff), "%09d", dec.buf[i]);
                }
            }
            
            char* point_pos = pos;
            *pos++ = '.';
            
            for(; i < end; i++) {
                pos += snprintf(pos, sizeof(buff) - (pos - buff), "%09d", dec.buf[i]);
            }
            
            pos--;
            
            while(*pos == '0') {
                //*pos = '\0';
                pos--;
            }
            
            if(*pos == '.') {
                //*pos = '\0';
                pos--;
            }
            
            if(dec.sign && *pos == '-') {
                *pos = '0';
            }
            
            if(decimals > 0) {
                if(point_pos == pos + 1) {
                    *++pos = '.';
                }
                
                const char* dest_pos = point_pos + decimals;
                
                while(++pos <= dest_pos) {
                    *pos = '0';
                }
                
                --pos;
            }
            
            field_data.set_value(buff, pos - buff + 1);
            break;
        }
        
    case MYSQL_TYPE_FLOAT: {
            *length = sizeof(float);
            
            if(!field_info.is_proc)
                break;
                
            float fl = * (float*)ptr;
            field_data.set_value(&fl, sizeof(float));
            break;
        }
        
    case MYSQL_TYPE_DOUBLE: {
            *length = sizeof(double);
            
            if(!field_info.is_proc)
                break;
                
            double dbl = * (double*)ptr;
            field_data.set_value(&dbl, sizeof(double));
            break;
        }
        
    case MYSQL_TYPE_BIT: {
            uint32_t nbits = ((meta >> 8) * 8) + (meta & 0xFF);
            uint32_t size = (nbits + 7) / 8;
            *length = size;
            
            if(!field_info.is_proc)
                break;
                
            field_data.set_value(ptr, size);
            break;
        }
        
    case MYSQL_TYPE_TIMESTAMP: {
            time_t tmp_time = 0;
            *length = sizeof(uint32_t);
            
            if(!field_info.is_proc)
                break;
                
            tmp_time = (time_t)get_uint32_value(ptr);
            field_data.set_value(&tmp_time, sizeof(uint32_t));
            break;
        }
        
    case MYSQL_TYPE_DATETIME: {
            *length = 8;
            
            if(!field_info.is_proc)
                break;
                
            uint64_t i64 = get_uint64_value(ptr, 8);
            field_data.set_value(&i64, sizeof(uint64_t));
            break;
        }
        
    case MYSQL_TYPE_TIME: {
            *length = 3;
            
            if(!field_info.is_proc)
                break;
                
            int i32 = get_sint24_value(ptr);
            field_data.set_value(&i32, sizeof(int));
            break;
        }
        
    case MYSQL_TYPE_DATE: {
            *length = 3;
            
            if(!field_info.is_proc)
                break;
                
            uint32_t i32 = get_uint24_value(ptr);
            field_data.set_value(&i32, sizeof(uint32_t));
            break;
        }
        
    case MYSQL_TYPE_YEAR: {
            *length = 1;
            
            if(!field_info.is_proc)
                break;
                
            uint8_t    i8 = * (uint8_t*)ptr;  // year from 1900
            field_data.set_value(&i8, sizeof(uint8_t));
            break;
        }
        
    case MYSQL_TYPE_ENUM: {
            uint16_t i16 = 0;
            *length = str_field_size;
            
            if(!field_info.is_proc)
                break;
                
            switch(*length) {
            case 1:
                i16 = (uint16_t) * (uint8_t*)ptr;
                break;
                
            case 2:
                i16 = get_uint16_value(ptr);
                break;
                
            default:
                LOG_WARNING("unknown enum length. [type='enum' len=%lu]", *length);
                break;
            }
            
            field_data.set_value(&i16, sizeof(uint16_t));
            break;
        }
        
    case MYSQL_TYPE_SET: {
            *length = str_field_size;
            
            if(!field_info.is_proc)
                break;
                
            uint64_t set_value = get_uint64_value(ptr, str_field_size);
            field_data.set_value(&set_value, sizeof(uint64_t));
            break;
        }
        
    case MYSQL_TYPE_GEOMETRY:
    case MYSQL_TYPE_BLOB: {
            uint32_t size = 0;
            
            switch(meta) {
            case 1:
                size = * (uint8_t*)ptr;
                *length = size + 1;
                break;
                
            case 2:
                size = get_uint16_value(ptr);
                *length = size + 2;
                break;
                
            case 3:
                size = get_uint24_value(ptr);
                *length = size + 3;
                break;
                
            case 4:
                size = get_uint32_value(ptr);
                *length = size + 4;
                break;
                
            default:
                LOG_WARNING("unknown BLOB size. [len=%u]", size);
                return -1;
            }
            
            if(field_info.is_proc) {
                field_data.set_value(ptr + meta, size);
            }
            
            break;
        }
        
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING: {
            uint32_t size = meta;
            uint8_t* ptr_res = NULL;
            uint32_t ret = get_quoted_length_buf((const uint8_t*)ptr, &ptr_res, &size);
            *length = ret;
            
            if(!field_info.is_proc)
                break;
                
            field_data.set_value(ptr_res, size);
            break;
        }
        
    case MYSQL_TYPE_STRING: {
            uint32_t real_len = str_field_size;
            uint8_t* ptr_res = NULL;
            uint32_t ret = get_quoted_length_buf((const uint8_t*)ptr, &ptr_res, &real_len);
            *length = ret;
            
            if(!field_info.is_proc)
                break;
                
            field_data.set_value(ptr_res, real_len);
            break;
        }
        
    default: {
            char tmp[5];
            snprintf(tmp, sizeof(tmp), "%04x", meta);
            LOG_WARNING("unknown column type. [type=%d meta=%d (%s)",
                        actual_field_type, meta, tmp);
            return -1;
        }
    }
    
end:

    if(field_info.is_proc) {
        // only set field_data info when this field is need proc.
        field_data.name = field_info.name;
        field_data.mysql_type = field_info.mysql_type;
        field_data.binloglib_type = field_info.binloglib_type;
        field_data.is_pk = field_info.is_pk;
        field_data.is_null = is_null;
        field_data.is_monitor_change = field_info.is_monitor_change;
        field_data.charset = field_info.charset;
        std::string value_str = "";
        field_data.get_value_string(value_str, false);
    }
    
    return 0;
}

int query_log_event::unpack(const char* buf, const uint32_t len, format_description_log_event& desc_event)
{
    const char* p = NULL;
    const char* exec_sql = NULL;
    const uint8_t* variable_end = NULL;
    uint16_t charset_id = 0;
    uint32_t sql_len = 0;
    uint32_t event_len = m_header.event_length;
    m_default_db.clear();
    m_real_sql.clear();
    
    if(!buf) {
        LOG_WARNING("parameter not valid. buffer is NULL");
        return -1;
    }
    
    query_post_header post_header;
    p = buf + desc_event.m_event_header_len;
#ifndef IS_BIG_ENDIAN
    memcpy(&post_header, p, sizeof(query_post_header));
#else
#error
#endif
    p += sizeof(query_post_header);
    variable_end = (uint8_t*)(p + post_header.status_vars_len);
    
    if(parse_query_event_variable(&charset_id, (uint8_t*)p, variable_end) != 0) {
        LOG_WARNING("parse_query_event_variable failed!");
        return -1;
    }
    
    /*const std::string& charset = _parser_info._id_charsets[charset_id];
    
    if(charset.empty()) {
        LOG_WARNING("can not find charset_id[%u] in charsets", charset_id);
        return -1;
    }
    
    _parser_info._current_charset = charset;
    */
    //WARNING : NO NULL at the end of sql statement.
    exec_sql = (char*)variable_end + post_header.db_name_len + 1;
    sql_len = event_len - (exec_sql - buf);
    m_default_db.assign((const char*)variable_end, post_header.db_name_len);
    m_real_sql.assign(exec_sql, sql_len);
    return 0;
}

int query_log_event::parse_query_event_variable(uint16_t* charset_id, const uint8_t* var_begin, const uint8_t* var_end)
{
    const uint8_t* pos = var_begin;
    
    while(pos < var_end) {
        switch(*pos++) {
        case Q_FLAGS2_CODE: {
                if(pos + 4 > var_end) {
                    return -1;
                }
                
                pos += 4;
                break;
            }
            
        case Q_SQL_MODE_CODE: {
                if(pos + 8 > var_end) {
                    return -1;
                }
                
                pos += 8;
                break;
            }
            
        case Q_CATALOG_NZ_CODE: {
                uint32_t len = *pos++;
                
                if(pos + len > var_end) {
                    return -1;
                }
                
                pos += len;
                break;
            }
            
        case Q_AUTO_INCREMENT: {
                if(pos + 4 > var_end) {
                    return -1;
                }
                
                pos += 4;
                break;
            }
            
        case Q_CHARSET_CODE: {
                if(pos + 6 > var_end) {
                    return -1;
                }
                
                *charset_id = get_uint16_value((char*)pos);
                pos = var_end;
                break;
            }
            
        case Q_TIME_ZONE_CODE: {
                uint32_t len = *pos++;
                
                if(pos + len > var_end) {
                    return -1;
                }
                
                pos += len;
                break;
            }
            
        case Q_LC_TIME_NAMES_CODE: {
                if(pos + 2 > var_end) {
                    return -1;
                }
                
                pos += 2;
                break;
            }
            
        case Q_CHARSET_DATABASE_CODE: {
                if(pos + 2 > var_end) {
                    return -1;
                }
                
                pos += 2;
                break;
            }
            
        case Q_TABLE_MAP_FOR_UPDATE_CODE: {
                if(pos + 8 > var_end) {
                    return -1;
                }
                
                pos += 8;
                break;
            }
            
        case Q_MASTER_DATA_WRITTEN_CODE: {
                if(pos + 4 > var_end) {
                    return -1;
                }
                
                pos += 4;
                break;
            }
            
        default:
            //That's why you must write status vars in growing order of code
            LOG_WARNING("query event has unknown status vars (first has code: %u), skipping the rest of them",
                        (uint32_t) * (pos - 1));
            pos = var_end;
        }
    }
    
    return 0;
}
}
