#include "dts_common.h"

namespace dts
{
    int get_sign_type(
        uint32_t mysql_type,
        uint32_t binloglib_type)
    {
        switch (mysql_type)
        {
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
            break;

        default:
            return 2;
        }

        switch (binloglib_type)
        {
        case BINLOGLIB_TYPE_UINT8:
        case BINLOGLIB_TYPE_UINT16:
        case BINLOGLIB_TYPE_UINT32:
        case BINLOGLIB_TYPE_UINT64:
            return 0;

        default:
            return 1;
        }
    }

    int bin2decimal(
        const uint8_t *from,
        decimal_t *to,
        int precision,
        int scale)
    {
        int error = 0;
        int intg = precision - scale;
        int intg0 = intg / DIG_PER_DEC1;
        int frac0 = scale / DIG_PER_DEC1;
        int intg0x = intg - intg0 * DIG_PER_DEC1;
        int frac0x = scale - frac0 * DIG_PER_DEC1;
        int intg1 = intg0 + (intg0x > 0);
        int frac1 = frac0 + (frac0x > 0);
        int *buf = to->buf;
        int mask = (*from & 0x80) ? 0 : -1;
        const uint8_t *stop;
        uint8_t d_copy[400];
        int bin_size = decimal_bin_size(precision, scale);
        memcpy(d_copy, from, bin_size);
        d_copy[0] ^= 0x80; /* first byte is sign */
        from = d_copy;

        if (intg1 + frac1 > (int)DECIMAL_BUFF_LENGTH)
        {
            LOG_WARNING("decimal is overflow. [intg1=%d frac1=%d len=%u]",
                        intg1, frac1, DECIMAL_BUFF_LENGTH);
            return -1;
        }

        to->sign = (mask != 0);
        to->intg = intg0 * DIG_PER_DEC1 + intg0x;
        to->frac = frac0 * DIG_PER_DEC1 + frac0x;

        if (intg0x)
        {
            int i = dig2bytes[intg0x];
            int x = 0;

            switch (i)
            {
            case 1:
                x = get_be_sint8_value((char *)from);
                break;

            case 2:
                x = get_be_sint16_value((char *)from);
                break;

            case 3:
                x = get_be_sint24_value((char *)from);
                break;

            case 4:
                x = get_be_sint32_value((char *)from);
                break;

            default:
                LOG_DEBUG("decimal integer must be mod 9=0. [i=%d]", i);
            }

            from += i;
            *buf = x ^ mask;

            if (((uint64_t)*buf) >= (uint64_t)powers10[intg0x + 1])
            {
                goto err;
            }

            if (buf > to->buf || *buf != 0)
            {
                buf++;
            }
            else
            {
                to->intg -= intg0x;
            }
        }

        for (stop = from + intg0 * sizeof(int); from < stop; from += sizeof(int))
        {
            *buf = get_be_sint32_value((char *)from) ^ mask;

            if (((uint32_t)*buf) > DIG_MAX)
            {
                goto err;
            }

            if (buf > to->buf || *buf != 0)
            {
                buf++;
            }
            else
            {
                to->intg -= DIG_PER_DEC1;
            }
        }

        for (stop = from + frac0 * sizeof(int); from < stop; from += sizeof(int))
        {
            *buf = get_be_sint32_value((char *)from) ^ mask;

            if (((uint32_t)*buf) > DIG_MAX)
            {
                goto err;
            }

            buf++;
        }

        if (frac0x)
        {
            int i = dig2bytes[frac0x];
            int x = 0;

            switch (i)
            {
            case 1:
                x = get_be_sint8_value((char *)from);
                break;

            case 2:
                x = get_be_sint16_value((char *)from);
                break;

            case 3:
                x = get_be_sint24_value((char *)from);
                break;

            case 4:
                x = get_be_sint32_value((char *)from);
                break;

            default:
                LOG_DEBUG("decimal frac must be mod 9 = 0. [i=%d]", i);
            }

            *buf = (x ^ mask) * powers10[DIG_PER_DEC1 - frac0x];

            if (((uint32_t)*buf) > DIG_MAX)
            {
                goto err;
            }

            buf++;
        }

        return error;
    err:
        return -1;
    }

    int make_field_metadata(
        std::vector<uint16_t> &metadata_vect,
        const uint8_t *meta_buffer,
        uint32_t meta_len,
        const std::vector<uint8_t> &field_types_vect)
    {
        if (NULL == meta_buffer)
        {
            LOG_WARNING("meta_buffer is NULL");
            return -1;
        }

        uint32_t index = 0;
        const uint8_t *field_metadata = meta_buffer;

        for (uint32_t k = 0; k < field_types_vect.size(); ++k)
        {
            uint16_t metadata = 0;

            switch (field_types_vect[k])
            {
            case MYSQL_TYPE_TINY_BLOB:
            case MYSQL_TYPE_BLOB:
            case MYSQL_TYPE_MEDIUM_BLOB:
            case MYSQL_TYPE_LONG_BLOB:
            case MYSQL_TYPE_DOUBLE:
            case MYSQL_TYPE_FLOAT:
            case MYSQL_TYPE_GEOMETRY:
            {
                metadata = field_metadata[index++];
                break;
            }

            case MYSQL_TYPE_SET:
            case MYSQL_TYPE_ENUM:
            case MYSQL_TYPE_STRING:
            {
                metadata = field_metadata[index++] << 8U; // real_type
                metadata |= field_metadata[index++];      // pack or field length
                break;
            }

            case MYSQL_TYPE_BIT:
            {
                metadata = field_metadata[index++];
                metadata |= (field_metadata[index++] << 8U);
                break;
            }

            case MYSQL_TYPE_VARCHAR:
            {
                metadata = get_uint16_value((char *)&field_metadata[index]);
                index += 2;
                break;
            }

            case MYSQL_TYPE_NEWDECIMAL:
            {
                metadata = field_metadata[index++] << 8U; // precision
                metadata |= field_metadata[index++];      // decimals
                break;
            }

            default:
                metadata = 0;
                break;
            }

            metadata_vect.push_back(metadata);
        }

        if (meta_len != index)
        {
            LOG_WARNING("metadata len equal check failed. [metadata_len=%u check_len=%u]",
                        meta_len, index);
            return -1;
        }

        return 0;
    }

    int str2type(
        const char *type_str,
        uint32_t *mysql_type,
        uint32_t *binloglib_type,
        bool *is_signed)
    {
        if (NULL == type_str || NULL == mysql_type || NULL == binloglib_type || NULL == is_signed)
        {
            return -1;
        }

        *is_signed = false;
        *binloglib_type = BINLOGLIB_TYPE_UNKNOW;
        *mysql_type = MYSQL_TYPE_UNKNOW;

        if (0 == memcmp(type_str, "tinyint", 7))
        {
            *mysql_type = MYSQL_TYPE_TINY;
            /* is unsigned ? */
            *is_signed = (NULL == strstr(type_str, "unsigned"));
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "smallint", 8))
        {
            *mysql_type = MYSQL_TYPE_SHORT;
            *is_signed = (NULL == strstr(type_str, "unsigned"));
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "mediumint", 9))
        {
            *mysql_type = MYSQL_TYPE_INT24;
            *is_signed = (NULL == strstr(type_str, "unsigned"));
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "int", 3))
        {
            *mysql_type = MYSQL_TYPE_LONG;
            *is_signed = (NULL == strstr(type_str, "unsigned"));
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "bigint", 6))
        {
            *mysql_type = MYSQL_TYPE_LONGLONG;
            *is_signed = (NULL == strstr(type_str, "unsigned"));
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "decimal", 7))
        {
            *mysql_type = MYSQL_TYPE_NEWDECIMAL;
            *is_signed = (NULL == strstr(type_str, "unsigned"));
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "date", 5))
        {
            *mysql_type = MYSQL_TYPE_DATE;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "time", 5))
        {
            *mysql_type = MYSQL_TYPE_TIME;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "datetime", 8))
        {
            *mysql_type = MYSQL_TYPE_DATETIME;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "timestamp", 9))
        {
            *mysql_type = MYSQL_TYPE_TIMESTAMP;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "year", 4))
        {
            *mysql_type = MYSQL_TYPE_YEAR;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "char", 4))
        {
            *mysql_type = MYSQL_TYPE_STRING;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "varchar", 7))
        {
            *mysql_type = MYSQL_TYPE_VARCHAR;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "tinytext", 8))
        {
            *mysql_type = MYSQL_TYPE_BLOB;
            *binloglib_type = BINLOGLIB_TYPE_STRING;
        }
        else if (0 == memcmp(type_str, "mediumtext", 10))
        {
            *mysql_type = MYSQL_TYPE_BLOB;
            *binloglib_type = BINLOGLIB_TYPE_STRING;
        }
        else if (0 == memcmp(type_str, "text", 4))
        {
            *mysql_type = MYSQL_TYPE_BLOB;
            *binloglib_type = BINLOGLIB_TYPE_STRING;
        }
        else if (0 == memcmp(type_str, "longtext", 8))
        {
            *mysql_type = MYSQL_TYPE_BLOB;
            *binloglib_type = BINLOGLIB_TYPE_STRING;
        }
        else if (0 == memcmp(type_str, "tinyblob", 8))
        {
            *mysql_type = MYSQL_TYPE_BLOB;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "mediumblob", 10))
        {
            *mysql_type = MYSQL_TYPE_BLOB;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "blob", 4))
        {
            *mysql_type = MYSQL_TYPE_BLOB;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "longblob", 8))
        {
            *mysql_type = MYSQL_TYPE_BLOB;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "bit", 3))
        {
            *mysql_type = MYSQL_TYPE_BIT;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "binary", 6))
        {
            *mysql_type = MYSQL_TYPE_STRING;
            *binloglib_type = BINLOGLIB_TYPE_BINARY;
        }
        else if (0 == memcmp(type_str, "varbinary", 9))
        {
            *mysql_type = MYSQL_TYPE_VARCHAR;
            *binloglib_type = BINLOGLIB_TYPE_BINARY;
        }
        /*}}}*/
        else if (0 == memcmp(type_str, "enum", 4))
        {
            *mysql_type = MYSQL_TYPE_ENUM;
            *is_signed = false;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "float", 5))
        {
            *mysql_type = MYSQL_TYPE_FLOAT;
            *is_signed = false;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "double", 6))
        {
            *mysql_type = MYSQL_TYPE_DOUBLE;
            *is_signed = false;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "set", 3))
        {
            *mysql_type = MYSQL_TYPE_SET;
            *is_signed = false;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else if (0 == memcmp(type_str, "geometry", 9) || 0 == memcmp(type_str, "geometrycollection", 17) || 0 == memcmp(type_str, "multipoint", 11) || 0 == memcmp(type_str, "linestring", 11) || 0 == memcmp(type_str, "point", 5) || 0 == memcmp(type_str, "multilinestring", 16) || 0 == memcmp(type_str, "polygon", 8) || 0 == memcmp(type_str, "multipolygon", 13))
        {
            *mysql_type = MYSQL_TYPE_GEOMETRY;
            mysql_type2binloglib_type(binloglib_type, *mysql_type, *is_signed);
        }
        else
        {
            LOG_WARNING("%s unknown field type in mysql. [type=%s]",
                        __FUNCTION__, type_str);
            return -1;
        }

        if (*binloglib_type == BINLOGLIB_TYPE_UNKNOW)
        {
            return -1;
        }

        return 0;
    }

    int
    mysql_type2binloglib_type(
        uint32_t *binloglib_type,
        uint32_t mysql_type,
        bool is_signed)
    {
        /*{{{*/
        if (binloglib_type == NULL)
        {
            return -1;
        }

        *binloglib_type = BINLOGLIB_TYPE_UNKNOW;

        switch (mysql_type)
        {
        case MYSQL_TYPE_NEWDECIMAL:
            *binloglib_type = BINLOGLIB_TYPE_DECIMAL;
            break;

        case MYSQL_TYPE_TINY:
            is_signed ? *binloglib_type = BINLOGLIB_TYPE_INT8 :
                      *binloglib_type = BINLOGLIB_TYPE_UINT8;
            break;

        case MYSQL_TYPE_SHORT:
            is_signed ? *binloglib_type = BINLOGLIB_TYPE_INT16 :
                      *binloglib_type = BINLOGLIB_TYPE_UINT16;
            break;

        case MYSQL_TYPE_INT24:
            is_signed ? *binloglib_type = BINLOGLIB_TYPE_INT32 :
                      *binloglib_type = BINLOGLIB_TYPE_UINT32;
            break;

        case MYSQL_TYPE_LONG:
            is_signed ? *binloglib_type = BINLOGLIB_TYPE_INT32 :
                      *binloglib_type = BINLOGLIB_TYPE_UINT32;
            break;

        case MYSQL_TYPE_LONGLONG:
            is_signed ? *binloglib_type = BINLOGLIB_TYPE_INT64 :
                      *binloglib_type = BINLOGLIB_TYPE_UINT64;
            break;

        case MYSQL_TYPE_TIMESTAMP:
            *binloglib_type = BINLOGLIB_TYPE_TIMESTAMP;
            break;

        case MYSQL_TYPE_DATE:
            *binloglib_type = BINLOGLIB_TYPE_STRING;
            break;

        case MYSQL_TYPE_TIME:
            *binloglib_type = BINLOGLIB_TYPE_INT32;
            break;

        case MYSQL_TYPE_DATETIME:
            *binloglib_type = BINLOGLIB_TYPE_UINT64;
            break;

        case MYSQL_TYPE_YEAR:
            *binloglib_type = BINLOGLIB_TYPE_UINT8;
            break;

        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_STRING:
            *binloglib_type = BINLOGLIB_TYPE_STRING;
            break;

        case MYSQL_TYPE_BIT:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_VAR_STRING:
            *binloglib_type = BINLOGLIB_TYPE_BINARY;
            break;

        case MYSQL_TYPE_ENUM:
            *binloglib_type = BINLOGLIB_TYPE_UINT16;
            break;

        case MYSQL_TYPE_FLOAT:
            *binloglib_type = BINLOGLIB_TYPE_FLOAT;
            break;

        case MYSQL_TYPE_DOUBLE:
            *binloglib_type = BINLOGLIB_TYPE_DOUBLE;
            break;

        case MYSQL_TYPE_SET:
            *binloglib_type = BINLOGLIB_TYPE_UINT64;
            break;

        case MYSQL_TYPE_GEOMETRY:
            *binloglib_type = BINLOGLIB_TYPE_BINARY;
            break;
#if 0
        
    case MYSQL_TYPE_DECIMAL :
    case MYSQL_TYPE_NEWDATE :
    case MYSQL_TYPE_NULL :
#endif
        default:
            LOG_WARNING("unknown field type mapping. [mysql_type=%u]",
                        mysql_type);
            return -1;
        }

        return 0;
    }

    /**
        @brief
               get real std::string buffer and length for var-std::string,
               in which first several bytes are buffer length.
               the ptr length must < 65535(2 bytes),
               used for parsing 'char' and 'varchar' in MySQL.

        @param [in] ptr          : buffer
        @param [out] ptr_res     : real buffer pointer
        @param [in] res_len      : all buffer length
        @param [out] res_len     : real buffer length
        @return
                all buffer length
    **/
    uint32_t
    get_quoted_length_buf(
        const uint8_t *ptr,
        uint8_t **ptr_res,
        uint32_t *res_len)
    {
        uint32_t length = (uint32_t)*res_len;

        if (length < 256)
        {
            /* buffer length stored in 1 byte */
            length = *ptr;
            *ptr_res = (uint8_t *)(ptr + 1);
            *res_len = length;
            return length + 1;
        }
        else
        {
            /* buffer length stored in 2 byte */
            length = get_uint16_value((char *)ptr);
            *ptr_res = (uint8_t *)(ptr + 2);
            *res_len = length;
            return length + 2;
        }
    }

    bool is_sql_starts_with(
        const char *sql,
        const char *first_pattern,
        const char *second_pattern)
    {
        int first_pattern_len = strlen(first_pattern);

        if (0 != strncasecmp(sql, first_pattern, first_pattern_len))
            return false;

        const char *c_loc = sql + first_pattern_len;

        if (*c_loc != ' ' && *c_loc != '\t' && *c_loc != '\n')
            // No extra character between first_pattern and second_pattern.
            return false;

        while (*c_loc == ' ' || *c_loc == '\t' || *c_loc == '\n')
            ++c_loc;

        return 0 == strncasecmp(c_loc, second_pattern, strlen(second_pattern));
    }

    static field_info_t get_default_field_info()
    {
        field_info_t field_info;
        field_info.id = 0;
        field_info.name = "";
        field_info.mysql_type = MYSQL_TYPE_UNKNOW;
        field_info.binloglib_type = BINLOGLIB_TYPE_UNKNOW;
        field_info.is_sign = false;
        field_info.can_null = true;
        field_info.is_pk = false;
        field_info.is_proc = false;
        field_info.is_monitor_change = false;
        return field_info;
    }

    const field_info_t field_info_t::S_DEFAULT_FIELD_INFO =
        get_default_field_info();

    bool field_info_t::is_compatible(
        const std::vector<field_info_t> &old_fields,
        const std::vector<field_info_t> &new_fields)
    {
        bool btmp = old_fields.size() < new_fields.size();
        uint32_t size = btmp ? old_fields.size() : new_fields.size();

        for (uint32_t i = 0; i < size; ++i)
        {
            if (old_fields[i].is_proc != new_fields[i].is_proc)
                return false;

            if (!old_fields[i].is_proc)
                continue;

            if (old_fields[i].id != new_fields[i].id)
                return false;

            if (old_fields[i].name != new_fields[i].name)
                return false;

            if (old_fields[i].mysql_type != new_fields[i].mysql_type)
                return false;

            if (old_fields[i].binloglib_type != new_fields[i].binloglib_type)
                return false;
        }

        const std::vector<field_info_t> long_fields = btmp ? new_fields : old_fields;

        for (uint32_t i = size; i < long_fields.size(); ++i)
        {
            if (long_fields[i].is_proc)
                return false;
        }

        return true;
    }
}
