#include "binlog_printer.h"
#include "rpl_slave.h"

namespace dts
{

    int print_field(const field_data_t &field_data, dts_record::Field &field)
    {
        field.set_name(field_data.name);
        int ret = get_sign_type(
            field_data.mysql_type, field_data.binloglib_type);
        field.set_is_signed(1 == ret);
        field.set_mysql_type((dts_record::MysqlType)field_data.mysql_type);
        field.set_is_pk(field_data.is_pk);

        if (!field_data.charset.empty())
        {
            field.set_charset(field_data.charset);
        }

        return 0;
    }

    int pb_printer::print(
        const binlog_processor *processor,
        const log_event_header &header,
        rows_log_event *rows_event,
        std::list<dts_record::Record *> &proto_buf_list)
    {
        for (uint32_t i = 0; i < rows_event->m_row_data_vect.size(); ++i)
        {
            dts_record::Record *event = new dts_record::Record;
            event->set_host(processor->get_host());
            event->set_port(processor->get_port());
            event->set_log_file(processor->m_binlog_name);
            event->set_offset(header.next_pos - header.event_length);
            event->set_safe_offset(processor->m_safe_offset);
            event->set_safe_timestamp(processor->m_safe_timestamp);
            event->set_timestamp(header.timestamp);
            uint64_t progress = 0;
            progress = processor->make_progress();
            event->set_progress(progress);
            event->set_db(rows_event->m_dbname);
            event->set_table(rows_event->m_tblname);
            event->set_event_type((dts_record::EventType)rows_event->m_row_type);
            event->set_charset(rows_event->m_charset);
            std::string proto_buf;
            const row_data_t &row_data = rows_event->m_row_data_vect[i];

            if (rows_log_event::OP_INSERT == rows_event->m_row_type)
            {
                for (uint32_t i = 0; i < row_data.new_data_vect.size(); ++i)
                {
                    const field_data_t &field_data = row_data.new_data_vect.at(i);
                    dts_record::Field *field = event->add_field();
                    print_field(field_data, *field);

                    if (!field_data.is_null)
                    {
                        std::string value_str;

                        if (0 == field_data.get_value_string(value_str, false))
                        {
                            field->set_new_value(value_str);
                        }
                        else
                        {
                            LOG_WARNING("Failed to get_value_string of [field_index:%u][field:%s]",
                                        i, field_data.name.c_str());
                        }
                    }
                }
            }
            else if (rows_log_event::OP_DELETE == rows_event->m_row_type)
            {
                for (uint32_t i = 0; i < row_data.old_data_vect.size(); ++i)
                {
                    const field_data_t &field_data = row_data.old_data_vect.at(i);
                    dts_record::Field *field = event->add_field();
                    print_field(field_data, *field);

                    if (!field_data.is_null)
                    {
                        std::string value_str;

                        if (0 == field_data.get_value_string(value_str, false))
                        {
                            field->set_old_value(value_str);
                        }
                        else
                        {
                            LOG_WARNING("Failed to get_value_string of [field_index:%u][field:%s]",
                                        i, field_data.name.c_str());
                        }
                    }
                }
            }
            else
            {
                if (row_data.old_data_vect.size() != row_data.new_data_vect.size())
                {
                    LOG_WARNING("old_data size[%zu] != new_data size[%zu] when print event",
                                row_data.old_data_vect.size(), row_data.new_data_vect.size());
                    return -1;
                }

                for (uint32_t i = 0; i < row_data.old_data_vect.size(); ++i)
                {
                    const field_data_t &old_field_data = row_data.old_data_vect.at(i);
                    dts_record::Field *field = event->add_field();
                    print_field(old_field_data, *field);

                    if (!old_field_data.is_null)
                    {
                        std::string value_str;

                        if (0 == old_field_data.get_value_string(value_str, false))
                        {
                            field->set_old_value(value_str);
                            // LOG_DEBUG("old value: %s:%s", old_field_data.name.c_str(),  value_str.c_str());
                        }
                        else
                        {
                            LOG_WARNING("Failed to get_value_string of [field_index:%u][field:%s]",
                                        i, old_field_data.name.c_str());
                        }
                    }

                    const field_data_t &new_field_data = row_data.new_data_vect.at(i);
                    print_field(new_field_data, *field);

                    if (!new_field_data.is_null)
                    {
                        std::string value_str;

                        if (0 == new_field_data.get_value_string(value_str, false))
                        {
                            field->set_new_value(value_str);
                            // LOG_DEBUG("new value %s : %s", new_field_data.name.c_str(), value_str.c_str());
                        }
                        else
                        {
                            LOG_WARNING("Failed to get_value_string of [field_index:%u][field:%s]",
                                        i, new_field_data.name.c_str());
                        }
                    }
                }
            }

            proto_buf_list.push_back(event);
        }

        return 0;
    }

}
