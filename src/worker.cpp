#include "worker.h"

namespace dts
{
    int binlog_worker::init(std::string &conf_file)
    {
        if (0 != m_conf.load(conf_file))
        {
            LOG_FATAL("configure load failed.");
            return -1;
        }

        if (m_record_process.init(&m_conf) != 0)
        {
            LOG_FATAL("pre process init failed.");
            return -1;
        }

        for (size_t i = 0; i < m_conf.m_master_list.size(); ++i)
        {
            dts::binlog_processor *binlog_processor = new dts::binlog_processor;

            if (NULL == binlog_processor)
                goto err;

            m_binlog_processor_vect.push_back(binlog_processor);
            int ret = binlog_processor->init(
                m_conf.m_master_list[i],
                m_conf.m_server_id + i * 2,
                &m_conf);

            if (ret != 0)
            {
                LOG_FATAL("binlog processor init failed. host: %s:%d", m_conf.m_master_list[i].host.c_str(), m_conf.m_master_list[i].port);
                goto err;
            }

            for (std::vector<dts::table_t>::const_iterator it = m_conf.m_table_list.begin();
                 it != m_conf.m_table_list.end(); ++it)
            {
                int sub_ret = binlog_processor->subscribe(*it);

                if (sub_ret != 0)
                {
                    LOG_FATAL("monitor_field must be subset of field.database: %s table: %d", it->db.c_str(), it->table.c_str());
                    goto err;
                }
            }

            ret = binlog_processor->init_server_index(i);

            if (0 != ret)
            {
                LOG_FATAL("binlog_processor init_server_index failed.");
                goto err;
            }
        }

        m_current_processor = NULL;
        m_publish_state = ERROR_STATE;
        return 0;
    err:

        for (size_t i = 0; i < m_binlog_processor_vect.size(); ++i)
        {
            delete m_binlog_processor_vect[i];
            m_binlog_processor_vect[i] = NULL;
        }

        m_binlog_processor_vect.clear();
        return -1;
    }

    int binlog_worker::run()
    {
        int ret = 0;
        ret = set_progress(m_conf.m_binlog_progress);

        if (0 != ret)
        {
            LOG_FATAL("set current master by progress failed.");
            return -1;
        }

        if (m_publish_state != PUBLISHING_STATE)
        {
            int tmp_ret = m_current_processor->send_binlog_dump();

            if (0 != tmp_ret)
            {
                LOG_WARNING("send_binlog_dump failed.[%s]", m_current_processor->get_master_info_str().c_str());
                return -1;
            }

            m_publish_state = PUBLISHING_STATE;
        }
        else
        {
            LOG_WARNING("state: %d", m_publish_state);
            return -1;
        }

        process();
        return 0;
    }
    int binlog_worker::destroy()
    {
        for (size_t i = 0; i < m_binlog_processor_vect.size(); ++i)
        {
            delete m_binlog_processor_vect[i];
            m_binlog_processor_vect[i] = NULL;
        }

        m_binlog_processor_vect.clear();
        return 0;
    }

    int binlog_worker::process_record()
    {
        if (m_record_process.process(m_event_list) != 0)
        {
            LOG_WARNING("record process failed");
        }

        std::list<dts_record::Record *>::iterator iter = m_event_list.begin();

        for (; iter != m_event_list.end(); ++iter)
        {
            dts_record::Record *ptrevent = NULL;
            ptrevent = (dts_record::Record *)*iter;

            if (ptrevent != NULL)
            {
                delete ptrevent;
                ptrevent = NULL;
            }
        }

        m_event_list.clear();
        return 0;
    }

    int binlog_worker::process()
    {
        int get_rows_event_ret = 0;
        dts::binlog_processor *p_processor = NULL;

        for (;;)
        {
            LOG_DEBUG("-----------------------------------------------------------------------------------");
            p_processor = NULL;

            if (PUBLISHING_STATE == m_publish_state)
            {
                if (PUBLISHING_STATE == m_publish_state)
                {
                    p_processor = m_current_processor;
                }

                if (NULL != p_processor)
                {
                    get_rows_event_ret = p_processor->get_rows_event(m_event_list);
                }
            }

            if (NULL == p_processor)
            {
                usleep(10000);
                continue;
            }

            if (m_event_list.size() > 0)
            {
                int ret = process_record();

                if (ret < 0)
                {
                    LOG_WARNING("process event failed.");
                    continue;
                }
            }

            if (0 == get_rows_event_ret)
            {
                fdatasync(1);
            }

            if (1 == get_rows_event_ret)
            {
                usleep(10000);
            }
            else if (-1 == get_rows_event_ret)
            {
                if (PUBLISHING_STATE != m_publish_state)
                    continue;

                if (PUBLISHING_STATE != m_publish_state || m_current_processor != p_processor)
                    continue;

                m_publish_state = PUBLISHING_STATE;
            }
        }

        return 0;
    }
    int binlog_worker::set_current_progress(uint64_t progress)
    {
        uint32_t binlog_index = 0;
        uint32_t binlog_offset = 0;
        uint32_t server_index = 0;
        int ret = dts::binlog_processor::parse_progress(
            progress,
            binlog_index,
            binlog_offset,
            server_index);

        if (ret != 0)
        {
            LOG_WARNING("parse progress failed.");
            return -1;
        }

        if (server_index >= m_binlog_processor_vect.size())
        {
            LOG_WARNING("server_index:%u >= binlog_process count:%zu",
                        server_index, m_binlog_processor_vect.size());
            return -1;
        }

        dts::binlog_processor *processor = m_binlog_processor_vect[server_index];

        if (processor != NULL)
        {
            std::string binlog_name;
            processor->make_binlog_name(binlog_index, binlog_name);
            LOG_DEBUG("binlog name : %s offset: %u", binlog_name.c_str(), binlog_offset);
        }

        return 0;
    }

    int binlog_worker::set_progress(uint64_t progress)
    {
        uint32_t binlog_index = 0;
        uint32_t binlog_offset = 0;
        uint32_t server_index = 0;
        int ret = binlog_processor::parse_progress(progress, binlog_index, binlog_offset, server_index);

        if (ret != 0)
        {
            LOG_WARNING("parse progress failed");
            return -1;
        }
        else
        {
            LOG_NOTICE("binlog index: %d binlog offset: %d server index: %d", binlog_index, binlog_offset, server_index);
        }

        if (server_index >= m_binlog_processor_vect.size())
        {
            LOG_WARNING("server_index:%u >= binlog_process count:%zu",
                        server_index, m_binlog_processor_vect.size());
            return -1;
        }

        dts::binlog_processor *processor = m_binlog_processor_vect[server_index];
        std::string binlog_name;
        processor->make_binlog_name(binlog_index, binlog_name);
        mysql_master_info info;
        info.host = processor->get_host();
        info.port = processor->get_port();
        info.binlog_name = binlog_name;
        info.offset = binlog_offset;
        return set_current_master_by_location(info);
    }
    int binlog_worker::set_current_master_by_location(mysql_master_info &info)
    {
        int current_state = m_publish_state;

        if (PUBLISHING_STATE != current_state)
        {
            current_state = m_publish_state;

            if (PUBLISHING_STATE != current_state)
            {
                dts::binlog_processor *processor = NULL;

                for (size_t i = 0; i < m_binlog_processor_vect.size(); ++i)
                {
                    dts::binlog_processor *cur_processor = m_binlog_processor_vect[i];

                    if (info.host == cur_processor->get_host() && cur_processor->get_port() == info.port)
                    {
                        processor = cur_processor;
                        break;
                    }
                }

                if (NULL == processor)
                {
                    LOG_WARNING("not found processor. host: %s port: %d", info.host, info.port);
                    return -1;
                }

                int tmp_ret = 0;
                tmp_ret = processor->set_start_pos(info.binlog_name.c_str(), info.offset);

                if (0 != tmp_ret)
                {
                    LOG_WARNING("set start pos failed. master info: %s", processor->get_master_info_str());
                    return -1;
                }

                m_current_processor = processor;
                m_publish_state = STOPPED_STATE;
                return 0;
            }
        }

        return -1;
    }

} // end of namespace dts
