#include "dts_conf.h"

namespace dts
{
    int dts_conf::load(const std::string &conf_file)
    {
        m_server_id = calculate_server_id();

        if (m_server_id == -1)
        {
            LOG_FATAL("calculate server id error.");
            return -1;
        }

        xmlDocPtr doc;
        doc = xmlReadFile(conf_file.c_str(), "UTF-8", XML_PARSE_RECOVER);

        if (doc == NULL)
        {
            LOG_FATAL("read xml file failed. file name : %s", conf_file.c_str());
            return -1;
        }

        xmlNodePtr rootNode = xmlDocGetRootElement(doc);

        if (rootNode == NULL)
        {
            LOG_FATAL("get root element failed.");
            return -1;
        }

        xmlNodePtr subNode;
        xmlNodePtr curNode;
        subNode = rootNode->children;

        if (subNode == NULL)
        {
            LOG_FATAL("root element get children failed.");
            return -1;
        }

        while (subNode != NULL && xmlStrcasecmp(subNode->name, BAD_CAST("subscribe")) != 0)
            subNode = subNode->next;

        curNode = subNode->children;

        if (curNode == NULL)
        {
            LOG_FATAL("get subscribe children failed.");
            return -1;
        }

        while (curNode != NULL)
        {
            if (xmlStrcasecmp(curNode->name, BAD_CAST("progress")) == 0)
            {
                xmlChar *value = xmlNodeGetContent(curNode);

                if (value == NULL)
                {
                    LOG_FATAL("get progress value failed.");
                    return -1;
                }

                m_progress_file_name = (char *)value;
                m_binlog_progress = get_progress_from_file();

                if (m_binlog_progress == 0)
                {
                    LOG_FATAL("get progress failed.");
                    return -1;
                }
            }

            if (xmlStrcasecmp(curNode->name, BAD_CAST("tables")) == 0)
            {
                break;
            }

            curNode = curNode->next;
        }

        if (curNode == NULL)
        {
            LOG_FATAL("get tables failed.");
            return -1;
        }

        curNode = curNode->children;

        while (curNode != NULL && xmlStrcasecmp(curNode->name, BAD_CAST("table")) != 0)
            curNode = curNode->next;

        if (curNode == NULL)
        {
            LOG_FATAL("get table failed.");
            return -1;
        }

        char *p = NULL;
        char *ptr = NULL;
        char *table_ptr = NULL;
        char *field_ptr = NULL;
        char *monitor_field_ptr = NULL;

        while (curNode != NULL)
        {
            if (xmlStrcasecmp(curNode->name, BAD_CAST("table")) != 0)
            {
                curNode = curNode->next;
                continue;
            }

            dts::table_t table;
            std::vector<std::string> table_vect;
            xmlNodePtr tblNode = curNode->children;

            while (tblNode != NULL)
            {
                if (xmlStrcasecmp(tblNode->name, BAD_CAST("database")) == 0)
                    table.db = (char *)xmlNodeGetContent(tblNode);

                if (xmlStrcasecmp(tblNode->name, BAD_CAST("table")) == 0)
                    table_ptr = (char *)xmlNodeGetContent(tblNode);

                if (xmlStrcasecmp(tblNode->name, BAD_CAST("field")) == 0)
                {
                    field_ptr = (char *)xmlNodeGetContent(tblNode);
                }

                if (xmlStrcasecmp(tblNode->name, BAD_CAST("monitor")) == 0)
                    monitor_field_ptr = (char *)xmlNodeGetContent(tblNode);

                tblNode = tblNode->next;
            }

            p = strtok_r(table_ptr, FIELD_REX_DELIMITER, &ptr);

            while (p)
            {
                char *field = p;
                field = dts::ltrim(field);

                if (field == NULL)
                {
                    LOG_FATAL("bad field rex. [field_name='%s']", p);
                    return -1;
                }

                field = dts::rtrim(field);

                if (field == NULL)
                {
                    LOG_FATAL("bad field rex. [field_name='%s']", p);
                    return -1;
                }

                if (field[0] != '\0')
                {
                    table_vect.push_back(field);
                }

                p = strtok_r(NULL, FIELD_REX_DELIMITER, &ptr);
            }

            std::ostringstream field_os;
            p = strtok_r(field_ptr, FIELD_REX_DELIMITER, &ptr);

            while (p)
            {
                char *field = p;
                field = dts::ltrim(field);

                if (field == NULL)
                {
                    LOG_FATAL("bad field rex. [field_name='%s']", p);
                    return -1;
                }

                field = dts::rtrim(field);

                if (field == NULL)
                {
                    LOG_FATAL("bad field rex. [field_name='%s']", p);
                    return -1;
                }

                if (field[0] != '\0')
                {
                    table.columns.insert(field);
                    field_os << "[" << field << "]";
                }

                p = strtok_r(NULL, FIELD_REX_DELIMITER, &ptr);
            }

            std::ostringstream monitor_field_os;
            p = strtok_r(monitor_field_ptr, FIELD_REX_DELIMITER, &ptr);

            while (p)
            {
                char *field = p;
                field = dts::ltrim(field);

                if (field == NULL)
                {
                    LOG_FATAL("bad field rex. [field_name='%s']", p);
                    return -1;
                }

                field = dts::rtrim(field);

                if (field == NULL)
                {
                    LOG_FATAL("bad field rex. [field_name='%s']", p);
                    return -1;
                }

                if (field[0] != '\0')
                {
                    table.monitor_columns.insert(field);
                    monitor_field_os << "[" << field << "]";
                }

                p = strtok_r(NULL, FIELD_REX_DELIMITER, &ptr);
            }

            std::string monitor_field_str;

            if (table.monitor_columns.empty())
            {
                table.monitor_columns = table.columns;
                monitor_field_str = field_os.str();
            }
            else
            {
                monitor_field_str = monitor_field_os.str();
            }

            for (size_t table_index = 0; table_index < table_vect.size(); ++table_index)
            {
                table.table = table_vect[table_index];
                std::ostringstream os;
                os << "[database:" << table.db << "][table:" << table.table << "]" << std::endl;
                os << "publish_field:" << field_os.str() << "" << std::endl;
                os << "monitor_field:" << monitor_field_os.str() << "" << std::endl;
                LOG_TRACE("subscribe detail:\n%s", os.str().c_str());
                m_table_list.push_back(table);
            }

            curNode = curNode->next;
        }

        curNode = subNode->children;

        if (curNode == NULL)
        {
            LOG_WARNING("get node failed.");
            return -1;
        }

        while (curNode != NULL && xmlStrcasecmp(curNode->name, BAD_CAST("dbinfos")) != 0)
        {
            curNode = curNode->next;
        }

        curNode = curNode->children;

        while (curNode != NULL && xmlStrcasecmp(curNode->name, BAD_CAST("dbinfo")) != 0)
            curNode = curNode->next;

        while (curNode != NULL)
        {
            if (xmlStrcasecmp(curNode->name, BAD_CAST("dbinfo")) != 0)
            {
                curNode = curNode->next;
                continue;
            }

            xmlNodePtr infoNode = curNode->children;
            dts::mysql_master_info info;

            while (infoNode != NULL)
            {
                if (xmlStrcasecmp(infoNode->name, BAD_CAST("dbusername")) == 0)
                    info.username = (char *)xmlNodeGetContent(infoNode);
                else if (xmlStrcasecmp(infoNode->name, BAD_CAST("dbpasswd")) == 0)
                    info.password = (char *)xmlNodeGetContent(infoNode);
                else if (xmlStrcasecmp(infoNode->name, BAD_CAST("dbhost")) == 0)
                    info.host = (char *)xmlNodeGetContent(infoNode);
                else if (xmlStrcasecmp(infoNode->name, BAD_CAST("dbport")) == 0)
                    info.port = atoi((char *)xmlNodeGetContent(infoNode));

                infoNode = infoNode->next;
            }

            m_master_list.push_back(info);
            curNode = curNode->next;
        }

        return 0;
    }

    uint64_t dts_conf::get_progress_from_file()
    {
        std::ifstream input;
        input.open(m_progress_file_name, std::fstream::in);

        if (input.fail())
        {
            LOG_FATAL("get progress failed.");
            return 0;
        }

        uint64_t progress = 0;
        input >> progress;

        if (input.fail())
        {
            LOG_FATAL("read progress from file failed.");
        }

        input.close();
        return progress;
    }

    int dts_conf::set_progress_to_file(const uint64_t progress)
    {
        std::ofstream output;
        output.open(m_progress_file_name, std::fstream::out);

        if (output.fail())
        {
            LOG_FATAL("open progress file failed.");
            return -1;
        }

        output << progress;

        if (output.fail())
        {
            LOG_FATAL("write progress to file failed.");
            output.close();
            return -1;
        }

        output.close();
        return 0;
    }

    std::string dts_conf::tostring() const
    {
        std::ostringstream os;
        os << "server id:" << m_server_id << std::endl;

        for (std::vector<dts::table_t>::const_iterator it = m_table_list.begin();
             it != m_table_list.end(); ++it)
        {
            os << "db:" << it->db << std::endl;
            os << "table:" << it->table << std::endl;
            uint32_t i = 0;

            for (std::set<std::string>::const_iterator set_ite = it->columns.begin();
                 set_ite != it->columns.end(); ++i, ++set_ite)
            {
                os << "column" << i << ":" << *set_ite << std::endl;
            }
        }

        for (std::vector<dts::mysql_master_info>::const_iterator it = m_master_list.begin();
             it != m_master_list.end(); ++it)
        {
            os << it->print();
        }

        return os.str();
    }

    int dts_conf::calculate_server_id()
    {
        char hname[128] = {0};
        char ip[16] = {0};
        int ret = gethostname(hname, sizeof(hname));

        if (ret != 0)
        {
            LOG_WARNING("gethostname error. error: %d", errno);
            return -1;
        }

        struct hostent *hent = NULL;

        hent = gethostbyname(hname);

        if (hent == NULL)
        {
            LOG_WARNING("gethostbyname error.error: %d", errno);
            return -1;
        }

        snprintf(ip, 256, "%s", inet_ntoa(*(struct in_addr *)(hent->h_addr_list[0])));
        pid_t pid = getpid();
        char tim[16] = {0};
        time_t t = time(0);
        strftime(tim, 64, "%Y%m%d%H", localtime(&t));
        std::stringstream ss;
        std::string str = "";
        ss << ip << pid << tim;
        ss >> str;
        uint32_t h = 0;
        const char *p = str.c_str();

        for (; *p; ++p)
            h = 131 * h + *p;

        ret = h & 0x7fffffff;
        return ret & 1 ? ret : ret + 1;
    }
} // end of namespace dts
