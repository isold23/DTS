#include "record_process.h"

namespace dts
{
    int record_process::init(const dts_conf *conf)
    {
        if (NULL == conf)
        {
            LOG_FATAL("conf is null.");
            return -1;
        }

        return 0;
    }

    int record_process::process(std::list<dts_record::Record *> &event)
    {
        std::list<dts_record::Record *>::iterator iter = event.begin();

        for (; iter != event.end(); ++iter)
        {
            LOG_NOTICE("**********************************");
            dts_record::Record *ptrEvent = *iter;
            std::string debuginfo = ptrEvent->DebugString();
            LOG_NOTICE("%s", debuginfo.c_str());
            LOG_NOTICE("**********************************");
        }

        return 0;
    }
} // end of namespace dts
