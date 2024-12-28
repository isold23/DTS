#ifndef _RECORD_PROCESS_H_
#define _RECORD_PROCESS_H_

#include <list>
#include "dts_conf.h"

namespace dts
{
    class record_process
    {
    public:
        record_process() {}
        ~record_process() {}

        // init configure
        int init(const dts_conf *conf);
        int process(std::list<dts_record::Record *> &event);
    };
} // end of namespace dts

#endif //_RECORD_PROCESS_H_
