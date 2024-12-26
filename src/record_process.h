#ifndef _RECORD_PROCESS_H_
#define _RECORD_PROCESS_H_

#include <list>
#include "rdt_conf.h"

namespace rdt
{
    class record_process
    {
    public:
        record_process() {}
        ~record_process() {}

        // init configure
        int init(const rdt_conf *conf);
        int process(std::list<rdt_record::Record *> &event);
    };
} // end of namespace rdt

#endif //_RECORD_PROCESS_H_
