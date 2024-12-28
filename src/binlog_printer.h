#ifndef _BINLOG_PRINTER_H_
#define _BINLOG_PRINTER_H_

#include "binlog_event.h"
#include "dts_common.h"

namespace dts
{
    class binlog_processor;
    enum printer_type_t
    {
        PROTO_BUF_PRINTER = 0,
    };

    class binlog_printer
    {
    public:
        binlog_printer()
        {
        }
        virtual ~binlog_printer()
        {
        }
        virtual int print(
            const binlog_processor *processor,
            const log_event_header &header,
            rows_log_event *binlog,
            std::list<dts_record::Record *> &res) = 0;
    };

    class pb_printer : public binlog_printer
    {
    public:
        pb_printer()
        {
        }
        ~pb_printer()
        {
        }
        int print(
            const binlog_processor *processor,
            const log_event_header &header,
            rows_log_event *binlog,
            std::list<dts_record::Record *> &res);
    };
} // end of namespace dts
#endif
