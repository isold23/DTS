# header files
h_sources = protobuf/record.pb.h \
dts_common.h \
binlog_event.h \
binlog_parser.h \
binlog_printer.h \
mysql_command.h \
dts_conf.h \
rpl_slave.h \
worker.h \
record_process.h

# cpp files
cpp_sources = protobuf/record.pb.cc \
dts_common.cpp \
binlog_event.cpp \
binlog_parser.cpp \
binlog_printer.cpp \
mysql_command.cpp \
dts_conf.cpp \
rpl_slave.cpp \
worker.cpp \
record_process.cpp \
dts.cpp

%.pb.h %.pb.cc: %.proto
	$(PROTOC) -I$(srcdir)/protobuf --cpp_out=$(srcdir)/protobuf $(srcdir)/protobuf/record.proto;
MOSTLYCLEANFILES = $(srcdir)/protobuf/record.pb.h $(srcdir)/protobuf/record.pb.cc

bin_PROGRAMS = dts
INCLUDES = -I/usr/include/mysql -I/usr/include/libxml2 -I/usr/local/include/google/protobuf -I$(top_srcdir)/common/include -I./protobuf -I./
bindir = $(prefix)/dts/bin
dts_LDADD = -lpthread -lrt -ldl -lssl -L/usr/lib64/mysql -lmysqlclient -lxml2 $(top_srcdir)/common/lib/libcommon.a /usr/local/lib/libprotobuf.a 
dts_SOURCES = $(h_sources) $(cpp_sources)

