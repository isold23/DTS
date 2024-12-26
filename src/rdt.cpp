#include <stdio.h>
#include <iostream>
#include <string>
#include <time.h>
#include <signal.h>
#include <unistd.h>
#include <glog/logging.h>
#include "worker.h"

const std::string gstrProgramVersion = "1.0.0.0";
const std::string gstrProgramName = "rdt";

static rdt::binlog_worker worker;
// CDebugTrace* goDebugTrace = NULL;

struct stru_argv
{
    std::string config_name;
    std::string log_path;
    int ppid;
};

void help_info(void)
{
    std::cout << "Real-time Data Transfer Server(RDT) Help Infomation:" << std::endl;
    std::cout << "./rdt -f configure_file_name(run application)" << std::endl;
    std::cout << "./rdt -v(display server version info)" << std::endl;
    std::cout << "./rdt -h(display help infomation)" << std::endl;
}

void si_handler(int ai_sig)
{
    if (SIGPIPE == ai_sig)
        std::cout << "pid: " << getpid() << " SIGPIPE !" << std::endl;
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    stru_argv arg;

    if (argc <= 1)
    {
        help_info();
        return 0;
    }

    for (int i = 1; i < argc; i = i + 2)
    {
        if (strcmp(argv[i], "-v") == 0)
        {
            std::cout << "Real-time Data Transfer Server(RDT) Version Infomation:" << std::endl;
            std::cout << "RDT Version: " << gstrProgramVersion.c_str() << " " << __DATE__ << " " << __TIME__ << std::endl;
            return 0;
        }
        else if (strcmp(argv[i], "-h") == 0)
        {
            help_info();
            return 0;
        }
        else if (strcmp(argv[i], "-f") == 0)
        {
            if (argc < i + 1)
            {
                std::cout << "argc error." << std::endl;
                exit(0);
            }

            arg.config_name = argv[i + 1];
            std::cout << arg.config_name << std::endl;

            if (arg.config_name.empty())
            {
                help_info();
                return 0;
            }
        }
    }
    /*
    goDebugTrace = new CDebugTrace;
    uint32_t process_id = CCommon::GetProcessId();
    char lszLogFileName[255];
    memset(lszLogFileName, 0, 255);

    if(arg.log_path.empty()) {
        CCommon::GetAppPath(lszLogFileName, 255);
        std::string lstrAppPath = lszLogFileName;
        std::string lstrApp = lstrAppPath.substr(0, lstrAppPath.find_last_of('/'));

        if(chdir(lstrApp.c_str())) {
            std::cout << gstrProgramName << " error: chdir error." << lszLogFileName << std::endl;
            return 0;
        }

        strcpy(strrchr(lszLogFileName, '/'), "//log");
        CCommon::CreatePath(lszLogFileName);
    } else {
        memcpy(lszLogFileName, arg.log_path.c_str(), arg.log_path.length());
    }

    char lszFileDate[50];
    memset(lszFileDate, 0, 50);
    sprintf(lszFileDate, "//%s-%u", gstrProgramName.c_str(), process_id);
    strcat(lszLogFileName, lszFileDate);
    SET_LOG_FILENAME(lszLogFileName);
    SET_TRACE_LEVEL(5);

    TRACE(3, "\n\n*******************" << gstrProgramName << " version:" <<
          gstrProgramVersion.c_str() << "*******************");
    TRACE(3, "configure file name : " << arg.config_name.c_str());
    */
    struct sigaction sig;
    memset(&sig, 0, sizeof(struct sigaction));
    sig.sa_handler = si_handler;
    int ret = sigemptyset(&sig.sa_mask);

    if (ret != 0)
    {
        LOG_FATAL("sigemptyset error. errno: %d", errno);
        return 0;
    }

    ret = sigaction(SIGPIPE, &sig, NULL);

    if (ret != 0)
    {
        LOG_FATAL("set sigpipe error. errno: %d", errno);
        return 0;
    }

    ret = 0;
    ret = worker.init(arg.config_name);

    if (ret < 0)
    {
        LOG_FATAL("worker init failed. ");
        return 0;
    }

    ret = worker.run();

    if (ret < 0)
    {
        LOG_FATAL("worker run failed. ");
        return 0;
    }

    return 0;
}
