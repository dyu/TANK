#include "tank_client.h"
#include <date.h>
#include <fcntl.h>
#include <network.h>
#include <set>
#include <sys/stat.h>
#include <sys/types.h>
#include <sysexits.h>
#include <text.h>
#include <unordered_map>
#include <iostream>

static void usage(int argc, char *argv[])
{
    std::cerr << "Usage:\n"
        << argv[0]
        << " (ip:)port topic seq [ flags ]\n" << std::endl;
}

static uint64_t parse_timestamp(strwlen32_t s)
{
    strwlen32_t c;
    struct tm tm;

    // for now, YYYYMMDDHH:MM:SS
    // Eventually, will support more date/timeformats
    if (s.len != 16)
        return 0;

    c = s.Prefix(4);
    s.StripPrefix(4);
    tm.tm_year = c.AsUint32() - 1900;
    c = s.Prefix(2);
    s.StripPrefix(2);
    tm.tm_mon = c.AsUint32() - 1;
    c = s.Prefix(2);
    s.StripPrefix(2);
    tm.tm_mday = c.AsUint32();

    c = s.Prefix(2);
    s.StripPrefix(2);

    s.StripPrefix(1);
    tm.tm_hour = c.AsUint32();
    c = s.Prefix(2);
    s.StripPrefix(2);
    tm.tm_min = c.AsUint32();

    s.StripPrefix(1);
    c = s.Prefix(2);
    s.StripPrefix(2);
    tm.tm_sec = c.AsUint32();

    tm.tm_isdst = -1;

    const auto res = mktime(&tm);
    
    return res == -1 ? 0 : Timings::Seconds::ToMillis(res);
}

/*static void parse_opts()
{
    while ((r = getopt(argc, argv, "+SF:hBT:KdE:s:")) != -1)
    {
        switch (r)
        {
				    case 'E':
					      endSeqNum = strwlen32_t(optarg).AsUint64();
					      break;
            
				    case 'd':
					      drainAndExit = true;
					      break;
            
				    case 'K':
					      asKV = true;
					      break;
            
				    case 's':
					      defaultMinFetchSize = strwlen32_t(optarg).AsUint64();
					      if (!defaultMinFetchSize)
					      {
						        Print("Invalid fetch size value\n");
						        return 1;
					      }
					      break;
            
            case 'T':
            {
                const auto r = strwlen32_t(optarg).Divided(',');

                timeRange.offset = parse_timestamp(r.first);
                //Print(Date::ts_repr(timeRange.offset / 1000), "\n");
                if (!timeRange.offset)
                {
                    Print("Failed to parse ", r.first, "\n");
                    return 1;
                }

                if (r.second)
                {
                    const auto end = parse_timestamp(r.second);

                    if (!end)
                    {
                        Print("Failed to parse ", r.second, "\n");
                        return 1;
                    }
                    
                    timeRange.SetEnd(end + 1);
                }
                else
                {
                    timeRange.len = UINT64_MAX - timeRange.offset;
                }
                break;
            }
            
            case 'S':
                statsOnly = true;
                break;
            
            case 'F':
                displayFields = 0;
                for (const auto it : strwlen32_t(optarg).Split(','))
                {
                    if (it.Eq(_S("seqnum")))
                            displayFields |= 1u << uint8_t(Fields::SeqNum);
                    else if (it.Eq(_S("key")))
                            displayFields |= 1u << uint8_t(Fields::Key);
                    else if (it.Eq(_S("content")))
                            displayFields |= 1u << uint8_t(Fields::Content);
                    else if (it.Eq(_S("ts")))
                            displayFields |= 1u << uint8_t(Fields::TS);
                    else if (it.Eq(_S("size")))
                            displayFields |= 1u << uint8_t(Fields::Size);
                    else
                    {
                            Print("Unknown field '", it, "'\n");
                            return 1;
                    }
                }
                break;
            
            case 'h':
                Print("CONSUME [options] from\n");
Print("Consumes/retrieves messages from Tank\n");
                Print("Options include:\n");
                Print("-F display format: Specify a ',' separated list of message properties to be displayed. Properties include: \"seqnum\", \"key\", \"content\", \"ts\". By default, only the content is displayed\n");
                Print("-S: statistics only\n");
Print("-E seqNum: Stop at sequence number specified\n");
Print("-d: drain and exit. As soon as all available messages have been consumed, exit (i.e do not tail)\n");
                Print("-T: optionally, filter all consumes messages by specifying a time range in either (from,to) or (from) format, where the first allows to specify a start and an end date/time and the later a start time and no end time. Currently, only one date-time format is supported (YYYMMDDHH:MM:SS)\n");
                Print("\"from\" specifies the first message we are interested in.\n");
                Print("If from is \"beginning\" or \"start\","
                      " it will start consuming from the first available message in the selected topic. If it is \"eof\" or \"end\", it will "
                      "tail the topic for newly produced messages, otherwise it must be an absolute 64bit sequence number\n");
                return 0;

            default:
                return 1;
        }
    }

    argc -= optind;
    argv += optind;
    
    if (!argc)
    {
        Print("Expected sequence number to begin consuming from. Please see ", app, " consume -h\n");
        return 1;
    }
    
    const strwlen32_t from(argv[0]);

    if (from.EqNoCase(_S("beginning")) || from.Eq(_S("first")))
    {
        next = 0;
    }
    else if (from.EqNoCase(_S("end")) || from.EqNoCase(_S("eof")))
    {
        next = UINT64_MAX;
    }
    else if (!from.IsDigits())
    {
        Print("Expected either \"beginning\", \"end\" or a sequence number for -f option\n");
        return 1;
    }
    else
    {
        next = from.AsUint64();
    }
}*/

enum class Fields : uint8_t
{
    SeqNum = 0,
    Key,
    Content,
    TS,
    Size
};

const uint8_t F_VERBOSE = 1;
const uint8_t F_RETRY = 2;
const uint8_t F_STATS_ONLY = 4;
const uint8_t F_AS_KV = 8;
const uint8_t F_DRAIN_AND_EXIT = 16;

int main(int argc, char *argv[])
{
    if (argc < 4)
    {
        usage(argc, argv);
        return 1;
    }
    
    Buffer endpoint, topic;
    uint16_t partition = 0;
    int r;
    TankClient tankClient;
    const char *const app = argv[0];
    int flags = argc > 4 ? std::atoi(argv[4]) : 0;
    bool verbose = 0 != (flags & F_VERBOSE);
    bool retry = 0 != (flags & F_RETRY);
    bool statsOnly = 0 != (flags & F_STATS_ONLY);
    bool asKV = 0 != (flags & F_AS_KV);
    bool drainAndExit = 0 != (flags & F_DRAIN_AND_EXIT);
    
    endpoint.append(argv[1]);
    topic.append(argv[2]);
    
    try
    {
        tankClient.set_default_leader(endpoint.AsS32());
    }
    catch (const std::exception &e)
    {
        std::cout << "connect error: " << e.what() << std::endl;
        return 1;
    }
    
    const TankClient::topic_partition topicPartition(topic.AsS8(), partition);
    const auto consider_fault = [](const TankClient::fault &f) {
        switch (f.type)
        {
            case TankClient::fault::Type::BoundaryCheck:
                std::cout << "Boundary Check fault. first available sequence number is " 
                    << f.ctx.firstAvailSeqNum << ", high watermark is " << f.ctx.highWaterMark << std::endl;
                break;

            case TankClient::fault::Type::UnknownTopic:
                std::cout << "Unknown topic '" << f.topic << "' error\n";
                break;

            case TankClient::fault::Type::UnknownPartition:
                std::cout << "Unknown partition of '" << f.topic << "' error\n";
                break;

            case TankClient::fault::Type::Access:
                std::cout << "Access error\n";
                break;

            case TankClient::fault::Type::SystemFail:
                std::cout << "System Error\n";
                break;

            case TankClient::fault::Type::InvalidReq:
                std::cout << "Invalid Request\n";
                break;

            case TankClient::fault::Type::Network:
                std::cout << "Network error\n";
                break;

            case TankClient::fault::Type::AlreadyExists:
                std::cout << "Already Exists\n";
                break;

            default:
                break;
        }
    };
    
    uint64_t next = std::atoi(argv[3]);
    uint8_t displayFields{1u << uint8_t(Fields::Content)};
    size_t defaultMinFetchSize{128 * 1024 * 1024};
    uint32_t pendingResp{0};
    IOBuffer buf;
    range64_t timeRange{0, UINT64_MAX};
		uint64_t endSeqNum{UINT64_MAX};
    
    //optind = 0;
    
		size_t totalMsgs{0}, sumBytes{0};
		const auto b = Timings::Microseconds::Tick();
    auto minFetchSize = defaultMinFetchSize;
    
    for (;;)
    {
        if (!pendingResp)
        {
            if (verbose)
                std:: cout << "Requesting from " << next << std::endl;
            
            // block for 5 seconds?
            pendingResp = tankClient.consume({{topicPartition, {next, minFetchSize}}}, drainAndExit ? 0 : 5000, 0);
            if (verbose)
                std::cout << "done consume" << std::endl;

            if (!pendingResp)
            {
                Print("Unable to issue consume request. Will abort\n");
                return 1;
            }
        }
        
        try
        {
            // every second
            tankClient.poll(1000);
            if (verbose)
                std::cout << "done poll" << std::endl;
        }
        catch (const std::exception &e)
        {
            std::cout << "poll error: " << e.what() << std::endl;
            continue;
        }
        
        for (const auto &it : tankClient.faults())
        {
            consider_fault(it);
            if (retry && it.type == TankClient::fault::Type::Network)
            {
                Timings::Milliseconds::Sleep(400);
                pendingResp = 0;
            }
            else
            {
                return 1;
            }
        }
        
        for (const auto &it : tankClient.consumed())
        {
				    if (drainAndExit && it.msgs.len == 0 && minFetchSize <= it.next.minFetchSize)
				    {
					      // Drained if we got no message in the response, and if the size we specified
					      // is <= next.minFetchSize. This is important because we could get no messages
					      // because the message is so large the minFetchSize we provided for the request was too low
					      goto out;
				    }
            
            if (statsOnly)
            {
                Print(it.msgs.len, " messages\n");
                totalMsgs += it.msgs.len;

                if (verbose)
                {
                    for (const auto m : it.msgs)
                    {
                        Print(m->seqNum, ": ", size_repr(m->content.size()), "\n");
                        sumBytes += m->content.len + m->key.len + sizeof(uint64_t);
                    }
                }
                else
                {
                    for (const auto m : it.msgs)
                        sumBytes += m->content.len + m->key.len + sizeof(uint64_t);
                }
            }
            else
            {
                size_t sum{0};

                for (const auto m : it.msgs)
                    sum += m->content.len;
                sum += it.msgs.len * 2;
                
                buf.clear();
                buf.reserve(sum);
                for (const auto m : it.msgs)
                {
                    if (m->seqNum > endSeqNum)
                        break;

                    if (timeRange.Contains(m->ts))
                    {
                        if (asKV)
                        {
                            buf.append(m->seqNum, " [", m->key, "] = [", m->content, "]");
                        }
                        else if (displayFields)
                        {
                            if (displayFields & (1u << uint8_t(Fields::TS)))
                                    buf.append(Date::ts_repr(Timings::Milliseconds::ToSeconds(m->ts)), ':');
                            if (displayFields & (1u << uint8_t(Fields::SeqNum)))
                                    buf.append("seq=", m->seqNum, ':');
                            if (displayFields & (1u << uint8_t(Fields::Size)))
                                    buf.append("size=", m->content.size(), ':');
                            if (displayFields & (1u << uint8_t(Fields::Content)))
                                    buf.append(m->content);
                        }
                        else
                            buf.append(m->content);
                        
                        buf.append('\n');
                    }
                }

                if (auto s = buf.AsS32())
                {
                    do
                    {
                        const auto r = write(STDOUT_FILENO, s.p, s.len);
                        
                        if (r == -1)
                        {
                            Print("(Failed to output data to stdout:", strerror(errno), ". Exiting\n");
                            return 1;
                        }
                        
                        s.StripPrefix(r);
                    }
                    while (s);
                }
            }

            minFetchSize = Max<size_t>(it.next.minFetchSize, defaultMinFetchSize);
            next = it.next.seqNum;
            pendingResp = 0;

				    if (next > endSeqNum)
					      exit(0);
        }
    }

out:
		if (statsOnly)
		{
		    Print(dotnotation_repr(totalMsgs), " messages consumed in ", 
		            duration_repr(Timings::Microseconds::Since(b)), ", ", 
		            size_repr(sumBytes), " bytes consumed\n");
		}
                  
    return 0;
}
