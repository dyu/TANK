#pragma once
#include "common.h"
#include <fs.h>
#include <network.h>
#include <switch.h>
#include <switch_dictionary.h>
#include <switch_ll.h>
#include <switch_refcnt.h>
#include <switch_vector.h>
#include <sys/mman.h>
#include <sys/sendfile.h>
#include <thread.h>

// The index is sparse, so we need to be able to locate the _last_ index entry for relative sequence number <=
// targetRelativeSeqNumber
// so that we can either
// 1. read from the log file starting from there onwards, until we find the first message
// /w sequence number >= target sequence number, or
// 2. we just stream rom that previous message and expect the clients to skip messages themselves
namespace std
{
// Returns an iterator to the _last_ element in [first, last) that is <= `value`
// You should probably return TrivialCmp(comp, value);
template <class ForwardIt, class T, class Compare>
static ForwardIt upper_bound_or_match(ForwardIt first, ForwardIt last, const T &value, const Compare comp)
{
    ForwardIt res = last;

    for (int32_t top = (last - first) - 1, btm{ 0 }; btm <= top;)
    {
        const auto mid = (btm + top) / 2;
        const auto p = first + mid;
        const auto r = comp(*p, value);

        if (!r)
            return p;
        else if (r > 0)
        {
            res = p;
            btm = mid + 1;
        }
        else
            top = mid - 1;
    }

    return res;
}
}

struct index_record
{
    uint32_t relSeqNum;
    uint32_t absPhysical;
};

struct ro_segment_lookup_res
{
    index_record record;
    uint32_t span;
};

struct fd_handle : public RefCounted<fd_handle>
{
    int fd;

    fd_handle(int f) : fd{ f }
    {
    }

    ~fd_handle()
    {
        if (fd != -1)
        {
            fdatasync(fd);
            close(fd);
        }
    }
};

// A read-only (immutable, frozen-sealed) partition commit log(Segment) (and the index file for quick lookups)
// we don't need to acquire a lock to access this
//
// we need to ref-count ro_segments so that we can hand off the list of current ro segments to another thread for
// compaction, so that
// while the compaction is in progess, we won't delete any of those segments passed to the thread (i.e in
// consider_ro_segments() )
// For now, we can return immediately from consider_ro_segments() if compaction is scheduled for the partition, and
// later we can
// do this properly.
struct ro_segment
{
    // the absolute sequence number of the first message in this segment
    const uint64_t baseSeqNum;
    // the absolute sequence number of the last message in this segment
    // i.e this segment contains [baseSeqNum, lastAssignedSeqNum]
    // See: https://github.com/phaistos-networks/TANK/issues/2 for rationale
    const uint64_t lastAvailSeqNum;

    // For RO segments, this used to be set to the creation time of the
    // mutable segment that was then turned into a R/O segment.
    //
    // This however turned out to be problematic, because retention logic would
    // consider that timestamp for retentions, instead of what makes more sense, the time when
    // the last message was appended to the mutable segment, before it was frozen as a RO segment.
    //
    // We need to encode this in the file path, because a process may update the mtime of the RO segment for whatever
    // reason
    // and so it's important that we do not depend on the file's mtime, and instead encode it in the path.
    // see: https://github.com/phaistos-networks/TANK/issues/37
    const uint32_t createdTS;

    Switch::shared_refptr<fd_handle> fdh;
    uint32_t fileSize;

    // In order to support compactions (in the future), in the very improbable and unlikely case compaction leads
    // to situations where because of deduplication we will end up having to store messages in a segment where any of
    // those
    // message.absSeqNum - segment.baseSeqNum > UINT32_MAX, and we don't want to just create a new immutable segment to
    // deal with it(maybe because
    // that'd lead to creating very small segments), then we encode {absSeqNum:u64, fileOffet:u32} instead in the
    // immutable segment's index and
    // to set that that, we 'll use a different name for those .index files.
    //
    // For now, the implementation is missing, but we 'll implement what's required if and when we need to do so.
    const bool haveWideEntries;

    // Every log file is associated with this skip-list index
    struct
    {
        const uint8_t *data;
        uint32_t fileSize;

        // last record in the index
        index_record lastRecorded;
    } index;

    ro_segment(const uint64_t absSeqNum, const uint64_t lastAbsSeqNum, const uint32_t creationTS)
        : baseSeqNum{ absSeqNum }, lastAvailSeqNum{ lastAbsSeqNum }, createdTS{ creationTS }, haveWideEntries{ false }
    {
    }

    ro_segment(const uint64_t absSeqNum, const uint64_t lastAbsSeqNum, const strwlen32_t base, const uint32_t,
               const bool haveWideEntries);

    ~ro_segment()
    {
        if (index.data && index.data != MAP_FAILED)
            munmap((void *)index.data, index.fileSize);
    }

    // Locate the (relative seq.num, abs.file offset) for the LAST index record where record.relSqNum <= targetRelSeqNum
    std::pair<uint32_t, uint32_t> snapDown(const uint64_t absSeqNum) const;

    // Locate the (relative seq.num, abs.file offset) for the LAST index record where record.relSqNum >= targetRelSeqNum
    std::pair<uint32_t, uint32_t> snapUp(const uint64_t absSeqNum) const;

    ro_segment_lookup_res translateDown(const uint64_t absSeqNum, const uint32_t max) const
    {
        const auto res = snapDown(absSeqNum);

        return { { res.first, res.second }, fileSize - res.second };
    }
};

struct append_res
{
    Switch::shared_refptr<fd_handle> fdh;
    range32_t dataRange;
    range_base<uint64_t, uint16_t> msgSeqNumRange;
};

struct lookup_res
{
    enum class Fault : uint8_t
    {
        NoFault = 0,
        Empty,
        BoundaryCheck,
        AtEOF
    } fault;

    // This is set to either fileSize of the segment log file or lower if
    // we are are setting a boundary based on last assigned committed sequence number phys.offset
    // adjust_range() cannot exceed that offset
    uint32_t fileOffsetCeiling;
    Switch::shared_refptr<fd_handle> fdh;

    // Absolute base sequence number of the first message in the first bundle
    // of all bundles in the log chunk in range.
    // Incremenent this by each bundle.header.msgSetMsgsCnt to compute the absolute sequence number
    // of each bundle message (use post-increment!)
    uint64_t absBaseSeqNum;

    // file offset for the bundle with the first message == absBaseSeqNum
    uint32_t fileOffset;

    // The last committed absolute sequence number
    uint64_t highWatermark;

    lookup_res(lookup_res &&o)
        : fault{ o.fault }
        , fileOffsetCeiling{ o.fileOffsetCeiling }
        , fdh(std::move(o.fdh))
        , absBaseSeqNum{ o.absBaseSeqNum }
        , fileOffset{ o.fileOffset }
        , highWatermark{ o.highWatermark }
    {
    }

    lookup_res(const lookup_res &) = delete;

    lookup_res() : fault{ Fault::NoFault }
    {
    }

    lookup_res(fd_handle *const f, const uint32_t c, const uint64_t seqNum, const uint32_t o, const uint64_t h)
        : fault{ Fault::NoFault }
        , fileOffsetCeiling{ c }
        , fdh{ f }
        , absBaseSeqNum{ seqNum }
        , fileOffset{ o }
        , highWatermark{ h }
    {
    }

    lookup_res(const Fault f, const uint64_t h) : fault{ f }, highWatermark{ h }
    {
    }
};

enum class CleanupPolicy : uint8_t
{
    DELETE = 0,
    CLEANUP
};

struct partition_config
{
    // Kafka defaults
    size_t roSegmentsCnt{ 0 };
    uint64_t roSegmentsSize{ 0 };
    uint64_t maxSegmentSize{ 1 * 1024 * 1024 * 1024 };
    size_t indexInterval{ 4096 };
    size_t maxIndexSize{ 10 * 1024 * 1024 };
    size_t maxRollJitterSecs{ 0 };
    size_t lastSegmentMaxAge{ 0 };  // Kafka's default is 1 week, we don't want to explicitly specify a retention limit
    size_t curSegmentMaxAge{ 86400 * 7 };  // 1 week (soft limit)
    size_t flushIntervalMsgs{ 0 };         // never
    size_t flushIntervalSecs{ 0 };         // never
    CleanupPolicy logCleanupPolicy{ CleanupPolicy::DELETE };
    float logCleanRatioMin{ 0.5 };
} config;

static void PrintImpl(Buffer &out, const lookup_res &res)
{
    if (res.fault != lookup_res::Fault::NoFault)
        out.append("{fd = ", res.fdh ? res.fdh->fd : -1, ", absBaseSeqNum = ", res.absBaseSeqNum, ", fileOffset = ",
                   res.fileOffset, "}");
    else
        out.append("{fault = ", unsigned(res.fault), "}");
}

// An append-only log for storing bundles, divided into segments
struct topic_partition;
struct topic_partition_log
{
    // The first available absolute sequence number across all segments
    uint64_t firstAvailableSeqNum;

    // This will be initialized from the latest segment in initPartition()
    uint64_t lastAssignedSeqNum{ 0 };

    topic_partition *partition;
    bool compacting{ false };

    // Whenever we cleanup, we update lastCleanupMaxSeqNum with the lastAvailSeqNum of the latest ro segment compacted
    uint64_t lastCleanupMaxSeqNum{ 0 };

    auto first_dirty_offset() const
    {
        return lastCleanupMaxSeqNum + 1;
    }

    struct
    {
        Switch::shared_refptr<fd_handle> fdh;

        // The absolute sequence number of the first message in the current segment
        uint64_t baseSeqNum;
        uint32_t fileSize;

        // We are going to be updating the index and skiplist frequently
        // This is always initialized to UINT32_MAX, so that we always index the first bundle in the segment, for
        // impl.simplicity
        uint32_t sinceLastUpdate;

        // Computed whenever we roll/create a new mutable segment
        uint32_t rollJitterSecs;

        // When this was created, in seconds
        uint32_t createdTS;
        bool nameEncodesTS;

        struct
        {
            int fd;

            // relative sequence number => file physical offset
            // relative sequence number = absSeqNum - baseSeqNum
            Switch::vector<std::pair<uint32_t, uint32_t>> skipList;

            // see above
            bool haveWideEntries;

            // We may access the index both directly, if ondisk is valid, and via the skipList
            // This is so that we won't have to restore the skiplist on init
            struct
            {
                const uint8_t *data;
                uint32_t span;

                // last recorded tuple in the index; we need this here
                struct
                {
                    uint32_t relSeqNum;
                    uint32_t absPhysical;
                } lastRecorded;

            } ondisk;

        } index;

        struct
        {
            uint64_t pendingFlushMsgs{ 0 };
            uint32_t nextFlushTS;
        } flush_state;

    } cur;  // the _current_ (latest) segment

    partition_config config;

    // a topic partition is comprised of a set of segments(log file, index file) which
    // are immutable, and we don't need to serialize access to them, and a cur(rent) segment, which is not immutable.
    //
    // roSegments can also be atomically exchanged with a new vector, so we don't need to protect that either
    // make sure roSegments is sorted
    std::shared_ptr<std::vector<ro_segment *>> roSegments;

    ~topic_partition_log()
    {
        if (roSegments)
        {
            for (ro_segment *it : *roSegments)
                delete it;
        }

        if (cur.index.fd != -1)
        {
            fdatasync(cur.index.fd);
            close(cur.index.fd);
        }
    }

    lookup_res read_cur(const uint64_t absSeqNum, const uint32_t maxSize, const uint64_t maxAbsSeqNum);

    lookup_res range_for(uint64_t absSeqNum, const uint32_t maxSize, uint64_t maxAbsSeqNum);

    append_res append_bundle(const time_t, const void *bundle, const size_t bundleSize, const uint32_t bundleMsgsCnt,
                             const uint64_t, const uint64_t);

    bool should_roll(const uint32_t) const;

    bool may_switch_index_wide(const uint64_t);

    void schedule_flush(const uint32_t);

    void consider_ro_segments();

    void compact(const char *);
};

struct connection;
struct topic_partition;

// In order to support minBytes semantics, we will
// need to track produced data for each tracked topic partition, so that
// we will be able to flush once we can satisfy the minBytes semantics
struct wait_ctx_partition
{
    fd_handle *fdh;
    uint64_t seqNum;
    range32_t range;
    topic_partition *partition;
};

struct wait_ctx
{
    connection *c;
    bool scheduledForDtor;
    uint32_t requestId;
    switch_dlist list, expList;
    uint64_t expiration;  // in MS
    uint32_t minBytes;

    // A request may involve multiple partitions
    // minBytes applies to the sum of all captured content for all specified partitions
    uint32_t capturedSize;

    uint8_t partitionsCnt;
    wait_ctx_partition partitions[0];
};

struct topic;
struct topic_partition : public RefCounted<topic_partition>
{
    uint16_t idx;  // (0, ...)
    uint32_t distinctId;
    topic *owner{ nullptr };
    uint16_t localBrokerId;  // for convenience
    partition_config config;

    struct replica : public RefCounted<replica>
    {
        const uint16_t brokerId;  // owner
        uint64_t time;
        uint64_t initialHighWaterMark;

        // The high watermark sequence number; maintained for followers replicas
        uint64_t highWatermMark;

        // Tracked across all replicas
        // for the local replica, it's the partition's lastAssignedSeqNum, for remote replicas, this is updated by
        // followers fetch reqs
        uint64_t lastAssignedSeqNum;

        replica(const uint16_t id) : brokerId{ id }
        {
        }
    };

    struct
    {
        uint16_t brokerId;
        uint64_t epoch;
        Switch::shared_refptr<struct replica> replica;
    } leader;

    // this node may or may not be a replica for this partition
    std::unique_ptr<topic_partition_log> log_;

#ifndef LEAN_SWITCH
    Switch::unordered_map<uint16_t, replica *, ReleaseRefDestructor> replicasMap;
#else
    Switch::unordered_map<uint16_t, replica *> replicasMap;
#endif

    Switch::vector<replica *> insyncReplicas;

    auto highwater_mark() const
    {
        return log_->lastAssignedSeqNum;
    }

    Switch::vector<wait_ctx *> waitingList;

    Switch::shared_refptr<replica> replicaByBrokerId(const uint16_t brokerId)
    {
        return replicasMap[brokerId];
    }

    void consider_append_res(append_res &res, Switch::vector<wait_ctx *> &waitCtxWorkL);

    append_res append_bundle_to_leader(const time_t, const uint8_t *const bundle, const size_t bundleLen,
                                       const uint32_t bundleMsgsCnt, Switch::vector<wait_ctx *> &waitCtxWorkL,
                                       const uint64_t, const uint64_t);

    lookup_res read_from_local(const bool fetchOnlyFromLeader, const bool fetchOnlyComittted, const uint64_t absSeqNum,
                               const uint32_t fetchSize);
};

struct topic : public RefCounted<topic>
{
    const strwlen8_t name_;
    Switch::vector<topic_partition *> *partitions_;
    partition_config partitionConf;

    topic(const strwlen8_t name, const partition_config c) : name_{ name.Copy(), name.len }, partitionConf{ c }
    {
        partitions_ = new Switch::vector<topic_partition *>();
    }

    auto name() const
    {
        return name_;
    }

    ~topic()
    {
        if (auto *const p = const_cast<char *>(name_.p))
            free(p);

        if (partitions_)
        {
            while (partitions_->size())
                partitions_->Pop()->Release();

            delete partitions_;
        }
    }

    void register_partition(topic_partition *const p)
    {
        p->owner = this;
        partitions_->push_back(p);

        std::sort(partitions_->begin(), partitions_->end(), [](const auto a, const auto b)
                  {
                      return a->idx < b->idx;
                  });

        require(partitions_->back()->idx == partitions_->size() - 1);
    }

    void register_partitions(topic_partition **const all, const size_t n)
    {
        for (uint32_t i{ 0 }; i != n; ++i)
        {
            require(all[i]);
            all[i]->owner = this;
            partitions_->push_back(all[i]);
        }

        std::sort(partitions_->begin(), partitions_->end(), [](const auto a, const auto b)
                  {
                      return a->idx < b->idx;
                  });

        require(partitions_->back()->idx == partitions_->size() - 1);
    }

    const topic_partition *partition(const uint16_t idx) const
    {
        return idx < partitions_->size() ? partitions_->at(idx) : nullptr;
    }

    topic_partition *partition(const uint16_t idx)
    {
        return idx < partitions_->size() ? partitions_->at(idx) : nullptr;
    }
};

// We could have used Switch::deque<> but let's just use something simpler
// TODO: Maybe we should just use a linked list instead
struct outgoing_queue
{
    struct content_file_range
    {
        fd_handle *fdh;
        range32_t range;

        content_file_range &operator=(const content_file_range &o)
        {
            fdh = o.fdh;
            fdh->Retain();
            range = o.range;
            return *this;
        }

        content_file_range &operator=(content_file_range &&o)
        {
            fdh = o.fdh;
            range = o.range;

            o.fdh = nullptr;
            o.range.reset();
            return *this;
        }
    };

    struct payload
    {
        bool payloadBuf;

        union
        {
            struct
            {
                IOBuffer *buf;

                struct
                {
                    struct iovec iov[64];  // https://github.com/phaistos-networks/TANK/issues/12
                    uint8_t iovIdx;
                    uint8_t iovCnt;
                };
            };

            content_file_range file_range;
        };

        void set_iov(const range32_t *const l, const uint32_t cnt)
        {
            iovCnt = cnt;

            for (uint32_t i{ 0 }; i != cnt; ++i)
            {
                auto ptr = iov + i;

                ptr->iov_base = static_cast<void *>(buf->At(l[i].offset));
                ptr->iov_len = l[i].len;
            }
        }

        payload() : payloadBuf{ false }
        {
        }

        payload(IOBuffer *const b)
        {
            payloadBuf = true;
            buf = b;
            iovCnt = iovIdx = 0;
        }

        payload(fd_handle *const fdh, const range32_t r)
        {
            payloadBuf = false;
            file_range.fdh = fdh;
            file_range.range = r;
            file_range.fdh->Retain();
        }

        payload &operator=(const payload &) = delete;

        payload &operator=(payload &&o)
        {
            payloadBuf = o.payloadBuf;

            if (payloadBuf)
            {
                buf = o.buf;
                iovCnt = o.iovCnt;
                iovIdx = o.iovIdx;
                memcpy(iov, o.iov, iovCnt * sizeof(struct iovec));

                o.buf = nullptr;
                o.iovCnt = o.iovIdx = 0;
            }
            else
            {
                file_range = std::move(o.file_range);
            }

            o.payloadBuf = false;
            return *this;
        }
    };

    using reference = payload &;
    using reference_const = const payload &;

    payload A[128];  // fixed size
    uint8_t backIdx{ 0 }, frontIdx{ 0 }, size_{ 0 };
    static constexpr size_t capacity{ sizeof_array(A) };

    inline uint32_t prev(const uint32_t idx) const noexcept
    {
        return (idx + (capacity - 1)) & (capacity - 1);
    }

    inline uint32_t next(const uint32_t idx) const noexcept
    {
        return (idx + 1) & (capacity - 1);
    }

    inline reference front() noexcept
    {
        return A[frontIdx];
    }

    inline reference_const front() const noexcept
    {
        return A[frontIdx];
    }

    inline reference back() noexcept
    {
        return A[prev(backIdx)];
    }

    inline reference_const back() const noexcept
    {
        return A[prev(backIdx)];
    }

    inline reference at(const size_t idx) noexcept
    {
        return A[(frontIdx + idx) & (capacity - 1)];
    }

    inline reference_const at(const size_t idx) const noexcept
    {
        return A[(frontIdx + idx) & (capacity - 1)];
    }

    inline void did_push() noexcept
    {
        ++size_;
    }

    inline void did_pop() noexcept
    {
        --size_;
    }

    payload &push_back(const payload &v) = delete;

    auto push_back(payload &&v)
    {
        if (unlikely(size_ == capacity))
            throw Switch::system_error("Queue full");

        payload *const res = A + backIdx;

        *res = std::move(v);
        backIdx = next(backIdx);
        did_push();

        return res;
    }

    void push_front(const payload &v) = delete;

    void push_front(payload &&v)
    {
        if (unlikely(size_ == capacity))
            throw Switch::system_error("Queue full");

        frontIdx = prev(frontIdx);
        A[frontIdx] = std::move(v);
        did_push();
    }

    inline void pop_back() noexcept
    {
        backIdx = prev(backIdx);
        did_pop();
    }

    inline void pop_front() noexcept
    {
        frontIdx = next(frontIdx);
        did_pop();
    }

    inline bool full() const noexcept
    {
        return size_ == capacity;
    }

    inline bool empty() const noexcept
    {
        return !size_;
    }

    inline auto size() const noexcept
    {
        return size_;
    }

    template <typename L>
    void clear(L &&l)
    {
        while (frontIdx != backIdx)
        {
            auto &p = A[frontIdx];

            if (p.payloadBuf)
                l(p.buf);
            else
                p.file_range.fdh->Release();

            frontIdx = next(frontIdx);
        }

        size_ = 0;
        frontIdx = backIdx = 0;
    }

    outgoing_queue()
    {
    }
};

struct connection
{
    int fd;
    IOBuffer *inB{ nullptr };

    // May have an attached outgoing queue
    outgoing_queue *outQ{ nullptr };

    switch_dlist connectionsList, waitCtxList;

    // This is initialized to(0) for clients, but followers are expected to
    // issue an REPLICA_ID request as soon as they connect
    uint16_t replicaId;

    struct State
    {
        enum class Flags : uint8_t
        {
            PendingIntro = 0,
            NeedOutAvail,
            ConsideredReqHeader
        };

        uint8_t flags;
        uint64_t lastInputTS;
    } state;
};

class Service final
{
    friend struct ro_segment;

  private:
    enum class OperationMode : uint8_t
    {
        Standalone = 0,
        Clustered
    } opMode{ OperationMode::Standalone };
    Switch::vector<wait_ctx *> waitCtxPool[255];
#ifndef LEAN_SWITCH
    Switch::unordered_map<strwlen8_t, topic *, ReleaseRefDestructor> topics;
#else
    Switch::unordered_map<strwlen8_t, topic *> topics;
#endif
    uint16_t selfBrokerId{ 1 };
    Switch::vector<IOBuffer *> bufs;
    Switch::vector<connection *> connsPool;
    Switch::vector<outgoing_queue *> outgoingQueuesPool;
    switch_dlist allConnections, waitExpList;
    // In the past, it was possible, however improbably, that
    // we 'd invoke destroy_wait_ctx() twice on the same context, or would otherwise
    // use or re-use the same context while it was either invalid, or was reused
    // and that would obviously cause problems.
    // In ordet to eliminate that possibility, destroy_wait_ctx() now
    // defers put_waitctx() until the next I/O loop iteration
    // where it's safe to release and reuse that context
    //
    // We used to use a single expiredCtxList, but now using multiple just so that we
    // won't accidently clear/use one for multiple occasions. It was used in order to track
    // down a hasenbug, though it turned out to be a silly application bug, not a Tank bug.
    // It's nonethless great that we figured out this edge case(no evidence that this
    // has ever happened) and we are dealing with it here.
    Switch::vector<wait_ctx *> expiredCtxList, expiredCtxList2, expiredCtxList3, waitCtxDeferredGC;
    uint32_t nextDistinctPartitionId{ 0 };
    int listenFd;
    EPoller poller;
    Switch::vector<topic_partition *> deferList;
    range32_t patchList[1024];
    time_t curTime;

  private:
    static void parse_partition_config(const char *, partition_config *);

    static void parse_partition_config(const strwlen32_t, partition_config *);

    auto get_outgoing_queue()
    {
        return outgoingQueuesPool.size() ? outgoingQueuesPool.Pop() : new outgoing_queue();
    }

    topic *topic_by_name(const strwlen8_t) const;

    void put_outgoing_queue(outgoing_queue *const q)
    {
        q->clear([this](auto buf)
                 {
                     this->put_buffer(buf);
                 });

        outgoingQueuesPool.push_back(q);
    }

    auto get_buffer()
    {
        return bufs.size() ? bufs.Pop() : new IOBuffer();
    }

    void put_buffer(IOBuffer *b)
    {
        if (b->Reserved() > 800 * 1024 || bufs.size() > 16)
            delete b;
        else
        {
            b->clear();
            bufs.push_back(b);
        }
    }

    auto get_connection()
    {
        return connsPool.size() ? connsPool.Pop() : new connection();
    }

    void put_connection(connection *const c)
    {
        if (c->inB)
        {
            put_buffer(c->inB);
            c->inB = nullptr;
        }

        if (connsPool.size() > 16)
            delete c;
        else
        {
            connsPool.push_back(c);
        }
    }

    void register_topic(topic *const t)
    {
        if (!topics.Add(t->name(), t))
            throw Switch::exception("Topic ", t->name(), " already registered");
    }

    Switch::shared_refptr<topic_partition> init_local_partition(const uint16_t idx, const char *const bp,
                                                                const partition_config &);

    bool isValidBrokerId(const uint16_t replicaId)
    {
        return replicaId != 0;
    }

    uint32_t partitionLeader(const topic_partition *const p)
    {
        return 1;
    }

    const topic_partition *getPartition(const strwlen8_t topic, const uint16_t partitionIdx) const
    {
#ifdef LEAN_SWITCH
        const auto it = topics.find(topic);

        if (it != topics.end())
            return it->second->partition(partitionIdx);
#else
        if (const auto t = topics[topic])
            return t->partition(partitionIdx);
#endif

        return nullptr;
    }

    topic_partition *getPartition(const strwlen8_t topic, const uint16_t partitionIdx)
    {
        if (auto t = topics[topic])
            return t->partition(partitionIdx);

        return nullptr;
    }

    bool process_consume(connection *const c, const uint8_t *p, const size_t len);

    bool process_replica_reg(connection *const c, const uint8_t *p, const size_t len);

    bool process_discover_partitions(connection *const c, const uint8_t *p, const size_t len);

    bool process_create_topic(connection *const c, const uint8_t *p, const size_t len);

    wait_ctx *get_waitctx(const uint8_t totalPartitions)
    {
        if (waitCtxPool[totalPartitions].size())
            return waitCtxPool[totalPartitions].Pop();
        else
            return (wait_ctx *)malloc(sizeof(wait_ctx) + totalPartitions * sizeof(wait_ctx_partition));
    }

    void put_waitctx(wait_ctx *const ctx)
    {
        waitCtxPool[ctx->partitionsCnt].push_back(ctx);
    }

    bool register_consumer_wait(connection *const c, const uint32_t requestId, const uint64_t maxWait,
                                const uint32_t minBytes, topic_partition **const partitions,
                                const uint32_t totalPartitions);

    bool process_produce(const TankAPIMsgType, connection *const c, const uint8_t *p, const size_t len);

    bool process_msg(connection *const c, const uint8_t msg, const uint8_t *const data, const size_t len);

    void wakeup_wait_ctx(wait_ctx *const wctx, const append_res &appendRes, connection *);

    void abort_wait_ctx(wait_ctx *const wctx);

    void destroy_wait_ctx(wait_ctx *const wctx);

    void cleanup_connection(connection *);

    bool shutdown(connection *const c, const uint32_t ref);

    bool flush_iov(connection *, struct iovec *, const uint32_t);

    bool try_send_ifnot_blocked(connection *const c)
    {
        if (c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail)))
        {
            // Blocked
            return true;
        }
        else
            return try_send(c);
    }

    void poll_outavail(connection *);

    void introduce_self(connection *, bool &);

    bool try_send(connection *const c);

  protected:
    static void rebuild_index(int, int);

    static uint32_t verify_log(int);

    static void verify_index(int, const bool);

  public:
    Service()
    {
        switch_dlist_init(&allConnections);
        switch_dlist_init(&waitExpList);
    }

    ~Service();

    int start(int argc, char **argv);
};
