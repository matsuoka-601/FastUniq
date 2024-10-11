#pragma once
#include <string>
#include <sys/mman.h>  
#include <sys/stat.h>
#include <fcntl.h>     
#include <unistd.h>    
#include <omp.h>
#include <vector>
#include <mutex>
#include <shared_mutex> 
#include <string.h>
#include <immintrin.h>
#include <cassert>
#include <iostream>

namespace FastUniq {
    using u32 = uint32_t;
    using u64 = uint64_t;
    using u8x32 = __m256i;
    using u8x16 = __m128i;
    constexpr u32 BATCHSIZE = 500;
    constexpr u32 PREFETCH_STRIDE = 16;

    class HashTable {
        static constexpr float LOAD_FACTOR = 0.5;
        static constexpr u32 INIT_CAPACITY = 64;
        u64 EMPTY = 0xffffffffffffffff;
        u32 capacity;
        u32 size;
        u64 *data;

        inline u32 CalcSlotIdx(u64 hash) {
            return (hash >> 32) % capacity;
        }

        inline bool InsertImpl(u64 hash) {
            u32 i = CalcSlotIdx(hash);
            for (; ; i = (i + 1) % capacity) {
                if (data[i] == EMPTY) { 
                    data[i] = hash;
                    return true;
                } else if (data[i] == hash) {
                    return false;
                }
            }
        }

        void resize() {
            u64* oldData = data;
            data = (u64*) malloc(2 * capacity * sizeof(u64));
            for (u32 i = 0; i < 2 * capacity; i++) {
                data[i] = EMPTY;
            }
            capacity *= 2;

            for (u32 i = 0; i < capacity / 2; i++) {
                if (oldData[i] != EMPTY) {
                    InsertImpl(oldData[i]);
                }
            }

            free(oldData);
        }
    public:
        HashTable() {
            data = (u64*)malloc(INIT_CAPACITY * sizeof(u64));
            capacity = INIT_CAPACITY;
            size = 0;
            for (u64 i = 0; i < capacity; i++)
                data[i] = EMPTY;
        }

        ~HashTable() {
            free(data); 
        }

        bool Find(u64 hash) {
            u32 i = CalcSlotIdx(hash);
            for (; ; i = (i + 1) % capacity) {
                if (data[i] == EMPTY) { 
                    return false;
                } else if (data[i] == hash) {
                    return true;
                }
            }
        }

        bool Insert(u64 hash) {
            while (size > capacity * LOAD_FACTOR) {
                resize();
            }

            bool insertResult = InsertImpl(hash);
            size += insertResult;
            return insertResult;
        }

        u32 Size() {
            return size;
        }

        inline void Prefetch(u64 hash) {
            u32 i = CalcSlotIdx(hash);
            __builtin_prefetch(data + i);
        }
    };

    class ParallelHashTable {
        struct Bucket {
            std::shared_mutex mtx;
            HashTable table;

            Bucket() : table() {} 

            Bucket(Bucket&& other) noexcept : table(std::move(other.table)) {}
        };
        std::vector<Bucket> buckets;

        static constexpr u32 BUCKETS_THREADS_FACTOR = 64;

        inline u32 CalcBucketIdx(u64 hash) {
            return (hash & ((1LL << 32) - 1)) % buckets.size();
        }
    public:
        ParallelHashTable(u32 num_threads) {
            buckets.resize(num_threads * BUCKETS_THREADS_FACTOR);
        }

        bool Insert(u64 hash) {
            u32 bucketIdx = CalcBucketIdx(hash);
            Bucket &bucket = buckets[bucketIdx];

            std::shared_lock<std::shared_mutex> readLock(bucket.mtx);
            if (bucket.table.Find(hash)) {
                return false;
            }
            readLock.unlock();
            std::unique_lock<std::shared_mutex> writeLock(bucket.mtx);
            return bucket.table.Insert(hash);
        }

        void ShowBucketsSize() {
            for (auto &bucket: buckets) {
                std::cerr << bucket.table.Size() << "\n";
            }
        }

        inline void Prefetch(u64 hash) {
            u32 bucketIdx = CalcBucketIdx(hash);
            Bucket &bucket = buckets[bucketIdx];
            bucket.table.Prefetch(hash);
        }

        u32 Size() {
            u32 ret = 0;
            for (auto &bucket: buckets) {
                ret += bucket.table.Size();
            }
            return ret;
        }
    };

    const u8x16 key = _mm_set_epi64x(884041218509897051, 464828032585196773);
    const u8x16 chunkMask[17] = {
        _mm_set_epi8(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00),
        _mm_set_epi8(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff),
        _mm_set_epi8(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff),
        _mm_set_epi8(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff),
        _mm_set_epi8(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff),
        _mm_set_epi8(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff),
        _mm_set_epi8(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
        _mm_set_epi8(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
        _mm_set_epi8(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
        _mm_set_epi8(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
        _mm_set_epi8(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
        _mm_set_epi8(0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
        _mm_set_epi8(0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
        _mm_set_epi8(0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
        _mm_set_epi8(0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
        _mm_set_epi8(0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
        _mm_set_epi8(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
    };
    

    void Hash(const char* input, u64 &hash, u32 &len) {
        const char* currentPtr = input;
        u8x32 newLines = _mm256_set1_epi8('\n');
        while (true) {
            const u8x32 chunk = _mm256_loadu_si256((u8x32*)currentPtr);
            const u8x32 cmpResult = _mm256_cmpeq_epi8(chunk, newLines);
            u32 mask = _mm256_movemask_epi8(cmpResult);
            if (mask == 0) {
                currentPtr += 32;
            } else { 
                currentPtr += __builtin_ctz(mask);
                break;
            }
        }

        len = currentPtr - input;
        u32 tmpLen = len;
        hash = 0;
        for (; tmpLen > 0; tmpLen -= std::min(tmpLen, 16u), input += std::min(tmpLen, 16u)) {
            u8x16 chunk = _mm_and_si128(_mm_loadu_si128((u8x16*)input), chunkMask[std::min(tmpLen, 16u)]);
            chunk = _mm_aesenc_si128(chunk, key);
            chunk = _mm_aesenc_si128(chunk, key);
            hash ^= chunk[0] ^ chunk[1];
        }
    }

    void ProcessChunk(
        ParallelHashTable &ht, 
        const char* inputChunk, 
        u32 chunkLen, 
        std::mutex &stdoutMutex
    ) {
        const char* currentPtr = inputChunk;

        u64 hashBuffer[BATCHSIZE];
        u32 lenBuffer[BATCHSIZE];
        const char* ptrBuffer[BATCHSIZE];

        char* threadBuf = (char*)malloc(1024);
        u32 bufCapacity = 1024;
        u32 currentBufBytes = 0;

        while (currentPtr - inputChunk < chunkLen) {
            // Batchfy hashing & inserting
            u32 i;
            for (i = 0; i < BATCHSIZE && currentPtr - inputChunk < chunkLen; i++) {
                Hash(currentPtr, hashBuffer[i], lenBuffer[i]);
                ptrBuffer[i] = currentPtr;
                currentPtr += lenBuffer[i] + 1;
            }

            u32 bufLen = i;
            for (i = 0; i < bufLen; i++) {
                if (i + PREFETCH_STRIDE < bufLen) ht.Prefetch(hashBuffer[i + PREFETCH_STRIDE]);
                if (ht.Insert(hashBuffer[i])) {
                    while (currentBufBytes + lenBuffer[i] + 1 >= bufCapacity) {
                        char* newThreadBuf = (char*)malloc(2 * bufCapacity);
                        memcpy(newThreadBuf, threadBuf, currentBufBytes);
                        free(threadBuf);
                        threadBuf = newThreadBuf;
                        bufCapacity *= 2;
                    }
                    memcpy(threadBuf + currentBufBytes, ptrBuffer[i], lenBuffer[i] + 1);
                    currentBufBytes += lenBuffer[i] + 1;
                }
            }
        }

        std::unique_lock<std::mutex> lock(stdoutMutex);
        write(STDOUT_FILENO, threadBuf, currentBufBytes);
        free(threadBuf);
    }

    const char* ClosestNewline(const char* input, const char* end) {
        while(input < end && *input != '\n') input++;
        return input;
    }

    // Divide the input equally
    std::vector<std::pair<const char*, u32>> DivideInput(
        const char* beg, const char* end, u32 threadNum
    ) {
        u32 fileSize = end - beg;
        u32 perChunkLen = fileSize / threadNum;

        std::vector<std::pair<const char*, u32>> ret;

        const char* prev = beg;
        u32 i = 0;
        for (; i < threadNum; i++) {
            if (i == threadNum - 1) {
                ret.push_back(std::make_pair(prev, end - prev));
            } else {
                const char* next = ClosestNewline(prev + perChunkLen, end);
                if (next == end) {
                    ret.push_back(std::make_pair(prev, end - prev));
                    break;
                } else {
                    ret.push_back(std::make_pair(prev, (next + 1) - prev));
                    prev = next + 1;
                }
            }
        }

        if (i != threadNum) {
            for (; i < threadNum; i++) {
                ret.push_back(std::make_pair((const char*)NULL, 0U)); // Just add an empty chunk
            }
        }

        return ret;
    }

    // Dedupliate newline separated strings in the input file
    // and write deduplicated strings to stdout.
    u32 Uniquify(const char *inputFile, u32 threadNum = 1) {
        // TODO : error handling
        int fd = open(inputFile, O_RDONLY);
        if (fd == -1) {
            perror("open");
            exit(1);
        }
        struct stat fileStat;
        fstat(fd, &fileStat);
        u32 fileSize = fileStat.st_size;

        if (fileSize == 0) {
            close(fd);
            return 0;
        }

        const char* input = (const char*)mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (input == MAP_FAILED) {
            perror("mmap");
            close(fd);
            exit(1);
        }

        ParallelHashTable ht(threadNum);

        std::mutex stdoutMutex;

        auto chunks = DivideInput(input, input + fileSize, threadNum);

        omp_set_num_threads(threadNum);
        #pragma omp parallel 
        {
            int thread_id = omp_get_thread_num();
            const char* beg = chunks[thread_id].first;
            u32 len = chunks[thread_id].second;
            if (len > 0) {
                ProcessChunk(ht, beg, len, stdoutMutex);
            }
        }

        munmap((void*)input, fileSize);
        close(fd);

        return ht.Size();
    }
}