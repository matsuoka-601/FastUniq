#include "cmdline.h"
#include "FastUniq.hpp"
#include <random>
#include <climits>
#include <unordered_set>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <filesystem>
#include <cassert>
#include <omp.h>
#include <functional>

// Bench the scalability changing the number of threads

int main(int argc, char** argv) {
    cmdline::parser p;
    p.add<unsigned>("lines", 'l', "Number of lines", false, 30000000, cmdline::range(1, INT_MAX));
    p.add<unsigned>("max-length", 'm', "Maximum length of a string", false, 16, cmdline::range(1, INT_MAX));
    p.add<unsigned>("unique-strings", 'u', "Number of unique strings", false, 1000000, cmdline::range(1, INT_MAX));
    p.add("vector", 'v', "Use Uniquify function, which returns a vector of unique strings");
    p.add("help", 'h', "print help");

    if (!p.parse(argc, argv) || p.exist("help")) {
        std::cerr << p.error_full() << p.usage();
        return 1;
    }

    unsigned    l = p.get<unsigned>("lines");
    unsigned    m = p.get<unsigned>("max-length");
    unsigned    u = p.get<unsigned>("unique-strings");

    if (l < u) {
        std::cerr << "Error: Invalid input. The number of unique strings (-u) should be equal to or less than the number of lines (-l)\n";
        return 1;
    }

    std::cerr << "Generating input strings...\n";

    std::unordered_set<uint64_t> uniqueStringsHash;
    char **uniqueStrings = (char **)malloc(sizeof(char*) * u);
    unsigned *len = (unsigned*)malloc(sizeof(unsigned) * u);

    std::random_device seed;
    std::mt19937 rng(seed());
    for (unsigned i = 0; i < u; i++) {
        unsigned counter = 0;
        while (true) {
            unsigned length = rng() % m + 1;
            std::string s = "";
            for (unsigned j = 0; j < length; j++) {
                s.push_back(rng() % 26 + 'a');
            }
            if (uniqueStringsHash.find(std::hash<std::string>()(s)) != uniqueStringsHash.end()) {
                continue;
            } else {
                uniqueStringsHash.insert(std::hash<std::string>()(s));
                uniqueStrings[i] = (char*)malloc(length + 1);
                memcpy(uniqueStrings[i], s.data(), length);
                uniqueStrings[i][length] = '\0';
                len[i] = length;
                s.clear();
                break;
            }

            // If max-length is too short, it might be impossible to generate enough unique strings
            if (counter++ > 10000000) { 
                std::cerr << "Error: Max-length is too small. Set a larger value.";
                return 1;
            }
        }
    }

    char fileName[] = "/tmp/tempfileXXXXXX";
    int fd = mkstemp(fileName);
    if (fd == -1) {
        perror("mkstemp");
        return 1;
    }

    {
        std::ofstream tmpFile(fileName);
        for (unsigned i = 0; i < l; i++) {
            if (i < u) {
                std::string s(uniqueStrings[i], len[i]);
                tmpFile << s << "\n";
            } else {
                unsigned idx = rng() % u;
                std::string s(uniqueStrings[idx], len[idx]);
                tmpFile << s << "\n";
            }
        }
    }

    unsigned fileSize = std::filesystem::file_size(fileName);

    freopen("/dev/null", "w", stdout);

    constexpr unsigned BENCH_REPEAT = 10;
    for (unsigned threadNum = 1; threadNum <= omp_get_num_procs(); threadNum++) {
        std::cerr << threadNum << ((threadNum == 1) ? " thread : " : " threads : ");
        double runTimeSum = 0;
        // Take average of BENCH_REPEAT times
        if (p.exist("vector")) { 
            for (unsigned i = 0; i < BENCH_REPEAT; i++) {
                auto start = std::chrono::high_resolution_clock::now();
                std::vector<std::string> uniqueCount = FastUniq::Uniquify(fileName, threadNum);
                auto end = std::chrono::high_resolution_clock::now();
                runTimeSum += std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                if (uniqueCount.size() != u) {
                    std::cerr << "Error: The number of unique strings is incorrect: ";
                    std::cerr << "Correct: " << u << " Returned answer: " << uniqueCount.size() << "\n";
                    return 1;
                }
            }
        } else {
            for (unsigned i = 0; i < BENCH_REPEAT; i++) {
                auto start = std::chrono::high_resolution_clock::now();
                unsigned uniqueCount = FastUniq::UniquifyToStdout(fileName, threadNum);
                auto end = std::chrono::high_resolution_clock::now();
                runTimeSum += std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                if (uniqueCount != u) {
                    std::cerr << "Error: The number of unique strings is incorrect: ";
                    std::cerr << "Correct: " << u << " Returned answer: " << uniqueCount << "\n";
                    return 1;
                }
            }
        }

        double throughput = fileSize / (runTimeSum / BENCH_REPEAT) / 1e3; // in megabytes
        std::cerr << throughput << " MB/s (average: " << (runTimeSum / BENCH_REPEAT) << " ms)\n";
    }

    std::remove(fileName);
}