#include "../FastUniq.hpp"
#include <unordered_set>
#include <fstream>

void Tester(std::string desctiption, std::vector<std::string> v) {
    std::unordered_set<std::string> stringSet(v.begin(), v.end());

    char fileName[] = "/tmp/tempfileXXXXXX";
    int fd = mkstemp(fileName);
    if (fd == -1) {
        perror("mkstemp");
        exit(1);
    }
    
    {
        std::ofstream tmpFile(fileName);
        for (unsigned i = 0; i < v.size(); i++) {
            tmpFile << v[i] << "\n";
        }
    }

    freopen("/dev/null", "w", stdout);

    // Test changing the number of threads
    for (unsigned i = 0; i < omp_get_num_procs(); i++) {
        unsigned result = FastUniq::Uniquify(fileName);
        if (result != stringSet.size()) {
            fprintf(stderr, "Test \"%s\" failed! : ", desctiption.data());
            fprintf(stderr, "Expected=%lu vs. Result=%u\n", stringSet.size(), result);
            std::remove(fileName);
            exit(1);
        }
    }

    fprintf(stderr, "\"%s\" passed\n", desctiption.data());
    std::remove(fileName);
}

// Test if FastUniq can handle edge cases
int main() {
    Tester("Empty File", {});
    Tester("Single newline", {""});
    Tester("Consecutive newlines", {"", "", "", "", ""});

    Tester("Short strings", {"a", "a", "b", "bc", "c", "d", "d"});
    Tester("Short strings and empty strings", {"a", "", "", "a", "", "b", "b", ""});
    Tester("Strings and empty strings", {"string1", "", "", "string1", "", "string2", "string2", ""});
}