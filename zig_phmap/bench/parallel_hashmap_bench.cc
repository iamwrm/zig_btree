#include <parallel_hashmap/phmap.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

using Map = phmap::flat_hash_map<uint64_t, uint64_t>;
using StringMap = phmap::flat_hash_map<std::string, uint64_t>;

static uint64_t splitmix64(uint64_t &state) {
    state += 0x9e3779b97f4a7c15ULL;
    uint64_t z = state;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static void fill_keys(std::vector<uint64_t> &keys) {
    uint64_t state = 0x5eedb7eeULL;
    for (auto &key : keys) key = splitmix64(state);
}

static uint64_t now_ns() {
    using clock = std::chrono::steady_clock;
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               clock::now().time_since_epoch())
        .count();
}

static double ns_per_op(uint64_t ns, size_t ops) {
    return static_cast<double>(ns) / static_cast<double>(ops);
}

int main() {
    constexpr size_t n = 1000000;
    std::vector<uint64_t> keys(n);
    fill_keys(keys);

    Map map;
    uint64_t last = now_ns();
    map.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        map[keys[i]] = static_cast<uint64_t>(i);
    }
    uint64_t current = now_ns();
    const uint64_t insert_ns = current - last;
    last = current;

    uint64_t checksum = 0;
    for (auto key : keys) {
        auto it = map.find(key);
        if (it != map.end()) checksum += it->second;
    }
    current = now_ns();
    const uint64_t lookup_ns = current - last;
    last = current;

    for (auto key : keys) {
        auto it = map.find(key ^ 0xaaaaaaaaaaaaaaaaULL);
        if (it != map.end()) checksum += it->second;
    }
    current = now_ns();
    const uint64_t miss_ns = current - last;
    last = current;

    size_t iter_count = 0;
    for (const auto &entry : map) {
        checksum += entry.first ^ entry.second;
        ++iter_count;
    }
    current = now_ns();
    const uint64_t iterate_ns = current - last;

    const uint64_t mixed_start = now_ns();
    {
        Map mixed;
        mixed.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            mixed[keys[i]] = static_cast<uint64_t>(i);
            if (i >= 2) {
                auto it = mixed.find(keys[i - 2]);
                if (it != mixed.end()) checksum += it->second;
            }
            if (i % 3 == 0) mixed.erase(keys[i / 3]);
        }
    }
    const uint64_t mixed_ns = now_ns() - mixed_start;
    last = now_ns();

    for (auto key : keys) {
        map.erase(key);
    }
    current = now_ns();
    const uint64_t remove_ns = current - last;

    constexpr size_t string_n = 100000;
    std::vector<std::string> string_keys;
    string_keys.reserve(string_n);
    uint64_t string_state = 0x51719eedULL;
    for (size_t i = 0; i < string_n; ++i) {
        std::string key(16, '\0');
        uint64_t a = splitmix64(string_state);
        uint64_t b = splitmix64(string_state);
        std::memcpy(key.data(), &a, sizeof(a));
        std::memcpy(key.data() + 8, &b, sizeof(b));
        string_keys.push_back(std::move(key));
    }
    StringMap string_map;
    last = now_ns();
    string_map.reserve(string_n);
    for (size_t i = 0; i < string_n; ++i) {
        string_map[string_keys[i]] = static_cast<uint64_t>(i);
    }
    current = now_ns();
    const uint64_t string_insert_ns = current - last;
    last = current;
    for (const auto &key : string_keys) {
        auto it = string_map.find(key);
        if (it != string_map.end()) checksum += it->second;
    }
    current = now_ns();
    const uint64_t string_lookup_ns = current - last;

    constexpr size_t high_n = 1800000;
    std::vector<uint64_t> high_keys(high_n);
    fill_keys(high_keys);
    Map high_map;
    high_map.reserve(high_n);
    for (size_t i = 0; i < high_n; ++i) {
        high_map[high_keys[i]] = static_cast<uint64_t>(i);
    }
    last = now_ns();
    for (auto key : high_keys) {
        auto it = high_map.find(key ^ 0x5555555555555555ULL);
        if (it != high_map.end()) checksum += it->second;
    }
    current = now_ns();
    const uint64_t high_load_miss_ns = current - last;

    constexpr size_t churn_n = 500000;
    Map churn_map;
    churn_map.reserve(churn_n);
    last = now_ns();
    for (size_t i = 0; i < churn_n; ++i) {
        churn_map[keys[i]] = static_cast<uint64_t>(i);
    }
    for (size_t i = 0; i < churn_n; ++i) {
        churn_map.erase(keys[i]);
    }
    for (size_t i = 0; i < churn_n; ++i) {
        churn_map[keys[i] ^ 0x3333333333333333ULL] = static_cast<uint64_t>(i);
    }
    current = now_ns();
    const uint64_t tombstone_churn_ns = current - last;

    std::printf("items inserted: %zu\n", n);
    std::printf("unique items: %zu\n", iter_count);
    std::printf("insert_reserved: %.3f ns/op\n", ns_per_op(insert_ns, n));
    std::printf("lookup_hit:      %.3f ns/op\n", ns_per_op(lookup_ns, n));
    std::printf("lookup_miss:     %.3f ns/op\n", ns_per_op(miss_ns, n));
    std::printf("iterate:         %.3f ns/item\n", ns_per_op(iterate_ns, std::max<size_t>(iter_count, 1)));
    std::printf("mixed:           %.3f ns/op\n", ns_per_op(mixed_ns, n));
    std::printf("remove:          %.3f ns/op\n", ns_per_op(remove_ns, n));
    std::printf("string_insert:   %.3f ns/op\n", ns_per_op(string_insert_ns, string_n));
    std::printf("string_lookup:   %.3f ns/op\n", ns_per_op(string_lookup_ns, string_n));
    std::printf("high_load_miss:  %.3f ns/op\n", ns_per_op(high_load_miss_ns, high_n));
    std::printf("tombstone_churn: %.3f ns/op\n", ns_per_op(tombstone_churn_ns, churn_n * 3));
    std::printf("checksum: %llu\n", static_cast<unsigned long long>(checksum));
    return 0;
}
