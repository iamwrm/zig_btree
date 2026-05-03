#include <absl/container/btree_map.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <vector>

using Clock = std::chrono::steady_clock;

static double ns_per_op(std::chrono::nanoseconds ns, std::size_t ops) {
  return static_cast<double>(ns.count()) / static_cast<double>(ops);
}

static std::uint64_t next_key(std::uint64_t* state) {
  *state += 0x9E3779B97F4A7C15ULL;
  std::uint64_t z = *state;
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
  return z ^ (z >> 31);
}

int main() {
  constexpr std::size_t n = 1'000'000;

  std::vector<std::uint64_t> keys;
  keys.reserve(n);
  std::uint64_t key_state = 0x5eedb7eeULL;
  for (std::size_t i = 0; i < n; ++i) {
    keys.push_back(next_key(&key_state));
  }

  absl::btree_map<std::uint64_t, std::uint64_t> map;

  auto last = Clock::now();
  for (std::size_t i = 0; i < n; ++i) {
    map[keys[i]] = static_cast<std::uint64_t>(i);
  }
  auto now = Clock::now();
  const auto insert_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now - last);
  last = now;

  std::uint64_t checksum = 0;
  for (std::uint64_t key : keys) {
    auto it = map.find(key);
    if (it != map.end()) checksum += it->second;
  }
  now = Clock::now();
  const auto lookup_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now - last);
  last = now;

  std::size_t iter_count = 0;
  for (const auto& entry : map) {
    checksum += entry.first ^ entry.second;
    ++iter_count;
  }
  now = Clock::now();
  const auto iterate_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now - last);
  last = now;

  for (std::uint64_t key : keys) {
    map.erase(key);
  }
  now = Clock::now();
  const auto remove_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now - last);

  std::printf(
      "items inserted: %zu\n"
      "unique items: %zu\n"
      "insert:  %.3f ns/op\n"
      "lookup:  %.3f ns/op\n"
      "iterate: %.3f ns/item\n"
      "remove:  %.3f ns/op\n"
      "checksum: %llu\n",
      n, iter_count, ns_per_op(insert_ns, n), ns_per_op(lookup_ns, n),
      ns_per_op(iterate_ns, iter_count == 0 ? 1 : iter_count),
      ns_per_op(remove_ns, n), static_cast<unsigned long long>(checksum));
}
