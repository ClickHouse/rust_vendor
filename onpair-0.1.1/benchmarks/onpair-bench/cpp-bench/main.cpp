// Mirror of rust-bench. Reads LF-delimited bytes, builds payload + offsets,
// times OnPairColumn::compress, optionally times per-row decompress and
// verifies output equals the input slice. Emits one JSON line on stdout.

#include <onpair/api.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace {

struct Args {
  std::string input;
  uint32_t bits = 12;
  uint32_t iters = 5;
  uint32_t warmup = 1;
  bool decompress = false;
  bool verify = false;
};

[[noreturn]] void die(const std::string& msg) {
  std::fprintf(stderr, "cpp_bench: %s\n", msg.c_str());
  std::exit(1);
}

Args parse_args(int argc, char** argv) {
  Args a;
  auto need = [&](int& i, const char* flag) {
    if (++i >= argc) die(std::string("missing value for ") + flag);
    return std::string(argv[i]);
  };
  for (int i = 1; i < argc; ++i) {
    std::string_view s(argv[i]);
    if (s == "--bits") a.bits = static_cast<uint32_t>(std::stoul(need(i, "--bits")));
    else if (s == "--iters") a.iters = static_cast<uint32_t>(std::stoul(need(i, "--iters")));
    else if (s == "--warmup") a.warmup = static_cast<uint32_t>(std::stoul(need(i, "--warmup")));
    else if (s == "--decompress") a.decompress = true;
    else if (s == "--verify") a.verify = true;
    else if (!s.empty() && s.substr(0, 2) != "--") a.input.assign(s);
    else die(std::string("unknown arg: ") + std::string(s));
  }
  if (a.input.empty()) die("missing input path");
  return a;
}

std::vector<uint8_t> read_file(const std::string& path) {
  std::ifstream f(path, std::ios::binary);
  if (!f) die("read " + path);
  f.seekg(0, std::ios::end);
  auto sz = f.tellg();
  f.seekg(0, std::ios::beg);
  std::vector<uint8_t> out(static_cast<size_t>(sz));
  if (sz > 0) f.read(reinterpret_cast<char*>(out.data()), sz);
  return out;
}

void build_payload_and_offsets(const std::vector<uint8_t>& src,
                               std::vector<uint8_t>& payload,
                               std::vector<uint32_t>& offsets) {
  payload.clear();
  offsets.clear();
  payload.reserve(src.size());
  offsets.reserve(src.size() / 32 + 2);
  offsets.push_back(0);
  size_t row_start = 0;
  for (size_t i = 0; i < src.size(); ++i) {
    if (src[i] == '\n') {
      payload.insert(payload.end(), src.begin() + row_start, src.begin() + i);
      offsets.push_back(static_cast<uint32_t>(payload.size()));
      row_start = i + 1;
    }
  }
  if (row_start < src.size()) {
    payload.insert(payload.end(), src.begin() + row_start, src.end());
    offsets.push_back(static_cast<uint32_t>(payload.size()));
  }
}

uint64_t elapsed_ns(std::chrono::steady_clock::time_point t0) {
  using namespace std::chrono;
  return static_cast<uint64_t>(duration_cast<nanoseconds>(steady_clock::now() - t0).count());
}

std::string join_u64(const std::vector<uint64_t>& v) {
  std::ostringstream oss;
  oss << '[';
  for (size_t i = 0; i < v.size(); ++i) {
    if (i) oss << ',';
    oss << v[i];
  }
  oss << ']';
  return oss.str();
}

onpair::encoding::TrainingConfig make_cfg(uint32_t bits) {
  onpair::encoding::TrainingConfig cfg;
  cfg.bits = static_cast<onpair::BitWidth>(bits);
  cfg.threshold = onpair::encoding::DynamicThreshold{0.2};
  return cfg;
}

onpair::OnPairColumn do_compress(uint32_t bits,
                                 const std::vector<uint8_t>& payload,
                                 const std::vector<uint32_t>& offsets) {
  const size_t n = offsets.empty() ? 0 : offsets.size() - 1;
  return onpair::OnPairColumn::compress(
      reinterpret_cast<const char*>(payload.data()), offsets.data(), n, make_cfg(bits));
}

size_t codes_bytes_of(const onpair::OnPairColumnView& view) {
  const auto store = view.store();
  const size_t total_bits = store.num_tokens() * static_cast<size_t>(store.bits());
  return (total_bits + 7) / 8;
}

size_t dict_bytes_of(const onpair::OnPairColumnView& view) {
  const auto dict = view.dictionary();
  const size_t n = dict.num_tokens();
  return n == 0 ? 0 : static_cast<size_t>(dict.raw_offsets()[n]);
}

}  // namespace

int main(int argc, char** argv) {
  Args args = parse_args(argc, argv);
  auto bytes = read_file(args.input);
  size_t input_bytes = bytes.size();
  std::vector<uint8_t> payload;
  std::vector<uint32_t> offsets;
  build_payload_and_offsets(bytes, payload, offsets);
  size_t num_rows = offsets.empty() ? 0 : offsets.size() - 1;

  std::vector<uint64_t> compress_ns;
  compress_ns.reserve(args.iters);
  for (uint32_t i = 0; i < args.warmup; ++i) {
    (void)do_compress(args.bits, payload, offsets);
  }
  std::optional<onpair::OnPairColumn> last;
  for (uint32_t i = 0; i < args.iters; ++i) {
    auto t0 = std::chrono::steady_clock::now();
    auto col = do_compress(args.bits, payload, offsets);
    compress_ns.push_back(elapsed_ns(t0));
    last.emplace(std::move(col));
  }
  if (!last) die("--iters must be >= 1");

  auto view = last->view();

  // Max row length, used to size the scratch buffer (+ decoder padding).
  size_t max_row = 0;
  for (size_t r = 0; r < num_rows; ++r) {
    const size_t len = offsets[r + 1] - offsets[r];
    if (len > max_row) max_row = len;
  }
  std::vector<char> scratch(max_row + onpair::DECOMPRESS_BUFFER_PADDING);

  std::vector<uint64_t> decompress_ns;
  if (args.decompress) {
    for (uint32_t i = 0; i < args.warmup; ++i) {
      for (size_t r = 0; r < num_rows; ++r) (void)view.decompress(r, scratch.data());
    }
    decompress_ns.reserve(args.iters);
    for (uint32_t i = 0; i < args.iters; ++i) {
      auto t0 = std::chrono::steady_clock::now();
      for (size_t r = 0; r < num_rows; ++r) (void)view.decompress(r, scratch.data());
      decompress_ns.push_back(elapsed_ns(t0));
    }
  }

  if (args.verify) {
    for (size_t r = 0; r < num_rows; ++r) {
      const size_t len = view.decompress(r, scratch.data());
      const uint32_t start = offsets[r];
      const uint32_t end = offsets[r + 1];
      if (len != static_cast<size_t>(end - start) ||
          std::memcmp(scratch.data(), payload.data() + start, len) != 0) {
        std::fprintf(stderr, "verify failed at row %zu\n", r);
        return 2;
      }
    }
  }

  std::cout << "{\"impl\":\"cpp\","
            << "\"bits\":" << args.bits << ','
            << "\"num_rows\":" << num_rows << ','
            << "\"input_bytes\":" << input_bytes << ','
            << "\"dict_size\":" << view.dictionary().num_tokens() << ','
            << "\"dict_bytes\":" << dict_bytes_of(view) << ','
            << "\"codes_bytes\":" << codes_bytes_of(view) << ','
            // dict + bit-packed codes only — excludes the StoreView boundary
            // index (the "Store" side-table). Rust bench reports the same.
            << "\"compressed_bytes\":" << (view.dictionary().bytes_used() + codes_bytes_of(view)) << ','
            << "\"compress_ns\":" << join_u64(compress_ns) << ','
            << "\"decompress_ns\":" << join_u64(decompress_ns)
            << '}' << std::endl;
  return 0;
}
