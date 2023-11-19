#include <algorithm>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <random>
#include <rusty/macro.h>
#include <rusty/time.h>
#include <thread>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using seed_t = std::mt19937_64::result_type;

enum class IOType {
	RandRead,
	Read,
	Write,
};

std::optional<size_t> parse_size(const char *start, size_t n) {
	if (n == 0) {
		return std::nullopt;
	}
	const char *cur = start + n - 1;
	std::string suffix;
	while (!std::isdigit(*cur)) {
		suffix.push_back(*cur);
		if (start == cur) {
			return std::nullopt;
		}
		--cur;
	}
	std::reverse(suffix.begin(), suffix.end());
	size_t size_unit;
	if (suffix.empty()) {
		size_unit = 1;
	} else if (suffix == "K" || suffix == "KiB") {
		size_unit = 1024;
	} else if (suffix == "KB") {
		size_unit = 1000;
	} else if (suffix == "M" || suffix == "MiB") {
		size_unit = 1024 * 1024;
	} else if (suffix == "MB") {
		size_unit = 1000000;
	} else if (suffix == "G" || suffix == "GiB") {
		size_unit = 1024 * 1024 * 1024;
	} else if (suffix == "GB") {
		size_unit = 1000000000;
	} else {
		return std::nullopt;
	}
	size_t size = 0;
	size_t base = 1;
	for (;;) {
		int v = *cur - '0';
		if (v < 0 || v > 9) {
			return std::nullopt;
		}
		size += v * base;
		if (cur == start) {
			break;
		}
		--cur;
		base *= 10;
	}
	return size * size_unit;
}

struct Options {
	// Allocated buffer should align to blksize
	size_t blksize;

	std::optional<size_t> bandwidth;
	size_t bs;
	IOType io_type;
	size_t num_blocks;
};

class Worker {
public:
	Worker(const Options &options, int fd, seed_t seed)
	  : options_(options),
		fd_(fd),
		rng_(seed),
		buf_(options_.bs + options_.blksize - 1),
		aligned_buf_((char *)(
			((uintptr_t)buf_.data() + options_.blksize - 1) &
				~(uintptr_t)(options_.blksize - 1)
		)),
		block_dist(0, options_.num_blocks - 1),
		io_time_(rusty::time::Duration::from_nanos(0)),
		run_time_(rusty::time::Duration::from_nanos(0)) {}
	void run() {
		rusty::time::Instant start = rusty::time::Instant::now();
		if (options_.bandwidth.has_value()) {
			rusty::time::Duration interval = rusty::time::Duration::from_nanos(
				options_.bs * 1e9 / options_.bandwidth.value()
			);
			rusty::time::Instant next_begin =
				rusty::time::Instant::now() + interval;
			size_t num_op = options_.num_blocks;
			while (num_op) {
				num_op -= 1;
				rw_one_block();
				std::optional<rusty::time::Duration> sleep_time =
					next_begin.checked_duration_since(
						rusty::time::Instant::now()
					);
				if (sleep_time.has_value()) {
					std::this_thread::sleep_for(
						std::chrono::nanoseconds(sleep_time.value().as_nanos())
					);
				}
				next_begin += interval;
			}
		} else {
			size_t num_op = options_.num_blocks;
			while (num_op) {
				num_op -= 1;
				rw_one_block();
			}
		}
		run_time_ += start.elapsed();
	}
	rusty::time::Duration io_time() const { return io_time_; }
	rusty::time::Duration run_time() const { return run_time_; }

private:
	void rw_one_block() {
		auto start = rusty::time::Instant::now();
		switch (options_.io_type) {
		case IOType::RandRead: {
			char *buf = aligned_buf_;
			size_t offset = block_dist(rng_) * options_.bs;
			size_t n = options_.bs;
			do {
				ssize_t ret = pread(fd_, buf, n, offset);
				if (ret == -1 || ret == 0) {
					perror("pread");
					rusty_panic();
				}
				assert(ret > 0);
				buf += ret;
				n -= ret;
				offset += ret;
			} while (n);
		} break;
		case IOType::Read: {
			char *buf = aligned_buf_;
			size_t n = options_.bs;
			do {
				ssize_t ret = read(fd_, buf, n);
				if (ret == -1 || ret == 0) {
					perror("read");
					rusty_panic();
				}
				assert(ret > 0);
				buf += ret;
				n -= ret;
			} while (n);
		} break;
		case IOType::Write: {
			char *buf = aligned_buf_;
			size_t n = options_.bs;
			do {
				ssize_t ret = write(fd_, buf, n);
				if (ret == -1 || ret == 0) {
					perror("write");
					rusty_panic();
				}
				assert(ret > 0);
				buf += ret;
				n -= ret;
			} while (n);
		} break;
		}
		io_time_ += start.elapsed();
	}

	const Options &options_;
	int fd_;

	std::mt19937_64 rng_;
	std::vector<char> buf_;
	char *aligned_buf_;
	std::uniform_int_distribution<size_t> block_dist;
	rusty::time::Duration io_time_;
	rusty::time::Duration run_time_;
};

int main(int argc, char **argv) {
	std::string arg_bs;
	std::string filename;
	size_t numjobs;
	std::string readwrite;
	std::string arg_size;

	namespace po = boost::program_options;
	po::options_description desc("Available options");
	desc.add_options()("help", "Print help message");
	desc.add_options()("bandwidth", po::value<std::string>());
	desc.add_options()("bs", po::value<std::string>(&arg_bs)->required());
	desc.add_options()(
		"filename", po::value<std::string>(&filename)->required()
	);
	desc.add_options()(
		"group_reporting",
		"Display statistics for groups of jobs as a whole "
			"instead of for each individual job"
	);
	desc.add_options()(
		"numjobs", po::value<size_t>(&numjobs)->default_value(1)
	);
	desc.add_options()(
		"readwrite", po::value<std::string>(&readwrite)->required(),
		"randread/read/write"
	);
	desc.add_options()("randseed", po::value<seed_t>());
	desc.add_options()("size", po::value<std::string>(&arg_size)->required());
	desc.add_options()("verbose", "Print extra messages");

	po::variables_map vm;
	po::store(po::parse_command_line(argc, argv, desc), vm);
	if (vm.count("help")) {
		std::cerr << desc << std::endl;
		return 1;
	}
	po::notify(vm);
	bool verbose = vm.count("verbose");

	std::optional<size_t> bandwidth;
	if (vm.count("bandwidth")) {
		std::string bw = vm["bandwidth"].as<std::string>();
		// xxxB/s, at least 4 characters
		rusty_assert(
			bw.size() >= 4, "Invalid argument bandwidth: %s", bw.c_str()
		);
		rusty_assert(
			bw[bw.size() - 3] == 'B' && bw[bw.size() - 2] == '/' &&
				bw[bw.size() - 1] == 's',
			"Invalid argument bandwidth: %s", bw.c_str()
		);
		auto ret = parse_size(bw.data(), bw.size() - 2);
		rusty_assert(
			ret.has_value(), "Invalid argument bandwidth: %s", bw.c_str()
		);
		bandwidth = ret.value();
		if (verbose) {
			std::cout << "bandwidth: " << bandwidth.value() << "B/s"
				<< std::endl;
		}
	}

	auto bs_ret = parse_size(arg_bs.data(), arg_bs.size());
	rusty_assert(bs_ret.has_value(), "Invalid argument bs: %s", arg_bs.c_str());
	size_t bs = bs_ret.value();
	if (verbose) {
		std::cout << "bs: " << bs << 'B' << std::endl;
	}

	bool group_reporting;
	if (vm.count("group_reporting")) {
		group_reporting = true;
	} else {
		group_reporting = false;
	}

	IOType io_type;
	if (readwrite == "randread") {
		io_type = IOType::RandRead;
	} else if (readwrite == "read") {
		io_type = IOType::Read;
	} else if (readwrite == "write") {
		io_type = IOType::Write;
	} else {
		rusty_panic("Invalid argument readwrite: %s", readwrite.c_str());
	}

	seed_t randseed;
	if (vm.count("randseed")) {
		randseed = vm["randseed"].as<seed_t>();
	} else {
		std::random_device rd;
		randseed = std::uniform_int_distribution<seed_t>()(rd);
	}
	std::mt19937_64 rng(randseed);

	auto ret = parse_size(arg_size.data(), arg_size.size());
	rusty_assert(
		ret.has_value(), "Invalid argument size: %s", arg_size.c_str()
	);
	size_t size = ret.value();
	if (verbose) {
		std::cout << "size in bytes: " << size << std::endl;
	}
	if (size % bs != 0) {
		std::cerr << "bs " << arg_bs << " does not divide size " << arg_size
			<< std::endl;
		return 1;
	}
	size_t num_blocks = size / bs;
	if (num_blocks == 0) {
		return 0;
	}

	int fd;
	switch (io_type) {
	case IOType::RandRead:
	case IOType::Read:
		for (;;) {
			fd = open(filename.c_str(), O_DIRECT | O_RDONLY);
			if (fd == -1) {
				if (errno != ENOENT) {
					perror("open");
					rusty_panic();
				}
				std::cout << "Target file does not exists, writing...";
			} else {
				rusty_assert(fd >= 0);
				struct stat file_stat;
				if (fstat(fd, &file_stat) == -1) {
					perror("fstat");
					rusty_panic();
				}
				if (file_stat.st_size >= size) {
					break;
				}
				std::cout << "Target file too small, rewriting...";
			}
			std::cout.flush();
			fd = open(
				filename.c_str(), O_DIRECT | O_WRONLY | O_CREAT | O_TRUNC,
				S_IRUSR | S_IWUSR
			);
			if (fd == -1) {
				perror("open");
				rusty_panic();
			}
			struct stat file_stat;
			if (fstat(fd, &file_stat) == -1) {
				perror("fstat");
				rusty_panic();
			}
			Worker(
				Options{
					.blksize = static_cast<size_t>(file_stat.st_blksize),
					.bandwidth = std::nullopt,
					.bs = bs,
					.io_type = IOType::Write,
					.num_blocks = num_blocks,
				},
				fd, rng()
			).run();
			rusty_assert(close(fd) == 0);
			std::cout << " done" << std::endl;
		}
		break;
	case IOType::Write:
		if (numjobs > 1) {
			std::cerr << "Multithread write is not supported yet." << std::endl;
			return 1;
		}
		fd = open(
			filename.c_str(), O_DIRECT | O_WRONLY | O_CREAT | O_TRUNC,
			S_IRUSR | S_IWUSR
		);
		break;
	}
	if (fd == -1) {
		perror("open");
		rusty_panic();
	}

	struct stat file_stat;
	if (fstat(fd, &file_stat) == -1) {
		perror("fstat");
		rusty_panic();
	}
	Options options {
		.blksize = static_cast<size_t>(file_stat.st_blksize),
		.bandwidth = bandwidth,
		.bs = bs,
		.io_type = io_type,
		.num_blocks = num_blocks,
	};

	std::vector<Worker> workers;
	workers.reserve(numjobs);
	std::vector<std::thread> threads;
	for (size_t i = 0; i < numjobs; ++i) {
		workers.emplace_back(options, fd, rng());
	}
	auto run_start = rusty::time::Instant::now();
	for (size_t i = 0; i < numjobs; ++i) {
		threads.emplace_back([&workers, i] { workers[i].run(); });
	}
	for (size_t i = 0; i < numjobs; ++i) {
		threads[i].join();
	}
	auto run_time = run_start.elapsed();
	if (numjobs > 1 && group_reporting) {
		auto io_time = rusty::time::Duration::from_nanos(0);
		for (size_t i = 0; i < numjobs; ++i) {
			io_time += workers[i].io_time();
		}
		std::cout << "Throughput "
			<< size * numjobs / run_time.as_secs_double() / 1e6
			<< "MB/s, avg latency " << io_time.as_nanos() / (num_blocks * numjobs)
			<< "ns" << std::endl;
	} else {
		for (size_t i = 0; i < numjobs; ++i) {
			if (numjobs > 1) {
				std::cout << i << ": ";
			}
			std::cout << "throughput "
				<< size / workers[i].run_time().as_secs_double() / 1e6
				<< "MB/s, avg latency "
				<< workers[i].io_time().as_nanos() / num_blocks << "ns"
				<< std::endl;
		}
	}

	return 0;
}
