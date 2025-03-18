//
//  Created by vibhanshu on 2024-11-01
//


#include <iostream>
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

static uint64_t rdtsc_snap() {
    uint64_t rax, rdx, aux;
    __asm__ __volatile__("rdtscp\n" : "=a"(rax), "=d"(rdx), "=c"(aux) : :);
    return (rdx << 32u) + rax;
}

class BenchContext{
    const std::string m_name;
    const uint64_t m_begin;
private:
    static inline bool s_in_context;
    static inline std::vector<uint64_t> s_latency_record;
public:
    explicit BenchContext(const std::string& name)
        : m_name{name}, m_begin{rdtsc_snap()} {
        if (s_in_context) {
            std::cout <<  "already in context" << std::endl;
            ::abort();
            // ::abort("already in context");
        }
        s_in_context = true;
    }
    ~BenchContext() {
        const uint64_t m_end = rdtsc_snap();
        const uint64_t l_latency = m_end - m_begin;
        // std::cout << "Latency:[" << m_name << "] [" << l_latency << "]" << std::endl;
        s_latency_record.emplace_back(l_latency);
        s_in_context = false;
    }
public:
    static const std::vector<uint64_t> latency_vec() {
        return s_latency_record;
    }
};


std::shared_ptr<arrow::ChunkedArray> test_arrow_chunked_array() {

    std::cout << " ======== TEST chunked array ============" << std::endl;
    arrow::Int32Builder int32Builder;
    enum : uint32_t {
        c_max_num_values = 50u,
        c_max_num_iter = 1000u
    };
    int32_t days_raw1[c_max_num_values];
    int32_t days_raw2[c_max_num_values];
    for (int32_t i = 0; i != c_max_num_values; ++i) {
        days_raw1[i] = i * i + i;
        days_raw2[i] = i * i * i + i + i;
    }

    arrow::Status l_status_days;
    arrow::ArrayVector days_vec_all;
    std::shared_ptr<arrow::Array> days_vec;
    for (uint32_t i = 0; i != c_max_num_iter; ++i) {
        BenchContext l_entry{"IterLatency"};
        if (i % 3 == 0) {
            l_status_days = int32Builder.AppendValues(days_raw2, c_max_num_values);
            days_vec = *int32Builder.Finish();
            days_vec_all.emplace_back(std::move(days_vec));
        } else {
            l_status_days = int32Builder.AppendValues(days_raw1, c_max_num_values);
            days_vec = *int32Builder.Finish();
            days_vec_all.emplace_back(std::move(days_vec));
        }
    }

    std::vector<uint64_t> l_latency_vec = BenchContext::latency_vec();
    std::sort(l_latency_vec.begin(), l_latency_vec.end());
    {
        std::stringstream ss;
        ss << "Minimum latency : ";
        for (uint32_t i = 0; i != 5u; ++i) {
            ss << ", " << l_latency_vec[i];
        }
        ss << "\n";
        std::cout << ss.str();
    }
    {
        std::stringstream ss;
        ss << "Maximum latency : ";
        for (uint32_t i = 0; i != 5u; ++i) {
            ss << ", " << l_latency_vec[l_latency_vec.size() - 1 - i];
        }
        ss << "\n";
        std::cout << ss.str();
    }
    std::cout << " median latency : " << l_latency_vec[c_max_num_values/2] << std::endl;

    std::shared_ptr<arrow::ChunkedArray> days_chunk;
    {
        BenchContext l_entry{"makeChunkArray"};
        days_chunk = std::make_shared<arrow::ChunkedArray>(days_vec_all);
    }
    std::cout << " >> " << days_chunk->ToString() << std::endl;
    std::cout << " >> Make chunk array lantency: " << BenchContext::latency_vec().back() << std::endl;
    std::cout << " ======== TEST chunked array ============" << std::endl;
    return days_chunk;
}


int main() {
    test_arrow_chunked_array();
    return 0;
}
