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
public:
    explicit BenchContext(const std::string& name)
        : m_name{name}, m_begin{rdtsc_snap()} {
    }
    ~BenchContext() {
        const uint64_t m_end = rdtsc_snap();
        std::cout << "Latency:[" << m_name << "] [" << (m_end - m_begin) << "]" << std::endl;
    }

};


std::shared_ptr<arrow::ChunkedArray> test_arrow_chunked_array() {

    std::cout << " ======== TEST chunked array ============" << std::endl;
    arrow::Int32Builder int32Builder;
    int32_t days_raw1[5] = {1, 12, 17, 23, 28};
    int32_t days_raw2[5] = {10, 120, 107, 230, 208};

    arrow::Status l_status_days;
    arrow::ArrayVector days_vec_all;
    std::shared_ptr<arrow::Array> days_vec;
    for (uint32_t i = 0; i != 10; ++i) {
        if (i % 3 == 0) {
            l_status_days = int32Builder.AppendValues(days_raw2, 5);
            days_vec = *int32Builder.Finish();
            days_vec_all.emplace_back(std::move(days_vec));
        } else {
            l_status_days = int32Builder.AppendValues(days_raw1, 5);
            days_vec = *int32Builder.Finish();
            days_vec_all.emplace_back(std::move(days_vec));
        }
    }

    std::shared_ptr<arrow::ChunkedArray> days_chunk = std::make_shared<arrow::ChunkedArray>(days_vec_all);
    std::cout << " >> " << days_chunk->ToString() << std::endl;
    std::cout << " ======== TEST chunked array ============" << std::endl;
    return days_chunk;
}


int main() {
    test_arrow_chunked_array();
    return 0;
}
