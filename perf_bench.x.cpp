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


std::shared_ptr<arrow::ChunkedArray> test_arrow_chunked_array() {

    std::cout << " ======== TEST chunked array ============" << std::endl;
    arrow::Int8Builder int8builder;
    int8_t days_raw1[5] = {1, 12, 17, 23, 28};
    int8_t days_raw2[5] = {1, 12, 17, 23, 28};
    [[maybe_unused]] arrow::Status l_status_days = int8builder.AppendValues(days_raw1, 5);
    std::shared_ptr<arrow::Array> days1 = *int8builder.Finish();
    l_status_days = int8builder.AppendValues(days_raw2, 5);
    std::shared_ptr<arrow::Array> days2 = *int8builder.Finish();

    arrow::ArrayVector days_vec{days1, days2};
    std::shared_ptr<arrow::ChunkedArray> days_chunk = std::make_shared<arrow::ChunkedArray>(days_vec);
    std::cout << " >> " << days_chunk->ToString() << std::endl;
    std::cout << " ======== TEST chunked array ============" << std::endl;
    return days_chunk;
}


int main() {
    test_arrow_chunked_array();
    return 0;
}
