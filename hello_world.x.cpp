//
//  Created by vibhanshu on 2024-11-01
//

#include <iostream>
#include <arrow/api.h>


void test_hello_arrow() {
    std::cout << " ======== TEST hello arrow ============" << std::endl;
    arrow::Int8Builder int8builder;
    int8_t days_raw[5] = {1, 12, 17, 23, 28};
    arrow::Status l_status = int8builder.AppendValues(days_raw, 5);
    (void)l_status;

    std::shared_ptr<arrow::Array> days = *int8builder.Finish();

    std::cout << " > " << *days << std::endl;
    std::cout << " > addr_orig: " << (int64_t)days_raw << std::endl;
    std::cout << " > addr_new:  "  << (int64_t)((std::static_pointer_cast<arrow::Int8Array>(days))->raw_values())
              << std::endl;
    std::cout << " ======== TEST hello arrow ============" << std::endl;
}

void test_arrow_schema() {

    std::cout << " ======== TEST arrow schema ============" << std::endl;
    std::shared_ptr<arrow::Field> field_day, field_month, field_year;
    std::shared_ptr<arrow::Schema> schema;

    field_day = arrow::field("Day", arrow::int8());
    field_month = arrow::field("Month", arrow::int8());
    field_year = arrow::field("Year", arrow::int16());

    schema = arrow::schema({field_day, field_month, field_year});
    // schema = arrow::schema({field_day, field_month});

    arrow::Int8Builder int8builder;
    arrow::Int16Builder int16builder;
    int8_t days_raw[5] = {1, 12, 17, 23, 28};
    int8_t month_raw[5] = {1, 2, 3, 4, 5};
    int16_t year_raw[5] = {1990, 1995, 2000, 2005, 2010};
    [[maybe_unused]] arrow::Status l_status_days = int8builder.AppendValues(days_raw, 5);
    std::shared_ptr<arrow::Array> days = *int8builder.Finish();
    [[maybe_unused]] arrow::Status l_status_month = int8builder.AppendValues(month_raw, 5);
    std::shared_ptr<arrow::Array> month = *int8builder.Finish();
    [[maybe_unused]] arrow::Status l_status_year = int16builder.AppendValues(year_raw, 5);
    std::shared_ptr<arrow::Array> year = *int16builder.Finish();

    std::shared_ptr<arrow::RecordBatch> rbatch;
    rbatch = arrow::RecordBatch::Make(schema, days->length(), {days, month, year});

    std::cout << rbatch->ToString() << std::endl;
    std::cout << " ======== TEST arrow schema ============" << std::endl;
}

void test_arrow_chunked_array() {

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
}

void test_table() {
    std::cout << " ======== TEST table ============" << std::endl;
    std::shared_ptr<arrow::Field> field_day, field_month, field_year;
    std::shared_ptr<arrow::Schema> schema;

    field_day = arrow::field("Day", arrow::int8());
    field_month = arrow::field("Month", arrow::int8());
    field_year = arrow::field("Year", arrow::int16());

    schema = arrow::schema({field_day, field_month, field_year});
    // schema = arrow::schema({field_day, field_month});

    arrow::Int8Builder int8builder;
    arrow::Int16Builder int16builder;
    int8_t days_raw[5] = {1, 12, 17, 23, 28};
    int8_t month_raw[5] = {1, 2, 3, 4, 5};
    int16_t year_raw[5] = {1990, 1995, 2000, 2005, 2010};
    [[maybe_unused]] arrow::Status l_status_days = int8builder.AppendValues(days_raw, 5);
    std::shared_ptr<arrow::Array> days = *int8builder.Finish();
    l_status_days = int8builder.AppendValues(days_raw, 5);
    std::shared_ptr<arrow::Array> days2 = *int8builder.Finish();
    [[maybe_unused]] arrow::Status l_status_month = int8builder.AppendValues(month_raw, 5);
    std::shared_ptr<arrow::Array> month = *int8builder.Finish();
    l_status_month = int8builder.AppendValues(month_raw, 5);
    std::shared_ptr<arrow::Array> month2 = *int8builder.Finish();
    [[maybe_unused]] arrow::Status l_status_year = int16builder.AppendValues(year_raw, 5);
    std::shared_ptr<arrow::Array> year = *int16builder.Finish();
    l_status_year = int16builder.AppendValues(year_raw, 5);
    std::shared_ptr<arrow::Array> year2 = *int16builder.Finish();

    arrow::ArrayVector days_vec{days, days2};
    std::shared_ptr<arrow::ChunkedArray> days_chunk = std::make_shared<arrow::ChunkedArray>(days_vec);
    arrow::ArrayVector month_vec{month, month2};
    std::shared_ptr<arrow::ChunkedArray> month_chunk = std::make_shared<arrow::ChunkedArray>(month_vec);
    arrow::ArrayVector year_vec{year, year2};
    std::shared_ptr<arrow::ChunkedArray> year_chunk = std::make_shared<arrow::ChunkedArray>(year_vec);

    std::shared_ptr<arrow::Table> table;
    table = arrow::Table::Make(schema, {days_chunk, month_chunk, year_chunk});

    std::cout << table->ToString() << std::endl;

    std::cout << " ======== TEST table ============" << std::endl;
}

int main() {

    test_hello_arrow();
    test_arrow_schema();
    test_arrow_chunked_array();
    test_table();
    return 0;
}
