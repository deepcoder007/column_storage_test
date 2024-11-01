//
//  Created by vibhanshu on 2024-11-01
//

#include <iostream>
#include <arrow/api.h>


void test_hello_arrow() {
    arrow::Int8Builder int8builder;
    int8_t days_raw[5] = {1, 12, 17, 23, 28};
    arrow::Status l_status = int8builder.AppendValues(days_raw, 5);
    (void)l_status;

    std::shared_ptr<arrow::Array> days = *int8builder.Finish();

    std::cout << " > " << *days << std::endl;
    std::cout << " > addr_orig: " << (int64_t)days_raw << std::endl;
    std::cout << " > addr_new:  "  << (int64_t)((std::static_pointer_cast<arrow::Int8Array>(days))->raw_values())
              << std::endl;
}

void test_arrow_schema() {

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
}

int main() {

    // test_hello_arrow();
    test_arrow_schema();
    return 0;
}