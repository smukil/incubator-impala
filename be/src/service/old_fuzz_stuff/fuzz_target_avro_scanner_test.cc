#include "exec/hdfs-avro-scanner.h"

#include <algorithm>
#include <limits.h>

#include "exec/read-write-util.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "testutil/gtest-util.h"
#include "common/init.h"
#include "runtime/test-env.h"

#include "common/names.h"

// TODO: IMPALA-3658: complete CHAR unit tests.
// TODO: IMPALA-3658: complete VARCHAR unit tests.

namespace impala {

class HdfsAvroScannerTestFuzz {
 public:
  void TestReadUnionType(int null_union_position, uint8_t* data, int64_t data_len,
      bool expected_is_null, TErrorCode::type expected_error = TErrorCode::OK) {
    // Reset parse_status_
    scanner_.parse_status_ = Status::OK();

    uint8_t* new_data = data;
    bool is_null = -1;
    bool expect_success = expected_error == TErrorCode::OK;

    bool success = scanner_.ReadUnionType(null_union_position, &new_data, data + data_len,
        &is_null);
    EXPECT_EQ(success, expect_success);

    if (success) {
      EXPECT_TRUE(scanner_.parse_status_.ok());
      EXPECT_EQ(is_null, expected_is_null);
      EXPECT_EQ(new_data, data + 1);
    } else {
      EXPECT_EQ(scanner_.parse_status_.code(), expected_error);
    }
  }

  template<typename T>
  void CheckReadResult(T expected_val, int expected_encoded_len,
      TErrorCode::type expected_error, T val, bool success, int actual_encoded_len) {
    bool expect_success = expected_error == TErrorCode::OK;
    EXPECT_EQ(success, expect_success);

    if (success) {
      EXPECT_TRUE(scanner_.parse_status_.ok());
      EXPECT_EQ(val, expected_val);
      EXPECT_EQ(expected_encoded_len, actual_encoded_len);
    } else {
      EXPECT_EQ(scanner_.parse_status_.code(), expected_error);
    }
  }

  // Templated function for calling different ReadAvro* functions.
  //
  // PrimitiveType is a template parameter so we can pass in int 'slot_byte_size' to
  // ReadAvroDecimal, but otherwise this argument is always the PrimitiveType 'type'
  // argument.
  template<typename T, typename ReadAvroTypeFn, typename PrimitiveType>
  void TestReadAvroType(ReadAvroTypeFn read_fn, PrimitiveType type, uint8_t* data,
      int64_t data_len, T expected_val, int expected_encoded_len,
      TErrorCode::type expected_error = TErrorCode::OK) {
    // Reset parse_status_
    scanner_.parse_status_ = Status::OK();
    uint8_t* new_data = data;
    T slot;
    bool success = (scanner_.*read_fn)(type, &new_data, data + data_len, true, &slot,
        NULL);
    //CheckReadResult(expected_val, expected_encoded_len, expected_error, slot, success,
    //    new_data - data);
  }

  void TestReadAvroBoolean(uint8_t* data, int64_t data_len, bool expected_val,
      TErrorCode::type expected_error = TErrorCode::OK) {
    TestReadAvroType(&HdfsAvroScanner::ReadAvroBoolean, TYPE_BOOLEAN, data, data_len,
        expected_val, 1, expected_error);
  }

  void TestReadAvroInt32(uint8_t* data, int64_t data_len, int32_t expected_val,
      int expected_encoded_len, TErrorCode::type expected_error = TErrorCode::OK) {
    TestReadAvroType(&HdfsAvroScanner::ReadAvroInt32, TYPE_INT, data, data_len,
        expected_val, expected_encoded_len, expected_error);
    // Test type promotion to long, float, and double
    int64_t expected_bigint = expected_val;
    TestReadAvroType(&HdfsAvroScanner::ReadAvroInt32, TYPE_BIGINT, data, data_len,
        expected_bigint, expected_encoded_len, expected_error);
    float expected_float = expected_val;
    TestReadAvroType(&HdfsAvroScanner::ReadAvroInt32, TYPE_FLOAT, data, data_len,
        expected_float, expected_encoded_len, expected_error);
    double expected_double = expected_val;
    TestReadAvroType(&HdfsAvroScanner::ReadAvroInt32, TYPE_DOUBLE, data, data_len,
        expected_double, expected_encoded_len, expected_error);
  }

  void TestReadAvroInt64(uint8_t* data, int64_t data_len, int64_t expected_val,
      int expected_encoded_len, TErrorCode::type expected_error = TErrorCode::OK) {
    TestReadAvroType(&HdfsAvroScanner::ReadAvroInt64, TYPE_BIGINT, data, data_len,
        expected_val, expected_encoded_len, expected_error);
    // Test type promotion to float and double
    float expected_float = expected_val;
    TestReadAvroType(&HdfsAvroScanner::ReadAvroInt64, TYPE_FLOAT, data, data_len,
        expected_float, expected_encoded_len, expected_error);
    double expected_double = expected_val;
    TestReadAvroType(&HdfsAvroScanner::ReadAvroInt64, TYPE_DOUBLE, data, data_len,
        expected_double, expected_encoded_len, expected_error);
  }

  void TestReadAvroFloat(uint8_t* data, int64_t data_len, float expected_val,
      TErrorCode::type expected_error = TErrorCode::OK) {
    TestReadAvroType(&HdfsAvroScanner::ReadAvroFloat, TYPE_FLOAT, data, data_len,
        expected_val, 4, expected_error);
    // Test type promotion to double
    double expected_double = expected_val;
    TestReadAvroType(&HdfsAvroScanner::ReadAvroFloat, TYPE_DOUBLE, data, data_len,
        expected_double, 4, expected_error);
  }

  void TestReadAvroDouble(uint8_t* data, int64_t data_len, double expected_val,
      TErrorCode::type expected_error = TErrorCode::OK) {
    TestReadAvroType(&HdfsAvroScanner::ReadAvroDouble, TYPE_DOUBLE, data, data_len,
        expected_val, 8, expected_error);
  }

  void TestReadAvroChar(int max_len, uint8_t* data, int64_t data_len,
      StringValue expected_val, int expected_encoded_len,
      TErrorCode::type expected_error = TErrorCode::OK) {
    // Reset parse_status_
    scanner_.parse_status_ = Status::OK();
    uint8_t* new_data = data;
    char slot[max<int>(sizeof(StringValue), max_len)];
    bool success = scanner_.ReadAvroChar(TYPE_CHAR, max_len, &new_data, data + data_len,
        true, slot, NULL);
    // Convert to non-inlined string value for comparison.
    StringValue value(slot, max_len);
    CheckReadResult(expected_val, expected_encoded_len, expected_error, value, success,
        new_data - data);
  }

  void TestReadAvroVarchar(int max_len, uint8_t* data, int64_t data_len,
      StringValue expected_val, int expected_encoded_len,
      TErrorCode::type expected_error = TErrorCode::OK) {
    // Reset parse_status_
    scanner_.parse_status_ = Status::OK();

    uint8_t* new_data = data;
    StringValue slot;
    bool success = scanner_.ReadAvroVarchar(TYPE_VARCHAR, max_len, &new_data,
        data + data_len, true, &slot, NULL);
    CheckReadResult(expected_val, expected_encoded_len, expected_error, slot, success,
        new_data - data);
  }

  void TestReadAvroString(uint8_t* data, int64_t data_len, StringValue expected_val,
      int expected_encoded_len, TErrorCode::type expected_error = TErrorCode::OK) {
    TestReadAvroType(&HdfsAvroScanner::ReadAvroString, TYPE_STRING, data, data_len,
        expected_val, expected_encoded_len, expected_error);
  }


  template<typename T>
  void TestReadAvroDecimal(uint8_t* data, int64_t data_len, DecimalValue<T> expected_val,
      int expected_encoded_len, TErrorCode::type expected_error = TErrorCode::OK) {
    TestReadAvroType(&HdfsAvroScanner::ReadAvroDecimal, sizeof(expected_val), data,
        data_len, expected_val, expected_encoded_len, expected_error);
  }

  void TestInt64Val(int64_t val) {
    uint8_t data[100];
    int len = ReadWriteUtil::PutZLong(val, data);
    DCHECK_GT(len, 0);
    TestReadAvroInt64(data, len, val, len);
    TestReadAvroInt64(data, len + 1, val, len);
    TestReadAvroInt64(data, len - 1, -1, -1, TErrorCode::SCANNER_INVALID_INT);
  }

 protected:
  HdfsAvroScanner scanner_;
};

}

static bool setup_done = false;
int inline SetUp() {
  if (setup_done == true) return 0;
  char** args;
  args = (char**)malloc(sizeof(char**));
  if (!args) return -1;
  args[0] = (char*)malloc(100);
  strcpy(args[0], "/home/dev/incubator-impala/be/build/debug/service/fuzz_target");
  impala::InitCommonRuntime(1, args, false, impala::TestInfo::BE_TEST);
  setup_done = true;
  return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size) {
  //std::cout << "Starting Fuzz target";
  if (SetUp() < 0) return -1;

  uint8_t data[129];
  impala::HdfsAvroScannerTestFuzz t;

  LOG(INFO) << "Size: " << Size;

  data[0] = Size - 1;
  memcpy(&data[1], (char*)Data, Size);
  impala::StringValue sv((char*)Data, Size);
  if (Size == 0) {
    t.TestReadAvroString(const_cast<uint8_t*>(Data), Size, sv, Size, impala::TErrorCode::SCANNER_INVALID_INT);
  } else if (Size == 1) {
    t.TestReadAvroString(const_cast<uint8_t*>(Data), Size, sv, Size, impala::TErrorCode::AVRO_TRUNCATED_BLOCK);
  } else {
    EXPECT_GT(Size, 1);
    t.TestReadAvroString(const_cast<uint8_t*>(Data), Size, sv, Size);
  }
  //FuzzMe(Data, Size);
  return 0;
}
