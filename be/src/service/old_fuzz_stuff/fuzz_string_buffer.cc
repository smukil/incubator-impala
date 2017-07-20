#include "common/init.h"
#include "runtime/test-env.h"

#include "common/names.h"

#include <string>

#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/string-buffer.h"
#include "testutil/gtest-util.h"

namespace impala {

void ValidateString(const string& std_str, const StringBuffer& str) {
  EXPECT_EQ(std_str.empty(), str.IsEmpty());
  EXPECT_EQ(static_cast<int64_t>(std_str.size()), str.len());
  if (std_str.size() > 0) {
    EXPECT_EQ(strncmp(std_str.c_str(), str.buffer(), std_str.size()), 0);
  }
}

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

  impala::MemTracker tracker;
  impala::MemPool pool(&tracker);
  impala::StringBuffer str(&pool);
  std::string std_str;

  std_str.append((char*)Data, Size);
  str.Append(Data, Size);
  impala::ValidateString(std_str, str);

  pool.FreeAll();
  return 0;
}
