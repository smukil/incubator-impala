// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>

#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "runtime/query-exec-mgr.h"
#include "scheduling/request-pool-service.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/metrics.h"
#include "util/pretty-printer.h"
#include "util/stopwatch.h"
#include "util/thread.h"
#include "util/uid-util.h"

#include "common/init.h"
#include "common/names.h"

#define NUM_QUERIES 100
#define NUM_ACCESSES 10

using boost::uuids::random_generator;

using namespace impala;

boost::scoped_ptr<MetricGroup> metrics_;
boost::scoped_ptr<RequestPoolService> request_pool_service_;
boost::scoped_ptr<QueryExecMgr> query_exec_mgr_;
vector<TUniqueId> query_ids;

void CreateAndAccessQuerys(const TUniqueId& query_id, int num_accesses) {
  QueryState* qs;
  TQueryCtx query_ctx;
  query_ctx.query_id = query_id;

  string resolved_pool;
  Status s = request_pool_service_->ResolveRequestPool(
      query_ctx, &resolved_pool);
  
  query_ctx.__set_request_pool(resolved_pool);

  //LOG (INFO) << "resolved_pool: " << resolved_pool;
  //for (int i=0 ; i < num_accesses; ++i) {
    qs = query_exec_mgr_->CreateQueryState(query_ctx);
    DCHECK(qs != nullptr);
  //}
  LOG (INFO) << "QS: " << qs;
}

// Runs N Impala Threads
void ImpalaThreadStarter(int num_threads) {
  vector<unique_ptr<Thread>> threads;
  threads.reserve(num_threads);

  for (int i=0; i < num_threads; ++i) {
    query_ids[i] = UuidToQueryId(random_generator()());
  }

  for (int i=0; i < num_threads; ++i) {
    unique_ptr<Thread> thread;
    function<void ()> f =
        bind(CreateAndAccessQuerys, query_ids[i], NUM_ACCESSES);
    Status s =
        Thread::Create("mythreadgroup", "thread", f, &thread);
    DCHECK(s.ok());
    threads.push_back(move(thread));
  }
  for (unique_ptr<Thread>& thread: threads) {
    thread->Join();
  }
}

int main(int argc, char **argv) {
  impala::InitCommonRuntime(argc, argv, false, impala::TestInfo::BE_TEST);

  query_ids.reserve(NUM_QUERIES);

  metrics_.reset(new MetricGroup("test-metrics"));
  request_pool_service_.reset(new RequestPoolService(metrics_.get()));
  query_exec_mgr_.reset(new QueryExecMgr());

  StopWatch total_time;
  total_time.Start();
  ImpalaThreadStarter(NUM_QUERIES);
  total_time.Stop();

  cout << "Total time: "
       << PrettyPrinter::Print(total_time.ElapsedTime(), TUnit::CPU_TICKS) << endl;

  return 0;
}
