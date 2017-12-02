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

#include "rpc/rpc-mgr.h"

#include "exec/kudu-util.h"
#include "kudu/rpc/acceptor_pool.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/service_if.h"
#include "kudu/util/net/net_util.h"
#include "util/cpu-info.h"
#include "util/network-util.h"
#include "kudu/util/jsonwriter.h"

#include "common/names.h"

#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

using kudu::HostPort;
using kudu::MetricEntity;
using kudu::rpc::AcceptorPool;
using kudu::rpc::DumpRunningRpcsRequestPB;
using kudu::rpc::DumpRunningRpcsResponsePB;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::Messenger;
using kudu::rpc::RpcCallInProgressPB;
using kudu::rpc::RpcConnectionPB;
using kudu::rpc::RpcController;
using kudu::rpc::ServiceIf;
using kudu::Sockaddr;

DECLARE_string(hostname);

DEFINE_int32(num_acceptor_threads, 2,
    "Number of threads dedicated to accepting connection requests for RPC services");
DEFINE_int32(num_reactor_threads, 0,
    "Number of threads dedicated to managing network IO for RPC services. If left at "
    "default value 0, it will be set to number of CPU cores.");
DEFINE_int32(rpc_retry_interval_ms, 5,
    "Time in millisecond of waiting before retrying an RPC when remote is busy");

using namespace rapidjson;

namespace impala {

Status RpcMgr::Init() {
  MessengerBuilder bld("impala-server");
  const scoped_refptr<MetricEntity> entity(
      METRIC_ENTITY_server.Instantiate(&registry_, "krpc-metrics"));
  int num_reactor_threads =
      FLAGS_num_reactor_threads > 0 ? FLAGS_num_reactor_threads : CpuInfo::num_cores();
  bld.set_num_reactors(num_reactor_threads).set_metric_entity(entity);
  KUDU_RETURN_IF_ERROR(bld.Build(&messenger_), "Could not build messenger");
  return Status::OK();
}

Status RpcMgr::RegisterService(int32_t num_service_threads, int32_t service_queue_depth,
    unique_ptr<ServiceIf> service_ptr) {
  DCHECK(is_inited()) << "Must call Init() before RegisterService()";
  DCHECK(!services_started_) << "Cannot call RegisterService() after StartServices()";
  scoped_refptr<ImpalaServicePool> service_pool =
      new ImpalaServicePool(std::move(service_ptr),
          messenger_->metric_entity(), service_queue_depth);
  // Start the thread pool first before registering the service in case the startup fails.
  RETURN_IF_ERROR(
      service_pool->Init(num_service_threads));
  KUDU_RETURN_IF_ERROR(
      messenger_->RegisterService(service_pool->service_name(), service_pool),
      "Could not register service");

  VLOG_QUERY << "Registered KRPC service: " << service_pool->service_name();
  service_pools_.push_back(std::move(service_pool));

  return Status::OK();
}

Status RpcMgr::StartServices(const TNetworkAddress& address) {
  DCHECK(is_inited()) << "Must call Init() before StartServices()";
  DCHECK(!services_started_) << "May not call StartServices() twice";

  // Convert 'address' to Kudu's Sockaddr
  DCHECK(IsResolvedAddress(address));
  Sockaddr sockaddr;
  RETURN_IF_ERROR(TNetworkAddressToSockaddr(address, &sockaddr));

  // Call the messenger to create an AcceptorPool for us.
  shared_ptr<AcceptorPool> acceptor_pool;
  KUDU_RETURN_IF_ERROR(messenger_->AddAcceptorPool(sockaddr, &acceptor_pool),
      "Failed to add acceptor pool");
  KUDU_RETURN_IF_ERROR(acceptor_pool->Start(FLAGS_num_acceptor_threads),
      "Acceptor pool failed to start");
  VLOG_QUERY << "Started " << FLAGS_num_acceptor_threads << " acceptor threads";
  services_started_ = true;
  return Status::OK();
}

void RpcMgr::Shutdown() {
  if (messenger_.get() == nullptr) return;
  for (auto service_pool : service_pools_) service_pool->Shutdown();

  messenger_->UnregisterAllServices();
  messenger_->Shutdown();
  service_pools_.clear();
}

bool RpcMgr::IsServerTooBusy(const RpcController& rpc_controller) {
  const kudu::Status status = rpc_controller.status();
  const kudu::rpc::ErrorStatusPB* err = rpc_controller.error_response();
  return status.IsRemoteError() && err != nullptr && err->has_code() &&
      err->code() == kudu::rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY;
}

void MergeObject(Value& target, Value& source, Value::AllocatorType& allocator) {
  for (Value::MemberIterator itr = source.MemberBegin(); itr != source.MemberEnd(); ++itr)
      target.AddMember(itr->name, itr->value, allocator);
}

Status GetInt64MetricFromKrpcJson(
    Document& krpc_document, const char* metric_to_find, int64_t* value_out) {
  DCHECK(value_out != nullptr);

  for (Value::ConstValueIterator itr = krpc_document["metrics"].Begin();
      itr != krpc_document["metrics"].End(); ++itr) {
    if (itr->IsObject()) {
      const char* itr_metric_name = (*itr)["name"].GetString();
      if (strcmp(itr_metric_name, metric_to_find) != 0) continue;

      DCHECK((*itr)["value"].IsInt64());
      *value_out = (*itr)["value"].GetInt64();
      return Status::OK();
    }
  }

  return Status(Substitute("Metric '$0' not found in KRPC Json", metric_to_find));
}

void GetKrpcJsonDocument(scoped_refptr<kudu::MetricEntity> metric_entity,
    Document* document_out) {
  const string HANDLER_LATENCY_METRIC_PREFIX = "handler_latency_impala_";
  const string RPCS_ACCEPTED_METRIC_NAME = "rpc_connections_accepted";

  std::ostringstream krpc_raw_json;
  vector<string> requested_metrics;
  requested_metrics.emplace_back(HANDLER_LATENCY_METRIC_PREFIX);
  requested_metrics.emplace_back(RPCS_ACCEPTED_METRIC_NAME);

  kudu::MetricJsonOptions opts;
  opts.include_raw_histograms = true;
  opts.include_schema_info = true;

  // Obtain the raw Json from KRPC.
  kudu::JsonWriter writer(&krpc_raw_json, kudu::JsonWriter::COMPACT);
  metric_entity->WriteAsJson(&writer, requested_metrics, opts);

  // Convert the output of KRPC's metrics in raw JSON into a rapidjson::Document.
  document_out->Parse<0>(krpc_raw_json.str().c_str());
}

void GetMatchingHistogramMetricsFromKrpcJson(Document& krpc_document,
    const std::vector<string>& matching_strings, Document* document_out,
        Value* array_out) {
  DCHECK(array_out != nullptr);
  DCHECK(array_out->IsArray());

  for (Value::ValueIterator itr = krpc_document["metrics"].Begin();
      itr != krpc_document["metrics"].End(); ++itr) {
    if (itr->IsObject()) {
      // If we've already moved the values out of this element, it will be a Null
      // object, so we skip it.
      if (!(*itr)["type"].IsString()) continue;

      // Only histogram metrics are stored per RPC.
      if (strcmp((*itr)["type"].GetString(), "histogram") != 0) continue;

      // Get the label tagged on the histogram. An example of the label format:
      // "impala.DataStreamService.TransmitData RPC Time"
      const char* histogram_label = (*itr)["label"].GetString();

      int i;
      // Check if the histogram label contains the corresponding 'matching_strings'.
      for (i = 0; i < matching_strings.size(); ++i) {
        if (strstr(histogram_label, matching_strings[i].c_str()) == nullptr) break;
      }

      // If it matches with all the strings, add it to the array.
      if (i == matching_strings.size()) {
        (*array_out).PushBack(*itr, document_out->GetAllocator());
        // TODO: Erase the array entry 'itr' from the document. Our version of rapidjson
        // does not support Erase(), but if we move to a newer version, we should do
        // this.
      }
    }
  }
}

void RpcMgr::ToJson(Document* document) {
  if (messenger_.get() == nullptr) return;

  DumpRunningRpcsResponsePB response;
  messenger_->DumpRunningRpcs(DumpRunningRpcsRequestPB(), &response);

  int32_t num_inbound_calls_in_flight = 0;
  int32_t num_outbound_calls_in_flight = 0;

  for (const RpcConnectionPB& conn : response.inbound_connections()) {
    num_inbound_calls_in_flight += conn.calls_in_flight().size();
  }
  for (const RpcConnectionPB& conn : response.outbound_connections()) {
    num_outbound_calls_in_flight += conn.calls_in_flight().size();
  }

  document->AddMember("num_inbound_calls_in_flight", num_inbound_calls_in_flight,
      document->GetAllocator());
  document->AddMember("num_outbound_calls_in_flight", num_outbound_calls_in_flight,
      document->GetAllocator());

  const string HANDLER_LATENCY_METRIC_PREFIX = "handler_latency_impala_";
  const string RPCS_ACCEPTED_METRIC_NAME = "rpc_connections_accepted";

  // Convert the output of KRPC's metrics in raw JSON into a rapidjson::Document.
  Document krpc_document;
  GetKrpcJsonDocument(metric_entity(), &krpc_document);

/*
  std::ostringstream krpc_raw_json;
  vector<string> requested_metrics;
  requested_metrics.emplace_back(HANDLER_LATENCY_METRIC_PREFIX);
  requested_metrics.emplace_back(RPCS_ACCEPTED_METRIC_NAME);

  kudu::MetricJsonOptions opts;
  opts.include_raw_histograms = true;
  opts.include_schema_info = true;

  kudu::JsonWriter writer(&krpc_raw_json, kudu::JsonWriter::COMPACT);
  metric_entity()->WriteAsJson(&writer, requested_metrics, opts);

  // Convert the output of KRPC's metrics in raw JSON into a rapidjson::Document.
  Document krpc_document;
  krpc_document.Parse<0>(krpc_raw_json.str().c_str());
*/

  //if (krpc_document.HasMember("metrics")) {
  int64_t rpc_connections_accepted = 0;
  if (GetInt64MetricFromKrpcJson(krpc_document,
      RPCS_ACCEPTED_METRIC_NAME.c_str(), &rpc_connections_accepted).ok()) {
    document->AddMember(
        "rpc_connections_accepted", rpc_connections_accepted, document->GetAllocator());
  }

  Value services(kArrayType);
  for (auto service_pool : service_pools_) {
    Value service_entry(kObjectType);

    // Fill in the Service pool's JSON.
    service_pool->ToJson(&service_entry, document);

    // Get the Service Pool's name.
    const char* service_name = service_entry["service_name"].GetString();
    DCHECK(service_name != nullptr);

    // Go through each RPC method belonging to this Service Pool and merge the
    // corresponding RPC's metrics from the KRPC metrics.
    for (Value::ValueIterator method_itr = service_entry["rpc_method_metrics"].Begin();
        method_itr != service_entry["rpc_method_metrics"].End(); ++method_itr) {

      Value per_method_metrics_from_krpc(kArrayType);
      std::vector<string> strings_to_match;
      strings_to_match.push_back(service_name);
      strings_to_match.push_back((*method_itr)["method_name"].GetString());

      GetMatchingHistogramMetricsFromKrpcJson(
          krpc_document, strings_to_match, document, &per_method_metrics_from_krpc);

      // Add all the KRPC metrics found for this RPC method into the Service Pool's JSON
      // structure.
      method_itr->AddMember("per_method_metrics_from_krpc", per_method_metrics_from_krpc,
          document->GetAllocator());
    }
    services.PushBack(service_entry, document->GetAllocator());
  }
  document->AddMember("services", services, document->GetAllocator());
 
}

} // namespace impala
