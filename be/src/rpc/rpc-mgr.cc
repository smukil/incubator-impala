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
#include "kudu/rpc/service_if.h"
#include "kudu/util/net/net_util.h"
#include "util/auth-util.h"
#include "util/cpu-info.h"
#include "util/network-util.h"
#include "util/openssl-util.h"

#include "common/names.h"

using kudu::rpc::MessengerBuilder;
using kudu::rpc::Messenger;
using kudu::rpc::AcceptorPool;
using kudu::rpc::RpcController;
using kudu::rpc::ServiceIf;
using kudu::rpc::ServicePool;
using kudu::Sockaddr;
using kudu::HostPort;
using kudu::MetricEntity;

DECLARE_string(hostname);
DECLARE_string(principal);
DECLARE_string(be_principal);

// Impala's TLS flags.
DECLARE_string(ssl_client_ca_certificate);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_private_key_password_cmd);
DECLARE_string(ssl_cipher_list);
DECLARE_string(ssl_minimum_version);

// KuduRPC's TLS flags
DECLARE_string(rpc_certificate_file);
DECLARE_string(rpc_private_key_file);
DECLARE_string(rpc_ca_certificate_file);
DECLARE_string(rpc_private_key_password_cmd);
DECLARE_string(rpc_tls_ciphers);
DECLARE_string(rpc_tls_min_protocol);

DEFINE_int32(num_acceptor_threads, 2,
    "Number of threads dedicated to accepting connection requests for RPC services");
DEFINE_int32(num_reactor_threads, 0,
    "Number of threads dedicated to managing network IO for RPC services. If left at "
    "default value 0, it will be set to number of CPU cores.");
DEFINE_int32(rpc_retry_interval_ms, 5,
    "Time in millisecond of waiting before retrying an RPC when remote is busy");

namespace impala {

Status RpcMgr::Init() {
  MessengerBuilder bld("impala-server");
  const scoped_refptr<MetricEntity> entity(
      METRIC_ENTITY_server.Instantiate(&registry_, "krpc-metrics"));
  int num_reactor_threads =
      FLAGS_num_reactor_threads > 0 ? FLAGS_num_reactor_threads : CpuInfo::num_cores();
  bld.set_num_reactors(num_reactor_threads).set_metric_entity(entity);

  if (!FLAGS_principal.empty() || !FLAGS_be_principal.empty()) {
    std::string internal_principal;
    RETURN_IF_ERROR(GetInternalKerberosPrincipal(&internal_principal));

    std::string service_name, unused_hostname, unused_realm;
    RETURN_IF_ERROR(DissectKerberosPrincipal(internal_principal, &service_name, &unused_hostname,
        &unused_realm));
    bld.set_sasl_proto_name(service_name);
  }

  if (use_tls_) {
    // Save the original values of the Kudu flags.
    flag_save_ca_certificate_file = FLAGS_rpc_ca_certificate_file;
    flag_save_rpc_private_key_file = FLAGS_rpc_private_key_file;
    flag_save_rpc_certificate_file = FLAGS_rpc_certificate_file;
    flag_save_rpc_private_key_password_cmd = FLAGS_rpc_private_key_password_cmd;
    flag_save_rpc_tls_ciphers = FLAGS_rpc_tls_ciphers;
    flag_save_rpc_tls_min_protocol = FLAGS_rpc_tls_min_protocol;

    // Set the hidden Kudu flags that are TLS related.
    FLAGS_rpc_ca_certificate_file = FLAGS_ssl_client_ca_certificate;
    FLAGS_rpc_private_key_file = FLAGS_ssl_private_key;
    FLAGS_rpc_certificate_file = FLAGS_ssl_server_certificate;
    FLAGS_rpc_private_key_password_cmd = FLAGS_ssl_private_key_password_cmd;
    if (!FLAGS_ssl_cipher_list.empty()) {
      FLAGS_rpc_tls_ciphers = FLAGS_ssl_cipher_list;
    }
    FLAGS_rpc_tls_min_protocol = FLAGS_ssl_minimum_version;

    LOG (INFO) << "Initing RpcMgr with TLS";
    // TODO: Cipher list and password key command.
    bld.enable_inbound_tls();
  }

  KUDU_RETURN_IF_ERROR(bld.Build(&messenger_), "Could not build messenger");
  return Status::OK();
}

Status RpcMgr::RegisterService(int32_t num_service_threads, int32_t service_queue_depth,
    unique_ptr<ServiceIf> service_ptr) {
  DCHECK(is_inited()) << "Must call Init() before RegisterService()";
  DCHECK(!services_started_) << "Cannot call RegisterService() after StartServices()";
  scoped_refptr<ServicePool> service_pool =
      new ServicePool(gscoped_ptr<ServiceIf>(service_ptr.release()),
          messenger_->metric_entity(), service_queue_depth);
  // Start the thread pool first before registering the service in case the startup fails.
  KUDU_RETURN_IF_ERROR(
      service_pool->Init(num_service_threads), "Service pool failed to start");
  KUDU_RETURN_IF_ERROR(
      messenger_->RegisterService(service_pool->service_name(), service_pool),
      "Could not register service");
  service_pools_.push_back(service_pool);
  VLOG_QUERY << "Registered KRPC service: " << service_pool->service_name();
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
  if (use_tls_) {
    // Restore the original values of the TLS related Kudu flags.
    FLAGS_rpc_ca_certificate_file = flag_save_ca_certificate_file;
    FLAGS_rpc_private_key_file = flag_save_rpc_private_key_file;
    FLAGS_rpc_certificate_file = flag_save_rpc_certificate_file;
    FLAGS_rpc_private_key_password_cmd = flag_save_rpc_private_key_password_cmd;
    FLAGS_rpc_tls_ciphers = flag_save_rpc_tls_ciphers;
    FLAGS_rpc_tls_min_protocol = flag_save_rpc_tls_min_protocol;
  }

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

} // namespace impala
