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

#ifndef IMPALA_RPC_RPC_MGR_H
#define IMPALA_RPC_RPC_MGR_H

#include "common/status.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/util/metrics.h"

#include "gen-cpp/Types_types.h"

namespace kudu {
namespace rpc {
class RpcController;
class ServiceIf;
} // rpc
} // kudu

namespace impala {

/// Central manager for all RPC services and proxies.
///
/// SERVICES
/// --------
///
/// An RpcMgr manages 0 or more services: RPC interfaces that are a collection of remotely
/// accessible methods. A new service is registered by calling RegisterService(). All
/// services are served on the same port; the underlying RPC layer takes care of
/// de-multiplexing RPC calls to their respective endpoints.
///
/// Services are made available to remote clients when RpcMgr::StartServices() is called;
/// before this method no service method will be called.
///
/// Services may only be registered and started after RpcMgr::Init() is called.
///
/// PROXIES
/// -------
///
/// A proxy is a client-side interface to a remote service. Remote methods exported by
/// that service may be called through a proxy as though they were local methods.
///
/// A proxy can be obtained by calling GetProxy(). Proxies implement local methods which
/// call remote service methods, e.g. proxy->Foo(request, &response) will call the Foo()
/// service method on the service that 'proxy' points to.
///
/// Proxies may only be created after RpcMgr::Init() is called.
///
/// For example usage of proxies, please see rpc-mgr-test.cc
///
/// LIFECYCLE
/// ---------
///
/// Before any proxy or service interactions, RpcMgr::Init() must be called to start the
/// reactor threads that service network events. Services must be registered with
/// RpcMgr::RegisterService() before RpcMgr::StartServices() is called.
///
/// When shutting down, clients must call Shutdown() to ensure that all services
/// are cleanly terminated.
///
/// KRPC INTERNALS
/// --------------
///
/// Each service and proxy interacts with the network via a shared pool of 'reactor'
/// threads which respond to incoming and outgoing RPC events. The number of 'reactor'
/// threads are configurable via FLAGS_reactor_thread. By default, it's set to the number
/// of cpu cores. Incoming events are passed immediately to one of two thread pools: new
/// connections are handled by an 'acceptor' pool, and RPC request events are handled by
/// a per-service 'service' pool. The size of a 'service' pool is specified when calling
/// RegisterService().
///
/// If the rate of incoming RPC requests exceeds the rate at which those requests are
/// processed, some requests will be placed in a FIFO fixed-size queue. If the queue
/// becomes full, the RPC will fail at the caller. The function IsServerTooBusy() below
/// will return true for this case. The size of the queue is specified when calling
/// RegisterService().
///
/// Inbound connection set-up is handled by a small fixed-size pool of 'acceptor'
/// threads. The number of threads that accept new TCP connection requests to the service
/// port is configurable via FLAGS_acceptor_threads.
class RpcMgr {
 public:
  /// Initializes the reactor threads, and prepares for sending outbound RPC requests.
  Status Init() WARN_UNUSED_RESULT;

  bool is_inited() const { return messenger_.get() != nullptr; }

  /// Start the acceptor threads which listen on 'address', making KRPC services
  /// available. Before this method is called, remote clients will get a 'connection
  /// refused' error when trying to invoke an RPC on this machine.
  Status StartServices(const TNetworkAddress& address) WARN_UNUSED_RESULT;

  /// Register a new service.
  ///
  /// 'num_service_threads' is the number of threads that should be started to execute RPC
  /// handlers for the new service.
  ///
  /// 'service_queue_depth' is the maximum number of requests that may be queued for this
  /// service before clients being to see rejection errors.
  ///
  /// 'service_ptr' contains an interface implementation that will handle RPCs. Note that
  /// the service name has to be unique within an Impala instance or the registration will
  /// fail.
  ///
  /// It is an error to call this after StartServices() has been called.
  Status RegisterService(int32_t num_service_threads, int32_t service_queue_depth,
      std::unique_ptr<kudu::rpc::ServiceIf> service_ptr) WARN_UNUSED_RESULT;

  /// Creates a new proxy for a remote service of type P at location 'address', and places
  /// it in 'proxy'. 'P' must descend from kudu::rpc::ServiceIf. Note that 'address' must
  /// be a resolved IP address.
  template <typename P>
  Status GetProxy(const TNetworkAddress& address, std::unique_ptr<P>* proxy)
      WARN_UNUSED_RESULT;

  /// Shut down all previously registered services. All service pools are shut down.
  /// All acceptor and reactor threads within the messenger are also shut down.
  void Shutdown();

  /// Returns true if the last RPC of 'rpc_controller' failed because the remote
  /// service's queue filled up and couldn't accept more incoming requests.
  /// 'rpc_controller' should contain the status of the last RPC call.
  static bool IsServerTooBusy(const kudu::rpc::RpcController& rpc_controller);

  const scoped_refptr<kudu::rpc::ResultTracker> result_tracker() const {
    return tracker_;
  }

  scoped_refptr<kudu::MetricEntity> metric_entity() const {
    return messenger_->metric_entity();
  }

  std::shared_ptr<kudu::rpc::Messenger> messenger() {
    return messenger_;
  }

  ~RpcMgr() {
    DCHECK_EQ(service_pools_.size(), 0)
        << "Must call Shutdown() before destroying RpcMgr";
  }

 private:
  /// One pool per registered service.
  std::vector<scoped_refptr<kudu::rpc::ServicePool>> service_pools_;

  /// Required Kudu boilerplate for constructing the MetricEntity passed
  /// to c'tor of ServiceIf when creating a service.
  /// TODO(KRPC): Integrate with Impala MetricGroup.
  kudu::MetricRegistry registry_;

  /// Used when creating a new service. Shared across all services which don't really
  /// track results for idempotent RPC calls.
  const scoped_refptr<kudu::rpc::ResultTracker> tracker_;

  /// Container for reactor threads which run event loops for RPC services, plus acceptor
  /// threads which manage connection setup.
  std::shared_ptr<kudu::rpc::Messenger> messenger_;

  /// True after StartServices() completes.
  bool services_started_ = false;
};

} // namespace impala

#endif
