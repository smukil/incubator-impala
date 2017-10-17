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

#include "service/data-stream-service.h"

#include "common/status.h"
#include "exec/kudu-util.h"
#include "kudu/rpc/rpc_context.h"
#include "rpc/rpc-mgr.h"
#include "runtime/krpc-data-stream-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/row-batch.h"
#include "testutil/fault-injection-util.h"

#include "gen-cpp/data_stream_service.pb.h"

#include "common/names.h"

using kudu::rpc::RpcContext;

namespace impala {

DataStreamService::DataStreamService(RpcMgr* mgr)
  : DataStreamServiceIf(mgr->metric_entity(), mgr->result_tracker()) {}

void DataStreamService::EndDataStream(const EndDataStreamRequestPB* request,
    EndDataStreamResponsePB* response, RpcContext* context) {
  TUniqueId finst_id;
  finst_id.__set_lo(request->dest_fragment_instance_id().lo());
  finst_id.__set_hi(request->dest_fragment_instance_id().hi());

  VLOG_ROW << "EndDataStream(): instance_id=" << PrintId(finst_id)
           << " node_id=" << request->dest_node_id()
           << " sender_id=" << request->sender_id();
  ExecEnv::GetInstance()->KrpcStreamMgr()->CloseSender(finst_id, request,
      response, context);
}

void DataStreamService::TransmitData(const TransmitDataRequestPB* request,
    TransmitDataResponsePB* response, RpcContext* context) {
  FAULT_INJECTION_RPC_DELAY(RPC_TRANSMITDATA);
  TUniqueId finst_id;
  finst_id.__set_lo(request->dest_fragment_instance_id().lo());
  finst_id.__set_hi(request->dest_fragment_instance_id().hi());

  VLOG_ROW << "TransmitData(): instance_id=" << finst_id
           << " node_id=" << request->dest_node_id()
           << " #rows=" << request->row_batch_header().num_rows()
           << " sender_id=" << request->sender_id();
  ProtoRowBatch batch;
  Status status = FromKuduStatus(context->GetInboundSidecar(
      request->row_batch_header().tuple_data_sidecar_idx(), &batch.tuple_data));
  if (status.ok()) {
    status = FromKuduStatus(context->GetInboundSidecar(
        request->row_batch_header().tuple_offsets_sidecar_idx(), &batch.tuple_offsets));
  }
  if (status.ok()) {
    batch.header = request->row_batch_header();
    // AddData() is guaranteed to eventually respond to this RPC so we don't do it here.
    batch.times_deferred = 0;
    ExecEnv::GetInstance()->KrpcStreamMgr()->AddData(finst_id, batch, request,
        response, context);
  } else {
    // An application-level error occurred, so return 'success', but set the error status.
    status.ToProto(response->mutable_status());
    context->RespondSuccess();
  }
}

}
