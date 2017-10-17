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

#include "runtime/krpc-data-stream-mgr.h"

#include <iostream>
#include <boost/functional/hash.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/thread.hpp>

#include "kudu/rpc/rpc_context.h"

#include "runtime/krpc-data-stream-recvr.h"
#include "runtime/raw-value.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile-counters.h"
#include "util/uid-util.h"

#include "gen-cpp/data_stream_service.pb.h"

#include "common/names.h"

/// This parameter controls the minimum amount of time a closed stream ID will stay in
/// closed_stream_cache_ before it is evicted. It needs to be set sufficiently high that
/// it will outlive all the calls to FindRecvr() for that stream ID, to distinguish
/// between was-here-but-now-gone and never-here states for the receiver. If the stream ID
/// expires before a call to FindRecvr(), the sender will see an error which will lead to
/// query cancellation. Setting this value higher will increase the size of the stream
/// cache (which is roughly 48 bytes per receiver).
/// TODO: We don't need millisecond precision here.
const int32_t STREAM_EXPIRATION_TIME_MS = 300 * 1000;

DECLARE_bool(use_krpc);
DECLARE_int32(datastream_sender_timeout_ms);

using boost::mutex;

namespace impala {

KrpcDataStreamMgr::KrpcDataStreamMgr(MetricGroup* metrics)
  : deserialize_pool_("data-stream-mgr", "deserialize", 8, 10000,
      boost::bind(&KrpcDataStreamMgr::Deserialize, this, _1, _2)) {
  MetricGroup* dsm_metrics = metrics->GetOrCreateChildGroup("datastream-manager");
  num_senders_waiting_ =
      dsm_metrics->AddGauge<int64_t>("senders-blocked-on-recvr-creation", 0L);
  total_senders_waited_ =
      dsm_metrics->AddCounter<int64_t>("total-senders-blocked-on-recvr-creation", 0L);
  num_senders_timedout_ = dsm_metrics->AddCounter<int64_t>(
      "total-senders-timedout-waiting-for-recvr-creation", 0L);
}

Status KrpcDataStreamMgr::Init() {
  RETURN_IF_ERROR(Thread::Create("krpc-data-stream-mgr", "maintenance",
      [this](){ this->Maintenance(); }, &maintenance_thread_));
  RETURN_IF_ERROR(deserialize_pool_.Init());
  return Status::OK();
}

inline uint32_t KrpcDataStreamMgr::GetHashValue(
    const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
  uint32_t value = RawValue::GetHashValue(&fragment_instance_id.lo, TYPE_BIGINT, 0);
  value = RawValue::GetHashValue(&fragment_instance_id.hi, TYPE_BIGINT, value);
  value = RawValue::GetHashValue(&node_id, TYPE_INT, value);
  return value;
}

shared_ptr<DataStreamRecvrBase> KrpcDataStreamMgr::CreateRecvr(
    RuntimeState* state, const RowDescriptor* row_desc,
    const TUniqueId& finst_id, PlanNodeId dest_node_id, int num_senders,
    int64_t buffer_size, RuntimeProfile* profile, bool is_merging) {

  DCHECK(profile != nullptr);
  VLOG_FILE << "creating receiver for fragment="<< finst_id
            << ", node=" << dest_node_id;
  shared_ptr<KrpcDataStreamRecvr> recvr(
      new KrpcDataStreamRecvr(this, state->instance_mem_tracker(), row_desc,
          finst_id, dest_node_id, num_senders, is_merging, buffer_size, profile));
  size_t hash_value = GetHashValue(finst_id, dest_node_id);
  EarlySendersList early_senders;
  {
    RecvrId recvr_id = make_pair(finst_id, dest_node_id);
    lock_guard<mutex> l(lock_);
    fragment_recvr_set_.insert(recvr_id);
    receiver_map_.insert(make_pair(hash_value, recvr));

    DCHECK(early_senders.waiting_sender_ctxs.empty());
    DCHECK(early_senders.closed_sender_ctxs.empty());
    EarlySendersMap::iterator it = early_senders_map_.find(recvr_id);
    if (it != early_senders_map_.end()) {
       early_senders = move(it->second);
       early_senders_map_.erase(it);
    }
  }

  for (unique_ptr<TransmitDataCtx>& ctx : early_senders.waiting_sender_ctxs) {
     EnqueueRowBatch({recvr->fragment_instance_id(), move(ctx)});
     num_senders_waiting_->Increment(-1);
  }
  for (unique_ptr<EndDataStreamCtx>& ctx : early_senders.closed_sender_ctxs) {
     recvr->RemoveSender(ctx->request->sender_id());
     Status::OK().ToProto(ctx->response->mutable_status());
     ctx->context->RespondSuccess();
     num_senders_waiting_->Increment(-1);
  }
  return recvr;
}

shared_ptr<KrpcDataStreamRecvr> KrpcDataStreamMgr::FindRecvr(
    const TUniqueId& finst_id, PlanNodeId node_id,
    bool* already_unregistered) {
  VLOG_ROW << "looking up fragment_instance_id=" << finst_id
           << ", node=" << node_id;
  *already_unregistered = false;
  RecvrId recvr_id = make_pair(finst_id, node_id);
  if (closed_stream_cache_.find(recvr_id) != closed_stream_cache_.end()) {
    *already_unregistered = true;
    return shared_ptr<KrpcDataStreamRecvr>();
  }

  size_t hash_value = GetHashValue(finst_id, node_id);
  pair<RecvrMap::iterator, RecvrMap::iterator> range =
      receiver_map_.equal_range(hash_value);
  while (range.first != range.second) {
    shared_ptr<KrpcDataStreamRecvr> recvr = range.first->second;
    if (recvr->fragment_instance_id() == finst_id &&
        recvr->dest_node_id() == node_id) {
      return recvr;
    }
    ++range.first;
  }
  return shared_ptr<KrpcDataStreamRecvr>();
}

void KrpcDataStreamMgr::AddEarlySender(const TUniqueId& finst_id,
    const ProtoRowBatch& proto_batch, const TransmitDataRequestPB* request,
    TransmitDataResponsePB* response, kudu::rpc::RpcContext* context) {
  RecvrId recvr_id = make_pair(finst_id, request->dest_node_id());
  auto payload = make_unique<TransmitDataCtx>(proto_batch, context, request, response);
  early_senders_map_[recvr_id].waiting_sender_ctxs.push_back(move(payload));
  num_senders_waiting_->Increment(1);
  total_senders_waited_->Increment(1);
}

void KrpcDataStreamMgr::AddData(const TUniqueId& finst_id,
    ProtoRowBatch& proto_batch, const TransmitDataRequestPB* request,
    TransmitDataResponsePB* response, kudu::rpc::RpcContext* context) {
  TPlanNodeId dest_node_id = request->dest_node_id();
  VLOG_ROW << "AddData(): fragment_instance_id=" << finst_id
           << " node=" << dest_node_id << " size="
           << RowBatch::GetSerializedSize(proto_batch);
  bool already_unregistered = false;
  shared_ptr<KrpcDataStreamRecvr> recvr;
  {
    lock_guard<mutex> l(lock_);
    recvr = FindRecvr(finst_id, request->dest_node_id(), &already_unregistered);
    // If no receiver is found and it's not in the closed stream cache, best guess is
    // that it is still preparing, so add payload to per-receiver list.
    if (!already_unregistered && recvr == nullptr) {
      AddEarlySender(finst_id, proto_batch, request, response, context);
      return;
    }
  }
  if (already_unregistered) {
    // The receiver may remove itself from the receiver map via DeregisterRecvr() at any
    // time without considering the remaining number of senders.  As a consequence,
    // FindRecvr() may return nullptr even though the receiver was once present. We
    // detect this case by checking already_unregistered - if true then the receiver was
    // already closed deliberately, and there's no unexpected error here.
    Status(TErrorCode::DATASTREAM_RECVR_CLOSED, PrintId(finst_id), dest_node_id)
        .ToProto(response->mutable_status());
    context->RespondSuccess();
    return;
  }

  // Don't hold lock.
  DCHECK(recvr.get() != nullptr);
  recvr->AddBatch(proto_batch, request, response, context);
}

void KrpcDataStreamMgr::EnqueueRowBatch(DeserializeWorkItem&& payload) {
  deserialize_pool_.Offer(move(payload));
}

void KrpcDataStreamMgr::Deserialize(int thread_id, const DeserializeWorkItem& workitem) {
  AddData(workitem.fragment_instance_id, workitem.ctx->proto_batch, workitem.ctx->request,
      workitem.ctx->response, workitem.ctx->context);
}

void KrpcDataStreamMgr::AddEarlyClosedSender(const TUniqueId& finst_id,
    const EndDataStreamRequestPB* request, EndDataStreamResponsePB* response,
    kudu::rpc::RpcContext* context) {
  RecvrId recvr_id = make_pair(finst_id, request->dest_node_id());
  auto payload = make_unique<EndDataStreamCtx>(context, request, response);
  early_senders_map_[recvr_id].closed_sender_ctxs.emplace_back(move(payload));
  num_senders_waiting_->Increment(1);
  total_senders_waited_->Increment(1);
}

void KrpcDataStreamMgr::CloseSender(const TUniqueId& finst_id,
    const EndDataStreamRequestPB* request, EndDataStreamResponsePB* response,
    kudu::rpc::RpcContext* context) {
  VLOG_FILE << "CloseSender(): fragment_instance_id=" << finst_id
            << ", node=" << request->dest_node_id();
  shared_ptr<KrpcDataStreamRecvr> recvr;
  {
    lock_guard<mutex> l(lock_);
    bool already_unregistered;
    recvr = FindRecvr(finst_id, request->dest_node_id(), &already_unregistered);
    // If no receiver is found and it's not in the closed stream cache, we still need
    // to make sure that the close operation is performed so add to per-recvr list of
    // pending closes. It's possible for a sender to issue EOS RPC without sending any
    // rows if no rows are materialized at all in the sender side.
    if (!already_unregistered && recvr == nullptr) {
      AddEarlyClosedSender(finst_id, request, response, context);
      return;
    }
  }

  // If we reach this point, either the receiver is found or it has been unregistered
  // already. In either cases, it's safe to just return an OK status.
  if (LIKELY(recvr != nullptr)) recvr->RemoveSender(request->sender_id());
  Status::OK().ToProto(response->mutable_status());
  context->RespondSuccess();

  {
    // Remove any closed streams that have been in the cache for more than
    // STREAM_EXPIRATION_TIME_MS.
    lock_guard<mutex> l(lock_);
    ClosedStreamMap::iterator it = closed_stream_expirations_.begin();
    int64_t now = MonotonicMillis();
    int32_t before = closed_stream_cache_.size();
    while (it != closed_stream_expirations_.end() && it->first < now) {
      closed_stream_cache_.erase(it->second);
      closed_stream_expirations_.erase(it++);
    }
    DCHECK_EQ(closed_stream_cache_.size(), closed_stream_expirations_.size());
    int32_t after = closed_stream_cache_.size();
    if (before != after) {
      VLOG_QUERY << "Reduced stream ID cache from " << before << " items, to " << after
                 << ", eviction took: "
                 << PrettyPrinter::Print(MonotonicMillis() - now, TUnit::TIME_MS);
    }
  }
}

Status KrpcDataStreamMgr::DeregisterRecvr(
    const TUniqueId& finst_id, PlanNodeId node_id) {
  VLOG_QUERY << "DeregisterRecvr(): fragment_instance_id=" << finst_id
             << ", node=" << node_id;
  size_t hash_value = GetHashValue(finst_id, node_id);
  lock_guard<mutex> l(lock_);
  pair<RecvrMap::iterator, RecvrMap::iterator> range =
      receiver_map_.equal_range(hash_value);
  while (range.first != range.second) {
    const shared_ptr<KrpcDataStreamRecvr>& recvr = range.first->second;
    if (recvr->fragment_instance_id() == finst_id &&
        recvr->dest_node_id() == node_id) {
      // Notify concurrent AddData() requests that the stream has been terminated.
      recvr->CancelStream();
      RecvrId recvr_id =
          make_pair(recvr->fragment_instance_id(), recvr->dest_node_id());
      fragment_recvr_set_.erase(recvr_id);
      receiver_map_.erase(range.first);
      closed_stream_expirations_.insert(
          make_pair(MonotonicMillis() + STREAM_EXPIRATION_TIME_MS, recvr_id));
      closed_stream_cache_.insert(recvr_id);
      return Status::OK();
    }
    ++range.first;
  }

  const string msg = Substitute(
      "Unknown row receiver id: fragment_instance_id=$0, node_id=$1",
      PrintId(finst_id), node_id);
  LOG(ERROR) << msg;
  return Status(msg);
}

void KrpcDataStreamMgr::Cancel(const TUniqueId& finst_id) {
  VLOG_QUERY << "cancelling all streams for fragment=" << finst_id;
  lock_guard<mutex> l(lock_);
  FragmentRecvrSet::iterator i =
      fragment_recvr_set_.lower_bound(make_pair(finst_id, 0));
  while (i != fragment_recvr_set_.end() && i->first == finst_id) {
    bool unused;
    shared_ptr<KrpcDataStreamRecvr> recvr = FindRecvr(i->first, i->second, &unused);
    if (recvr != nullptr) {
      recvr->CancelStream();
    } else {
      // keep going but at least log it
      LOG(ERROR) << Substitute("Cancel(): missing in stream_map: fragment=$0 node=$1",
          PrintId(i->first), i->second);
    }
    ++i;
  }
}

template<typename ContextType, typename RequestPBType>
void KrpcDataStreamMgr::HandleTimedOutSenders(const std::unique_ptr<ContextType>& ctx) {
   const RequestPBType* request = ctx->request;
   TUniqueId finst_id;
   finst_id.__set_lo(request->dest_fragment_instance_id().lo());
   finst_id.__set_hi(request->dest_fragment_instance_id().hi());

   Status(TErrorCode::DATASTREAM_SENDER_TIMEOUT, PrintId(finst_id)).ToProto(
       ctx->response->mutable_status());
   ctx->context->RespondSuccess();
   num_senders_waiting_->Increment(-1);
   num_senders_timedout_->Increment(1);
}

void KrpcDataStreamMgr::Maintenance() {
  while (true) {
    // Notify any senders that have been waiting too long for their receiver to
    // appear. Keep lock_ held for only a short amount of time.
    vector<EarlySendersList> timed_out_senders;
    {
      int64_t now = MonotonicMillis();
      lock_guard<mutex> l(lock_);
      auto it = early_senders_map_.begin();
      while (it != early_senders_map_.end()) {
        if (now - it->second.arrival_time > FLAGS_datastream_sender_timeout_ms) {
          timed_out_senders.emplace_back(move(it->second));
          it = early_senders_map_.erase(it);
        } else {
          ++it;
        }
      }
    }

    // Send response to all timed-out senders. We need to do so even for senders
    // which sent EOS RPC. Otherwise, the receiver may hang when it eventually
    // gets created as num_remaining_senders_ will be larger than number of
    // outstanding senders.
    for (const EarlySendersList& senders_list : timed_out_senders) {
      for (const auto& ctx : senders_list.waiting_sender_ctxs) {
        HandleTimedOutSenders<TransmitDataCtx, TransmitDataRequestPB>(ctx);
      }
      for (const auto& ctx : senders_list.closed_sender_ctxs) {
        HandleTimedOutSenders<EndDataStreamCtx, EndDataStreamRequestPB>(ctx);
      }
    }

    bool timed_out = false;
    // Wait for 10s
    shutdown_promise_.Get(10000, &timed_out);
    if (!timed_out) return;
  }
}

KrpcDataStreamMgr::~KrpcDataStreamMgr() {
  shutdown_promise_.Set(true);
  deserialize_pool_.Shutdown();
  LOG(INFO) << "Waiting for data-stream-mgr maintenance thread...";
  maintenance_thread_->Join();
  LOG(INFO) << "Waiting for deserialization thread pool...";
  deserialize_pool_.Join();
}

} // namespace impala
