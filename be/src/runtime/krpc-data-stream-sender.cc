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

#include "runtime/krpc-data-stream-sender.h"

#include <boost/bind.hpp>

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <thrift/protocol/TDebugProtocol.h>

#include "common/logging.h"
#include "exec/kudu-util.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "gutil/strings/substitute.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "rpc/rpc-mgr.inline.h"
#include "runtime/descriptors.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "util/aligned-new.h"
#include "util/debug-util.h"
#include "util/network-util.h"

#include "gen-cpp/data_stream_service.pb.h"
#include "gen-cpp/data_stream_service.proxy.h"
#include "gen-cpp/Types_types.h"

#include "common/names.h"

using std::chrono::milliseconds;
using std::chrono::system_clock;
using std::condition_variable_any;
using namespace apache::thrift;
using kudu::rpc::RpcController;
using kudu::rpc::RpcSidecar;
using kudu::MonoDelta;

DECLARE_int32(rpc_retry_interval_ms);

namespace impala {

// A datastream sender may send row batches to multiple destinations. There is one
// channel for each destination.
//
// Clients can call SendBatch() to directly send a row batch to the destination or it
// can call AddRow() to accumulate rows in an internal row batch to certain capacity
// before sending it. The underlying RPC layer is implemented with KRPC, which provides
// interfaces for asynchronous RPC calls. Normally, the calls above will return before
// the RPC has completed but they may block if there is already an in-flight RPC.
//
// Each channel internally has two cached protobuf row batches to serialize to. This
// allows client to serialize the next row batch while the current row batch is being
// sent. Upon completion of a RPC, the callback TransmitDataCompleteCb() will be invoked.
// If the RPC fails due to remote service's queue being full, TransmitDataCompleteCb()
// will schedule the retry callback RetryCb() after some delay dervied from
// 'FLAGS_rpc_retry_internal_ms'.
//
// When a data stream sender is shut down, it will call TearDown() on all channels to
// release resources. TearDown() will cancel any in-flight RPC and wait for the
// completion callback to be called before returning. It's expected that the execution
// thread to call FlushAndSendEos() before closing the data stream sender to flush all
// buffered row batches and send the end-of-stream message to the remote receiver.
// Note that the protobuf row batches are owned solely by the channel and the KRPC layer
// will relinquish references of them before the completion callback is invoked so it's
// safe to free them once the callback has been invoked.
class KrpcDataStreamSender::Channel : public CacheLineAligned {
 public:
  // Create a channel to send data to particular ipaddress/port/query/node
  // combination. buffer_size is specified in bytes and a soft limit on
  // how much tuple data is getting accumulated before being sent; it only applies
  // when data is added via AddRow() and not sent directly via SendBatch().
  Channel(KrpcDataStreamSender* parent, const RowDescriptor* row_desc,
      const TNetworkAddress& destination, const TUniqueId& fragment_instance_id,
      PlanNodeId dest_node_id, int buffer_size)
    : parent_(parent),
      buffer_size_(buffer_size),
      row_desc_(row_desc),
      address_(destination),
      fragment_instance_id_(fragment_instance_id),
      dest_node_id_(dest_node_id) {
    DCHECK(IsResolvedAddress(address_));
  }

  // Initialize channel.
  // Returns OK if successful, error indication otherwise.
  Status Init(RuntimeState* state) WARN_UNUSED_RESULT;

  // Serialize the given row batch and send it to the destination. If the preceding
  // RPC is in progress, this function may block until the previous RPC finishes.
  // Return error status if serialization or the preceding RPC failed. Return OK
  // otherwise.
  Status SendBatch(RowBatch* batch) WARN_UNUSED_RESULT;

  // Copies a single row into this channel's row batch and flushes the row batch once
  // it reaches capacity. This call may block if the row batch's capacity is reached
  // and the preceding RPC is still in progress. Returns error status if serialization
  // failed or if the preceding RPC failed. Return OK otherwise.
  Status AddRow(TupleRow* row) WARN_UNUSED_RESULT;

  // Shutdown the RPC thread and free the row batch allocation. Any in-flight RPC will
  // be cancelled. It's expected that clients normally call FlushAndSendEos() before
  // calling Teardown() to flush all buffered row batches to destinations. Teardown()
  // may be called without FlushAndSendEos() in cases such as cancellation or error.
  void Teardown(RuntimeState* state);

  // Flushes any buffered row batches and sends the EOS RPC to close the channel.
  Status FlushAndSendEos(RuntimeState* state) WARN_UNUSED_RESULT;

  int64_t num_data_bytes_sent() const { return num_data_bytes_sent_; }

  typedef boost::function<Status()> DoRpcFn;

 private:
  KrpcDataStreamSender* parent_;
  int buffer_size_;
  const RowDescriptor* row_desc_;
  const TNetworkAddress address_;
  TUniqueId fragment_instance_id_;
  PlanNodeId dest_node_id_;

  // Reference to the parent datastream sender's runtime state. Not owned.
  RuntimeState* runtime_state_ = nullptr;

  // Number of bytes of all serialized row batches sent successfully.
  int64_t num_data_bytes_sent_ = 0;

  // The row batch for accumulating rows copied from AddRow().
  scoped_ptr<RowBatch> batch_;

  // The two cached protobuf row batches. Each entry contains a ProtoRowBatch and the
  // buffer for the serialized row batches. When one is used for the in-flight RPC,
  // the execution thread can continue to run and serialize another row batch to the
  // other entry. 'cur_batch_idx_' is the index of the entry being used by the in-flight
  // or last completed RPC.
  static const int num_cached_proto_batches_ = 2;
  CachedProtoRowBatch cached_proto_batches_[num_cached_proto_batches_];

  // This is expected to be read and written by the main execution thread. The KRPC
  // reactor thread may read it so 'lock_' needs to be held when reading it from reactor
  // thread and writing it in main execution thread. 'lock_' is not needed when reading it
  // in main execution thread.
  int cur_batch_idx_ = 0;

  // Synchronize accesses to the following fields between the main execution thread and
  // the KRPC reactor thread. Note that there should be only one reactor thread invoking
  // the callbacks for a channel so there should be no races between multiple reactor
  // threads. Protect all subsequent fields.
  SpinLock lock_;

  // 'lock_' needs to be held when accessing the following fields.
  std::unique_ptr<DataStreamServiceProxy> proxy;
  RpcController rpc_controller_;

  // Protobuf request and response buffer for TransmitData() RPC.
  TransmitDataRequestPB req_;
  TransmitDataResponsePB resp_;

  // Protobuf request and response buffer for EndDataStream() RPC.
  EndDataStreamResponsePB eos_resp_;
  EndDataStreamRequestPB eos_req_;

  // Signaled when 'rpc_in_flight_' changes from true to false.
  condition_variable_any rpc_done_cv_;

  // True if there is an RPC in progress.
  bool rpc_in_flight_ = false;

  // True if the channel is being shut down or shut down already.
  bool shutdown_ = false;

  // True if the remote receiver is closed already. In which case, all rows would
  // be dropped silently.
  bool remote_recvr_closed_ = false;

  // Status of the most recent completed RPC.
  Status rpc_status_;

  // Returns true if the channel should terminate because the parent sender
  // has been closed or cancelled.
  bool ShouldTerminate() const {
    return shutdown_ || parent_->state_->is_cancelled();
  }

  // Returns pointer to the current serialized row batch.
  ProtoRowBatch* CurrentProtoBatch() {
    DCHECK_GE(cur_batch_idx_, 0);
    DCHECK_LT(cur_batch_idx_, num_cached_proto_batches_);
    return cached_proto_batches_[cur_batch_idx_].proto_batch();
  }

  // Returns the index of spare entry to use in 'cached_proto_batches_' for
  // the next TransmitData() RPC.
  int NextProtoBatchIdx() const {
    return (cur_batch_idx_ + 1) % 2;
  }

  // Send the rows accumulated in the internal row batch. This will serialize the
  // internal row batch before sending them to the destination. This may block if
  // the preceding RPC is still in progress. Returns error status if serialization
  // fails or if the preceding RPC fails.
  Status SendCurrentBatch() WARN_UNUSED_RESULT;

  // Called when a KRPC call failed. If it turns out that the RPC failed because the
  // remote server is too busy, this function will schedule RetryCb() to be called
  // after FLAGS_rpc_retry_interval_ms milliseconds, which in turn re-invoke the RPC.
  // Otherwise, it will call MarkDone() to mark the RPC as done and failed.
  // 'controller_status' is a Kudu status returned from the KRPC layer.
  // 'rpc_fn' is a worker function which initializes the RPC parameters and invokes
  // the actual RPC when the RPC is rescheduled.
  // 'err_msg' is an error message to be prepeneded to the status converted from the
  // Kudu status 'controller_status'.
  void HandleFailedRPC(const DoRpcFn& rpc_fn, const kudu::Status& controller_status,
      const string& err_msg);

  // Waits for the preceding RPC to complete. Expects to be called with 'lock_' held.
  // May drop the lock while waiting for the RPC to complete. Return error status if
  // the preceding RPC fails. Returns OK otherwise.
  Status WaitForRpc(std::unique_lock<SpinLock>* lock) WARN_UNUSED_RESULT;

  // Transmits the next serialized row batch stored in 'cached_proto_batches_'.
  // It's expected that the caller has already serialized the row batch to
  // cached_proto_batches_[NextProtoBatchIdx()] and waited for any in-flight RPC to
  // complete before calling this function. This is expected to be called only from the
  // fragment instance execution thread. Return error status if initialization of the
  // RPC request parameters failed. Returns OK otherwise.
  Status TransmitData() WARN_UNUSED_RESULT;

  // A callback function called from KRPC reactor thread to retry an RPC which failed
  // previously due to remote server being too busy. This will re-arm the request
  // parameters of the RPC. The retry may not happen if the callback has been aborted
  // internally by KRPC stack or if the parent sender has been cancelled or closed since
  // the scheduling of this callback. In which case, MarkDone() will be called with the
  // error status and the RPC is considered complete. 'status' is the error status passed
  // by KRPC stack in case the callback was aborted.
  void RetryCb(DoRpcFn rpc_fn, const kudu::Status& status);

  // A callback function called from KRPC reactor thread upon completion of an in-flight
  // TransmitData() RPC. This is called when the remote server responds to the RPC or
  // when the RPC ends prematurely due to various reasons (e.g. cancellation). Upon a
  // successful KRPC call, MarkDone() is called to update 'rpc_status_' based on the
  // response. HandleFailedRPC() is called to handle failed KRPC call. The RPC may be
  // rescheduled if it's due to remote server being too busy.
  void TransmitDataCompleteCb();

  // Initializes the parameters for TransmitData() RPC and invokes the async RPC call.
  // It will add 'tuple_offsets_' and 'tuple_data_' in the current cached_proto_batches_
  // entry as sidecars to the KRPC RpcController and store the sidecars' indices to the
  // header in the serialized ProtoRowBatch to be sent. Returns error status if adding
  // sidecars to the RpcController failed.
  Status DoTransmitDataRpc() WARN_UNUSED_RESULT;

  // A callback function called from KRPC reactor thread upon completion of an in-flight
  // EndDataStream() RPC. This is called when the remote server responds to the RPC or
  // when the RPC ends prematurely due to various reasons (e.g. cancellation). Upon a
  // successful KRPC call, MarkDone() is called to update 'rpc_status_' based on the
  // response. HandleFailedRPC() is called to handle failed KRPC calls. The RPC may be
  // rescheduled if it's due to remote server being too busy.
  void EndDataStreamCompleteCb();

  // Initializes the parameters for EndDataStream() RPC and invokes the async RPC call.
  Status DoEndDataStreamRpc();

  // Marks the in-flight RPC as completed, updates 'rpc_status_' with the status of the
  // RPC (indicated in parameter 'status') and notifies any thread waiting for RPC
  // completion. Expects to be called with 'lock_' held. In debug builds, also clears
  // the header of the current serialized row batch.
  void MarkDone(const Status& status);
};

Status KrpcDataStreamSender::Channel::Init(RuntimeState* state) {
  runtime_state_ = state;

  // TODO: take into account of var-len data at runtime.
  int capacity = max(1, buffer_size_ / max(row_desc_->GetRowSize(), 1));
  batch_.reset(new RowBatch(row_desc_, capacity, parent_->mem_tracker()));

  // Create a DataStreamService proxy to the destination.
  RpcMgr* rpc_mgr = ExecEnv::GetInstance()->rpc_mgr();
  RETURN_IF_ERROR(rpc_mgr->GetProxy(address_, &proxy));

  // Initialize some constant fields in the request protobuf.
  req_.Clear();
  UniqueIdPB* finstance_id_pb = req_.mutable_dest_fragment_instance_id();
  finstance_id_pb->set_lo(fragment_instance_id_.lo);
  finstance_id_pb->set_hi(fragment_instance_id_.hi);
  req_.set_sender_id(parent_->sender_id_);
  req_.set_dest_node_id(dest_node_id_);

  return Status::OK();
}

void KrpcDataStreamSender::Channel::MarkDone(const Status& status) {
#ifndef NDEBUG
  // Clear the header to indicate the proto-batch is not in use. Used to verify that
  // proto-batch is not re-used while RPC is still in-flight.
  CurrentProtoBatch()->header.Clear();
#endif
  rpc_status_ = status;
  rpc_in_flight_ = false;
  rpc_done_cv_.notify_one();
}

Status KrpcDataStreamSender::Channel::WaitForRpc(std::unique_lock<SpinLock>* lock) {
  DCHECK(lock != nullptr);
  DCHECK(lock->owns_lock());

  SCOPED_TIMER(parent_->state_->total_network_send_timer());

  // Wait for in-flight RPCs to complete unless the parent sender is closed or cancelled.
  auto pred = [this]() -> bool { return !rpc_in_flight_ || ShouldTerminate(); };
  auto timeout = std::chrono::system_clock::now() + milliseconds(50);
  while (!rpc_done_cv_.wait_until(*lock, timeout, pred)) {
    timeout = system_clock::now() + milliseconds(50);
  }
  if (UNLIKELY(ShouldTerminate())) {
    // DSS is single-threaded so it's impossible for shutdown_ to be true here.
    DCHECK(!shutdown_);
    return Status::CANCELLED;
  }

  DCHECK(!rpc_in_flight_);
  if (UNLIKELY(!rpc_status_.ok())) {
    LOG(ERROR) << "channel send status: " << rpc_status_.GetDetail();
    return rpc_status_;
  }
  return Status::OK();
}

void KrpcDataStreamSender::Channel::RetryCb(
    DoRpcFn rpc_fn, const kudu::Status& cb_status) {
  COUNTER_ADD(parent_->rpc_retry_counter_, 1);
  std::unique_lock<SpinLock> l(lock_);
  DCHECK(rpc_in_flight_);
  // Aborted by KRPC layer.
  if (UNLIKELY(!cb_status.ok())) {
    MarkDone(FromKuduStatus(cb_status));
    return;
  }
  // Parent datastream sender has been closed or cancelled.
  if (UNLIKELY(ShouldTerminate())) {
    MarkDone(Status::CANCELLED);
    return;
  }
  // Retry the RPC.
  Status status = rpc_fn();
  if (UNLIKELY(!status.ok())) {
    MarkDone(status);
  }
}

void KrpcDataStreamSender::Channel::HandleFailedRPC(const DoRpcFn& rpc_fn,
    const kudu::Status& controller_status, const string& err_msg) {
  // Retrying later if the destination is busy.
  if (RpcMgr::IsServerTooBusy(rpc_controller_) && !ShouldTerminate()) {
    RpcMgr* rpc_mgr = ExecEnv::GetInstance()->rpc_mgr();
    rpc_mgr->messenger()->ScheduleOnReactor(
        boost::bind(&KrpcDataStreamSender::Channel::RetryCb, this, rpc_fn, _1),
        MonoDelta::FromMilliseconds(FLAGS_rpc_retry_interval_ms));
    return;
  }
  MarkDone(FromKuduStatus(controller_status, err_msg));
}

void KrpcDataStreamSender::Channel::TransmitDataCompleteCb() {
  std::unique_lock<SpinLock> l(lock_);
  DCHECK(rpc_in_flight_);
  const kudu::Status controller_status = rpc_controller_.status();
  if (LIKELY(controller_status.ok())) {
    Status rpc_status;
    int32_t status_code = resp_.status().status_code();
    if (LIKELY(status_code == TErrorCode::OK)) {
      ProtoRowBatch* proto_batch = CurrentProtoBatch();
      num_data_bytes_sent_ += RowBatch::GetSerializedSize(*proto_batch);
      VLOG_ROW << "incremented #data_bytes_sent=" << num_data_bytes_sent_;
    } else if (status_code == TErrorCode::DATASTREAM_RECVR_CLOSED) {
      remote_recvr_closed_ = true;
    } else {
      rpc_status = Status(resp_.status());
    }
    MarkDone(rpc_status);
  } else {
    DoRpcFn rpc_fn =
        boost::bind(&KrpcDataStreamSender::Channel::DoTransmitDataRpc, this);
    string err_msg =
        Substitute("TransmitData() to $0 failed", TNetworkAddressToString(address_));
    HandleFailedRPC(rpc_fn, controller_status, err_msg);
  }
}

Status KrpcDataStreamSender::Channel::DoTransmitDataRpc() {
  ProtoRowBatch* proto_batch = CurrentProtoBatch();
  DCHECK(proto_batch->IsInitialized());

  rpc_controller_.Reset();
  // Add 'tuple_offsets_' as sidecar.
  int sidecar_idx;
  KUDU_RETURN_IF_ERROR(rpc_controller_.AddOutboundSidecar(
      RpcSidecar::FromSlice(proto_batch->tuple_offsets), &sidecar_idx),
      "Unable to add tuple offsets to sidecar");
  proto_batch->header.set_tuple_offsets_sidecar_idx(sidecar_idx);

  // Add 'tuple_data_' as sidecar.
  KUDU_RETURN_IF_ERROR(rpc_controller_.AddOutboundSidecar(
      RpcSidecar::FromSlice(proto_batch->tuple_data), &sidecar_idx),
      "Unable to add tuple data to sidecar");
  proto_batch->header.set_tuple_data_sidecar_idx(sidecar_idx);

  // Set the RowBatchHeader in the request.
  req_.release_row_batch_header();
  req_.set_allocated_row_batch_header(&proto_batch->header);

  proxy->TransmitDataAsync(req_, &resp_, &rpc_controller_,
      boost::bind(&KrpcDataStreamSender::Channel::TransmitDataCompleteCb, this));
  return Status::OK();
}

Status KrpcDataStreamSender::Channel::TransmitData() {
  std::unique_lock<SpinLock> l(lock_);
  RETURN_IF_ERROR(WaitForRpc(&l));
  DCHECK(!rpc_in_flight_);
  DCHECK(!CurrentProtoBatch()->IsInitialized());
  // If the remote receiver is closed already, there is no point in sending anything.
  // TODO: IMPALA-3990: propagate this information to the caller.
  if (UNLIKELY(remote_recvr_closed_)) return Status::OK();
  rpc_in_flight_ = true;
  cur_batch_idx_ = NextProtoBatchIdx();
  RETURN_IF_ERROR(DoTransmitDataRpc());
  return Status::OK();
}

Status KrpcDataStreamSender::Channel::SendBatch(RowBatch* batch) {
  CachedProtoRowBatch* next_batch = &cached_proto_batches_[NextProtoBatchIdx()];
  RETURN_IF_ERROR(parent_->SerializeBatch(batch, next_batch));
  VLOG_ROW << "Channel::SendBatch() instance_id=" << fragment_instance_id_
           << " dest_node=" << dest_node_id_
           << " #rows=" << next_batch->proto_batch()->num_rows();
  RETURN_IF_ERROR(TransmitData());
  return Status::OK();
}

Status KrpcDataStreamSender::Channel::SendCurrentBatch() {
  RETURN_IF_ERROR(SendBatch(batch_.get()));
  batch_->Reset();
  return Status::OK();
}

Status KrpcDataStreamSender::Channel::AddRow(TupleRow* row) {
  if (batch_->AtCapacity()) {
    // batch_ is full, let's send it.
    RETURN_IF_ERROR(SendCurrentBatch());
  }
  TupleRow* dest = batch_->GetRow(batch_->AddRow());
  const vector<TupleDescriptor*>& descs = row_desc_->tuple_descriptors();
  for (int i = 0; i < descs.size(); ++i) {
    if (UNLIKELY(row->GetTuple(i) == nullptr)) {
      dest->SetTuple(i, nullptr);
    } else {
      dest->SetTuple(i, row->GetTuple(i)->DeepCopy(*descs[i], batch_->tuple_data_pool()));
    }
  }
  batch_->CommitLastRow();
  return Status::OK();
}

void KrpcDataStreamSender::Channel::EndDataStreamCompleteCb() {
  std::unique_lock<SpinLock> l(lock_);
  DCHECK(rpc_in_flight_);
  const kudu::Status controller_status = rpc_controller_.status();
  if (LIKELY(controller_status.ok())) {
    MarkDone(Status(eos_resp_.status()));
  } else {
    DoRpcFn rpc_fn =
        boost::bind(&KrpcDataStreamSender::Channel::DoEndDataStreamRpc, this);
    string err_msg =
        Substitute("EndDataStream() to $0 failed", TNetworkAddressToString(address_));
    HandleFailedRPC(rpc_fn, controller_status, err_msg);
  }
}

Status KrpcDataStreamSender::Channel::DoEndDataStreamRpc() {
  DCHECK(rpc_in_flight_);
  rpc_controller_.Reset();
  UniqueIdPB* finstance_id_pb = eos_req_.mutable_dest_fragment_instance_id();
  finstance_id_pb->set_lo(fragment_instance_id_.lo);
  finstance_id_pb->set_hi(fragment_instance_id_.hi);
  eos_req_.set_sender_id(parent_->sender_id_);
  eos_req_.set_dest_node_id(dest_node_id_);
  proxy->EndDataStreamAsync(eos_req_, &eos_resp_, &rpc_controller_,
      boost::bind(&KrpcDataStreamSender::Channel::EndDataStreamCompleteCb, this));
  return Status::OK();
}

Status KrpcDataStreamSender::Channel::FlushAndSendEos(RuntimeState* state) {
  VLOG_RPC << "Channel::FlushAndSendEos() instance_id=" << fragment_instance_id_
           << " dest_node=" << dest_node_id_
           << " #rows= " << batch_->num_rows();

  // We can return an error here and not go on to send the EOS RPC because the error that
  // we returned will be sent to the coordinator who will then cancel all the remote
  // fragments including the one that this sender is sending to.
  if (batch_->num_rows() > 0) RETURN_IF_ERROR(SendCurrentBatch());
  {
    std::unique_lock<SpinLock> l(lock_);
    RETURN_IF_ERROR(WaitForRpc(&l));
    DCHECK(!rpc_in_flight_);
    VLOG_RPC << "calling EndDataStream() to terminate channel.";
    rpc_in_flight_ = true;
    RETURN_IF_ERROR(DoEndDataStreamRpc());
    RETURN_IF_ERROR(WaitForRpc(&l));
  }
  return Status::OK();
}

void KrpcDataStreamSender::Channel::Teardown(RuntimeState* state) {
  // Normally, FlushAndSendEos() should have been called before calling Teardown(),
  // which means that all the data should already be drained. If the fragment was
  // was closed or cancelled, there may still be some in-flight RPCs and buffered
  // row batches to be flushed.
  std::unique_lock<SpinLock> l(lock_);
  shutdown_ = true;
  // Cancel any in-flight RPC.
  if (rpc_in_flight_) {
    rpc_controller_.Cancel();
    auto pred = [this]() -> bool { return !rpc_in_flight_; };
    rpc_done_cv_.wait(l, pred);
  }
  DCHECK(!rpc_in_flight_);
  batch_.reset();
  req_.release_row_batch_header();
}

KrpcDataStreamSender::KrpcDataStreamSender(int sender_id, const RowDescriptor* row_desc,
    const TDataStreamSink& sink, const vector<TPlanFragmentDestination>& destinations,
    int per_channel_buffer_size)
  : DataSink(row_desc),
    sender_id_(sender_id),
    partition_type_(sink.output_partition.type),
    dest_node_id_(sink.dest_node_id),
    next_unknown_partition_(0) {
  DCHECK_GT(destinations.size(), 0);
  DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED
      || sink.output_partition.type == TPartitionType::HASH_PARTITIONED
      || sink.output_partition.type == TPartitionType::RANDOM
      || sink.output_partition.type == TPartitionType::KUDU);

  for (int i = 0; i < destinations.size(); ++i) {
    channels_.push_back(
        new Channel(this, row_desc, destinations[i].krpc_server,
            destinations[i].fragment_instance_id, sink.dest_node_id,
            per_channel_buffer_size));
  }

  if (partition_type_ == TPartitionType::UNPARTITIONED ||
      partition_type_ == TPartitionType::RANDOM) {
    // Randomize the order we open/transmit to channels to avoid thundering herd problems.
    srand(reinterpret_cast<uint64_t>(this));
    random_shuffle(channels_.begin(), channels_.end());
  }
}

string KrpcDataStreamSender::GetName() {
  return Substitute("KrpcDataStreamSender (dst_id=$0)", dest_node_id_);
}

KrpcDataStreamSender::~KrpcDataStreamSender() {
  // TODO: check that sender was either already closed() or there was an error
  // on some channel
  for (int i = 0; i < channels_.size(); ++i) {
    delete channels_[i];
  }
}

Status KrpcDataStreamSender::Init(const vector<TExpr>& thrift_output_exprs,
    const TDataSink& tsink, RuntimeState* state) {
  DCHECK(tsink.__isset.stream_sink);
  if (partition_type_ == TPartitionType::HASH_PARTITIONED ||
      partition_type_ == TPartitionType::KUDU) {
    RETURN_IF_ERROR(ScalarExpr::Create(tsink.stream_sink.output_partition.partition_exprs,
        *row_desc_, state, &partition_exprs_));
  }
  return Status::OK();
}

Status KrpcDataStreamSender::Prepare(
    RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));
  state_ = state;
  SCOPED_TIMER(profile_->total_time_counter());
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(partition_exprs_, state,
      state->obj_pool(), expr_perm_pool_.get(), expr_results_pool_.get(),
      &partition_expr_evals_));
  serialize_batch_timer_ = ADD_TIMER(profile(), "SerializeBatchTime");
  rpc_retry_counter_ = ADD_COUNTER(profile(), "RpcRetry", TUnit::UNIT);
  bytes_sent_counter_ = ADD_COUNTER(profile(), "BytesSent", TUnit::BYTES);
  uncompressed_bytes_counter_ =
      ADD_COUNTER(profile(), "UncompressedRowBatchSize", TUnit::BYTES);
  total_sent_rows_counter_= ADD_COUNTER(profile(), "RowsReturned", TUnit::UNIT);
  overall_throughput_ =
      profile()->AddDerivedCounter("OverallThroughput", TUnit::BYTES_PER_SECOND,
           bind<int64_t>(&RuntimeProfile::UnitsPerSecond, bytes_sent_counter_,
                         profile()->total_time_counter()));
  for (int i = 0; i < channels_.size(); ++i) {
    RETURN_IF_ERROR(channels_[i]->Init(state));
  }
  return Status::OK();
}

Status KrpcDataStreamSender::Open(RuntimeState* state) {
  return ScalarExprEvaluator::Open(partition_expr_evals_, state);
}

Status KrpcDataStreamSender::Send(RuntimeState* state, RowBatch* batch) {
  DCHECK(!closed_);
  DCHECK(!flushed_);

  if (batch->num_rows() == 0) return Status::OK();
  if (partition_type_ == TPartitionType::UNPARTITIONED || channels_.size() == 1) {
    // SendBatch() will block if there are still in-flight rpcs (and those will
    // reference the previously written thrift batch)
    for (int i = 0; i < channels_.size(); ++i) {
      RETURN_IF_ERROR(channels_[i]->SendBatch(batch));
    }
  } else if (partition_type_ == TPartitionType::RANDOM) {
    // Round-robin batches among channels. Wait for the current channel to finish its
    // rpc before overwriting its batch.
    Channel* current_channel = channels_[current_channel_idx_];
    RETURN_IF_ERROR(current_channel->SendBatch(batch));
    current_channel_idx_ = (current_channel_idx_ + 1) % channels_.size();
  } else if (partition_type_ == TPartitionType::KUDU) {
    DCHECK_EQ(partition_expr_evals_.size(), 1);
    int num_channels = channels_.size();
    for (int i = 0; i < batch->num_rows(); ++i) {
      TupleRow* row = batch->GetRow(i);
      int32_t partition =
          *reinterpret_cast<int32_t*>(partition_expr_evals_[0]->GetValue(row));
      if (partition < 0) {
        // This row doesn't correspond to a partition, e.g. it's outside the given ranges.
        partition = next_unknown_partition_;
        ++next_unknown_partition_;
      }
      RETURN_IF_ERROR(channels_[partition % num_channels]->AddRow(row));
    }
  } else {
    DCHECK_EQ(partition_type_, TPartitionType::HASH_PARTITIONED);
    // hash-partition batch's rows across channels
    // TODO: encapsulate this in an Expr as we've done for Kudu above and remove this case
    // once we have codegen here.
    int num_channels = channels_.size();
    for (int i = 0; i < batch->num_rows(); ++i) {
      TupleRow* row = batch->GetRow(i);
      uint32_t hash_val = HashUtil::FNV_SEED;
      for (int i = 0; i < partition_exprs_.size(); ++i) {
        ScalarExprEvaluator* eval = partition_expr_evals_[i];
        void* partition_val = eval->GetValue(row);
        // We can't use the crc hash function here because it does not result
        // in uncorrelated hashes with different seeds.  Instead we must use
        // fnv hash.
        // TODO: fix crc hash/GetHashValue()
        DCHECK(&partition_expr_evals_[i]->root() == partition_exprs_[i]);
        hash_val = RawValue::GetHashValueFnv(
            partition_val, partition_exprs_[i]->type(), hash_val);
      }
      RETURN_IF_ERROR(channels_[hash_val % num_channels]->AddRow(row));
    }
  }
  COUNTER_ADD(total_sent_rows_counter_, batch->num_rows());
  expr_results_pool_->Clear();
  RETURN_IF_ERROR(state->CheckQueryState());
  return Status::OK();
}

Status KrpcDataStreamSender::FlushFinal(RuntimeState* state) {
  DCHECK(!flushed_);
  DCHECK(!closed_);
  flushed_ = true;
  for (int i = 0; i < channels_.size(); ++i) {
    // If we hit an error here, we can return without closing the remaining channels as
    // the error is propagated back to the coordinator, which in turn cancels the query,
    // which will cause the remaining open channels to be closed.
    RETURN_IF_ERROR(channels_[i]->FlushAndSendEos(state));
  }
  return Status::OK();
}

void KrpcDataStreamSender::Close(RuntimeState* state) {
  if (closed_) return;
  for (int i = 0; i < channels_.size(); ++i) {
    channels_[i]->Teardown(state);
  }
  ScalarExprEvaluator::Close(partition_expr_evals_, state);
  ScalarExpr::Close(partition_exprs_);
  DataSink::Close(state);
  closed_ = true;
}

Status KrpcDataStreamSender::SerializeBatch(
    RowBatch* src, CachedProtoRowBatch* dest, int num_receivers) {
  VLOG_ROW << "serializing " << src->num_rows() << " rows";
  {
    SCOPED_TIMER(profile_->total_time_counter());
    SCOPED_TIMER(serialize_batch_timer_);
    RETURN_IF_ERROR(src->Serialize(dest));
    ProtoRowBatch* proto_batch = dest->proto_batch();
    int64_t bytes = RowBatch::GetSerializedSize(*proto_batch);
    int64_t uncompressed_bytes = RowBatch::GetDeserializedSize(*proto_batch);
    COUNTER_ADD(bytes_sent_counter_, bytes * num_receivers);
    COUNTER_ADD(uncompressed_bytes_counter_, uncompressed_bytes * num_receivers);
  }
  return Status::OK();
}

int64_t KrpcDataStreamSender::GetNumDataBytesSent() const {
  // TODO: do we need synchronization here or are reads & writes to 8-byte ints
  // atomic?
  int64_t result = 0;
  for (int i = 0; i < channels_.size(); ++i) {
    result += channels_[i]->num_data_bytes_sent();
  }
  return result;
}

} // namespace impala

