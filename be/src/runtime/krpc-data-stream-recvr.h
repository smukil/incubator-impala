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

#ifndef IMPALA_RUNTIME_KRPC_DATA_STREAM_RECVR_H
#define IMPALA_RUNTIME_KRPC_DATA_STREAM_RECVR_H

#include "data-stream-recvr-base.h"

#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "common/object-pool.h"
#include "common/status.h"
#include "gen-cpp/Types_types.h"   // for TUniqueId
#include "runtime/descriptors.h"
#include "util/tuple-row-compare.h"

namespace kudu {
namespace rpc {
class RpcContext;
} // namespace rpc
} // namespace kudu

namespace impala {

class KrpcDataStreamMgr;
class MemTracker;
class RowBatch;
class RuntimeProfile;
class SortedRunMerger;
class TransmitDataRequestPB;
class TransmitDataResponsePB;

/// Single receiver of an m:n data streams
///
/// KrpcDataStreamRecvr maintains one or more queues of row batches received by a
/// KrpcDataStreamMgr from one or more sender fragment instances. Receivers are created
/// via KrpcDataStreamMgr::CreateRecvr(). Ownership of a stream recvr is shared between
/// the KrpcDataStreamMgr that created it and the caller of
/// KrpcDataStreamMgr::CreateRecvr() (i.e. the exchange node)
///
/// The is_merging_ member determines if the recvr merges input streams from different
/// sender fragment instances according to a specified sort order.
/// If is_merging_ = false : Only one batch queue is maintained for row batches from all
/// sender fragment instances. These row batches are returned one at a time via
/// GetBatch().
/// If is_merging_ is true : One queue is created for the batches from each distinct
/// sender. A SortedRunMerger instance must be created via CreateMerger() prior to
/// retrieving any rows from the receiver. Rows are retrieved from the receiver via
/// GetNext(RowBatch* output_batch, int limit, bool eos). After the final call to
/// GetNext(), TransferAllResources() must be called to transfer resources from the input
/// batches from each sender to the caller's output batch.
/// The receiver sets deep_copy to false on the merger - resources are transferred from
/// the input batches from each sender queue to the merger to the output batch by the
/// merger itself as it processes each run.
///
/// KrpcDataStreamRecvr::Close() must be called by the caller of CreateRecvr() to remove
/// the recvr instance from the tracking structure of its KrpcDataStreamMgr in all cases.
class KrpcDataStreamRecvr : public DataStreamRecvrBase {
 public:
  ~KrpcDataStreamRecvr();

  /// Returns next row batch in data stream; blocks if there aren't any.
  /// Retains ownership of the returned batch. The caller must acquire data from the
  /// returned batch before the next call to GetBatch(). A NULL returned batch indicated
  /// eos. Must only be called if is_merging_ is false.
  /// TODO: This is currently only exposed to the non-merging version of the exchange.
  /// Refactor so both merging and non-merging exchange use GetNext(RowBatch*, bool* eos).
  Status GetBatch(RowBatch** next_batch);

  /// Deregister from KrpcDataStreamMgr instance, which shares ownership of this instance.
  void Close();

  /// Create a SortedRunMerger instance to merge rows from multiple sender according to
  /// the specified row comparator. Fetches the first batches from the individual sender
  /// queues. The exprs used in less_than must have already been prepared and opened.
  Status CreateMerger(const TupleRowComparator& less_than);

  /// Fill output_batch with the next batch of rows obtained by merging the per-sender
  /// input streams. Must only be called if is_merging_ is true.
  Status GetNext(RowBatch* output_batch, bool* eos);

  /// Transfer all resources from the current batches being processed from each sender
  /// queue to the specified batch.
  void TransferAllResources(RowBatch* transfer_batch);

  const TUniqueId& fragment_instance_id() const { return fragment_instance_id_; }
  PlanNodeId dest_node_id() const { return dest_node_id_; }
  const RowDescriptor* row_desc() const { return row_desc_; }
  MemTracker* mem_tracker() const { return mem_tracker_.get(); }

 private:
  friend class KrpcDataStreamMgr;
  class SenderQueue;

  KrpcDataStreamRecvr(KrpcDataStreamMgr* stream_mgr, MemTracker* parent_tracker,
      const RowDescriptor* row_desc, const TUniqueId& fragment_instance_id,
      PlanNodeId dest_node_id, int num_senders, bool is_merging,
      int64_t total_buffer_limit, RuntimeProfile* profile);

  /// Add a new batch of rows to the appropriate sender queue. Does not block - if the
  /// batch can't be added, it is discarded and this method returns. The RPC that
  /// 'payload' encapsulates will be guaranteed a response to once this method is called.
  void AddBatch(ProtoRowBatch& proto_batch, const TransmitDataRequestPB* request,
      TransmitDataResponsePB* response, kudu::rpc::RpcContext* context);

  /// Indicate that a particular sender is done. Delegated to the appropriate
  /// sender queue. Called from KrpcDataStreamMgr.
  void RemoveSender(int sender_id);

  /// Empties the sender queues and notifies all waiting consumers of cancellation.
  void CancelStream();

  /// Return true if the addition of a new batch of size 'batch_size' would exceed the
  /// total buffer limit.
  bool ExceedsLimit(int64_t batch_size) {
    return num_buffered_bytes_.Load() + batch_size > total_buffer_limit_;
  }

  /// XXX
  void DumpDetails(int sender_id);

  /// KrpcDataStreamMgr instance used to create this recvr. (Not owned)
  KrpcDataStreamMgr* mgr_;

  /// Fragment and node id of the destination exchange node this receiver is used by.
  TUniqueId fragment_instance_id_;
  PlanNodeId dest_node_id_;

  /// soft upper limit on the total amount of buffering allowed for this stream across
  /// all sender queues. we stop acking incoming data once the amount of buffered data
  /// exceeds this value
  int64_t total_buffer_limit_;

  /// Row schema.
  const RowDescriptor* row_desc_;

  /// True if this reciver merges incoming rows from different senders. Per-sender
  /// row batch queues are maintained in this case.
  bool is_merging_;

  /// total number of bytes held across all sender queues.
  AtomicInt32 num_buffered_bytes_;

  /// Memtracker for batches in the sender queue(s).
  boost::scoped_ptr<MemTracker> mem_tracker_;

  /// One or more queues of row batches received from senders. If is_merging_ is true,
  /// there is one SenderQueue for each sender. Otherwise, row batches from all senders
  /// are placed in the same SenderQueue. The SenderQueue instances are owned by the
  /// receiver and placed in sender_queue_pool_.
  std::vector<SenderQueue*> sender_queues_;

  /// SortedRunMerger used to merge rows from different senders.
  boost::scoped_ptr<SortedRunMerger> merger_;

  /// Pool of sender queues.
  ObjectPool sender_queue_pool_;

  /// Runtime profile storing the counters below.
  RuntimeProfile* profile_;

  /// Maintain two child profiles - receiver side measurements (from the GetBatch() path),
  /// and sender side measurements (from AddBatch()).
  RuntimeProfile* recvr_side_profile_;
  RuntimeProfile* sender_side_profile_;

  /// Number of bytes received
  RuntimeProfile::Counter* bytes_received_counter_;

  /// Time series of number of bytes received, samples bytes_received_counter_
  RuntimeProfile::TimeSeriesCounter* bytes_received_time_series_counter_;

  RuntimeProfile::Counter* deserialize_row_batch_timer_;

  /// XXX
  RuntimeProfile::Counter* add_batch_timer_;
  RuntimeProfile::Counter* early_check_timer_;
  RuntimeProfile::Counter* enqueue_timer_;
  RuntimeProfile::Counter* num_early_senders_;
  RuntimeProfile::Counter* num_closed_senders_;

  /// Time spent waiting until the first batch arrives across all queues.
  /// TODO: Turn this into a wall-clock timer.
  RuntimeProfile::Counter* first_batch_wait_total_timer_;

  /// Total number of batches received and deferred as sender queue is full.
  RuntimeProfile::Counter* num_deferred_batches_;

  /// Total number of batches received and accepted into the sender queue.
  RuntimeProfile::Counter* num_accepted_batches_;

  /// Total time spent waiting for data to arrive in the recv buffer
  RuntimeProfile::Counter* data_arrival_timer_;

  /// Pointer to profile's inactive timer.
  RuntimeProfile::Counter* inactive_timer_;

  /// Total time spent in SenderQueue::GetBatch().
  RuntimeProfile::Counter* queue_get_batch_time_;
};

} // namespace impala

#endif // IMPALA_RUNTIME_KRPC_DATA_STREAM_RECVR_H
