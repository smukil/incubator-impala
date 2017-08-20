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


#ifndef IMPALA_RUNTIME_KRPC_DATA_STREAM_SENDER_H
#define IMPALA_RUNTIME_KRPC_DATA_STREAM_SENDER_H

#include <vector>
#include <string>

#include "exec/data-sink.h"
#include "common/global-types.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "runtime/row-batch.h"
#include "util/runtime-profile.h"

namespace impala {

class RowDescriptor;
class MemTracker;
class TDataStreamSink;
class TNetworkAddress;
class TPlanFragmentDestination;

/// Single sender of an m:n data stream.
/// Row batch data is routed to destinations based on the provided partitioning
/// specification.
/// *Not* thread-safe.
///
/// TODO: capture stats that describe distribution of rows/data volume
/// across channels.
/// TODO: create a PlanNode equivalent class for DataSink.
class KrpcDataStreamSender : public DataSink {
 public:
  /// Construct a sender according to the output specification (sink),
  /// sending to the given destinations:
  /// 'sender_id' identifies this sender instance, and is unique within a fragment.
  /// 'per_channel_buffer_size' is the buffer size in bytes allocated to each channel.
  /// 'row_desc' is the descriptor of the tuple row. It must out-live the sink.
  /// NOTE: supported partition types are UNPARTITIONED (broadcast), HASH_PARTITIONED,
  /// and RANDOM.
  KrpcDataStreamSender(int sender_id, const RowDescriptor* row_desc,
      const TDataStreamSink& tsink,
      const std::vector<TPlanFragmentDestination>& destinations,
      int per_channel_buffer_size);

  virtual ~KrpcDataStreamSender();

  virtual std::string GetName();

  /// Initialize the sender by initializing all the channels and allocates all
  /// the stat counters. Return error status if any channels failed to initialize.
  virtual Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker)
      WARN_UNUSED_RESULT;

  /// Initialize the evaluator of the partitioning expressions. Return error status
  /// if initialization failed.
  virtual Status Open(RuntimeState* state) WARN_UNUSED_RESULT;

  /// Flush all buffered data and close all existing channels to destination hosts.
  /// Further Send() calls are illegal after calling FlushFinal(). It is legal to call
  /// FlushFinal() no more than once. Return error status if Send() failed or the end
  /// of stream call failed.
  virtual Status FlushFinal(RuntimeState* state) WARN_UNUSED_RESULT;

  /// Send data in 'batch' to destination nodes according to partitioning
  /// specification provided in c'tor.
  /// Blocks until all rows in batch are placed in their appropriate outgoing
  /// buffers (ie, blocks if there are still in-flight rpcs from the last
  /// Send() call).
  virtual Status Send(RuntimeState* state, RowBatch* batch) WARN_UNUSED_RESULT;

  /// Shutdown all existing channels to destination hosts. Further FlushFinal() calls are
  /// illegal after calling Close().
  virtual void Close(RuntimeState* state);

  /// Serializes the src batch into the cached protobuf row batch 'dest' and updates
  /// various stat counters.
  /// 'num_receivers' is the number of receivers this batch will be sent to. Used for
  /// updating the stat counters.
  Status SerializeBatch(RowBatch* src, CachedProtoRowBatch* dest, int num_receivers = 1)
      WARN_UNUSED_RESULT;

  /// Return total number of bytes sent. If batches are broadcast to multiple receivers,
  /// they are counted once per receiver.
  int64_t GetNumDataBytesSent() const;

 protected:
  friend class DataStreamTest;

  /// Initialize any partitioning expressions based on 'thrift_output_exprs' and stores
  /// them in 'partition_exprs_'. Returns error status if the initialization failed.
  virtual Status Init(const std::vector<TExpr>& thrift_output_exprs,
      const TDataSink& tsink, RuntimeState* state) WARN_UNUSED_RESULT;

 private:
  class Channel;

  /// Sender instance id, unique within a fragment.
  int sender_id_;

  /// RuntimeState of the fragment instance.
  RuntimeState* state_ = nullptr;

  /// The type of partitioning to perform.
  const TPartitionType::type partition_type_;

  /// Index of current channel to send to if random_ == true.
  int current_channel_idx_ = 0;

  /// Index of the next cached proto batch to use for serialization.
  int current_batch_idx_ = 0;

  // The two cached protobuf row batches. Each entry contains a ProtoRowBatch and the
  // buffer for the serialized row batches. When one is used for the in-flight RPC,
  // the execution thread can continue to run and serialize another row batch to the
  // other entry. 'current_batch_idx_' is the index of the entry being used by the
  // in-flight or last completed RPC.
  static const int num_cached_proto_batches_ = 2;
  CachedProtoRowBatch cached_proto_batches_[num_cached_proto_batches_];

  /// If true, this sender has called FlushFinal() successfully.
  /// Not valid to call Send() anymore.
  bool flushed_ = false;

  /// If true, this sender has been closed. Not valid to call Send() anymore.
  bool closed_ = false;

  /// List of all channels. One for each destination.
  std::vector<Channel*> channels_;

  /// Expressions of partition keys. It's used to compute the
  /// per-row partition values for shuffling exchange;
  std::vector<ScalarExpr*> partition_exprs_;
  std::vector<ScalarExprEvaluator*> partition_expr_evals_;

  /// Time for serializing row batches.
  RuntimeProfile::Counter* serialize_batch_timer_ = nullptr;

  /// Number of TransmitData() RPC retries due to remote service being busy.
  RuntimeProfile::Counter* rpc_retry_counter_ = nullptr;

  /// Total number of bytes sent.
  RuntimeProfile::Counter* bytes_sent_counter_ = nullptr;

  /// Total number of bytes of the row batches before compression.
  RuntimeProfile::Counter* uncompressed_bytes_counter_ = nullptr;

  /// Total number of rows sent.
  RuntimeProfile::Counter* total_sent_rows_counter_ = nullptr;

  /// Throughput per total time spent in sender
  RuntimeProfile::Counter* overall_throughput_ = nullptr;

  /// Identifier of the destination plan node.
  PlanNodeId dest_node_id_;

  /// Used for Kudu partitioning to round-robin rows that don't correspond to a partition
  /// or when errors are encountered.
  int next_unknown_partition_;
};

} // namespace impala

#endif // IMPALA_RUNTIME_KRPC_DATA_STREAM_SENDER_H
