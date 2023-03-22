require "./entity"
require "./stream_config"
require "./stream_state"
require "./cluster_info"
require "./stream_source_info"

module NATS::JetStream
  struct Stream < Entity
    getter config : StreamConfig
    getter created : Time
    getter state : StreamState
    getter cluster : ClusterInfo?
    getter mirror : StreamSourceInfo?
    getter sources : Array(StreamSourceInfo) { [] of StreamSourceInfo }
  end

  deprecate_api_v1 Stream
end
