require "./entity"
require "./peer_info"

module NATS::JetStream
  struct ClusterInfo < Entity
    getter name : String?
    getter leader : String?
    getter replicas : Array(PeerInfo) { [] of PeerInfo }
  end

  deprecate_api_v1 ClusterInfo
end
