require "json"

module NATS
  struct ServerInfo
    include JSON::Serializable

    getter server_id : String
    getter server_name : String
    getter version : String
    getter go : String
    getter host : String
    getter port : Int32
    getter? headers : Bool
    getter max_payload : Int64
    getter proto : Int32
    getter? auth_required : Bool = false
    getter? tls_required : Bool = false
    getter? tls_verify : Bool = false
    getter? tls_available : Bool = false
    getter client_id : UInt64?
    getter client_ip : String?
    getter nonce : String?
    getter cluster : String?
    getter domain : String?
    getter connect_urls : Array(String) { [] of String }
    @[JSON::Field(key: "ldm")]
    getter? lame_duck_mode : Bool = false
  end
end
