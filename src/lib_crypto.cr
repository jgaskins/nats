require "openssl_ext/lib_crypto"

lib LibCrypto
  enum PKEY : LibC::Int
    # https://github.com/openssl/openssl/blob/f2f7cff20377f7402b132a19d953a9d998be26aa/include/openssl/evp.h#L62
    # https://github.com/openssl/openssl/blob/f2f7cff20377f7402b132a19d953a9d998be26aa/include/openssl/obj_mac.h#L4996
    ED25519 = 1087
  end

  alias EVP_PKEY_CTX = Void

  fun evp_pkey_ctx_new = EVP_PKEY_CTX_new(pkey : EvpPKey*, engine : Engine*) : EVP_PKEY_CTX*

  fun evp_pkey_ctx_free = EVP_PKEY_CTX_free(pkey : EVP_PKEY_CTX*) : Void

  fun evp_pkey_new_raw_private_key = EVP_PKEY_new_raw_private_key(
    type : PKEY,
    engine : Engine*,
    key : LibC::Char*,
    keylen : LibC::SizeT
  ) : EvpPKey*

  fun evp_pkey_get_raw_public_key = EVP_PKEY_get_raw_public_key(
    key : EvpPKey*,
    pub : LibC::Char*,
    len : LibC::SizeT*
  ) : LibC::Int

  fun evp_digest_sign_init = EVP_DigestSignInit(
    context : EVP_MD_CTX,
    pkey_context : EVP_PKEY_CTX**,
    type : Void*,
    engine : Void*,
    pkey : EvpPKey*
  ) : LibC::Int

  fun evp_digest_sign = EVP_DigestSign(
    context : EVP_MD_CTX,
    sigret : LibC::Char*,
    siglen : LibC::SizeT*,
    tbs : LibC::Char*,
    tbslen : LibC::SizeT
  ) : LibC::Int

  fun err_error_string_n = ERR_error_string_n(e : ULong, buf : Char*, len : SizeT)
end
