# stolen from https://github.com/amitmurthy/AWS.jl/blob/master/src/crypto.jl
# "since OpenSSL.jl does not yet support the functions this package
# needs. This will be replaced with calls to OpenSSL.jl when it
# supports the same"

module Crypto

macro c(ret_type, func, arg_types, lib)
  local args_in = Any[ symbol(string('a',x)) for x in 1:length(arg_types.args) ]
  quote
    $(esc(func))($(args_in...)) = ccall( ($(string(func)), $(Expr(:quote, eval(lib))) ), $ret_type, $arg_types, $(args_in...) )
  end
end

macro ctypedef(fake_t,real_t)
  quote
    typealias $fake_t $real_t
  end
end


@ctypedef EVP_MD Void
@ctypedef EVP_MD_CTX Void
@ctypedef ENGINE Void

typealias size_t Csize_t

@unix_only const libcrypto = "libcrypto"
@windows_only const libcrypto = Pkg.dir("WinRPM","deps","usr","$(Sys.ARCH)-w64-mingw32","sys-root","mingw","bin","libcrypto-10")

@c Ptr{UInt8} HMAC (Ptr{EVP_MD}, Ptr{Void}, Int32, Ptr{UInt8}, size_t, Ptr{UInt8}, Ptr{UInt32}) libcrypto
@c Ptr{UInt8} MD5 (Ptr{UInt8}, size_t, Ptr{UInt8}) libcrypto
@c Ptr{UInt8} SHA256 (Ptr{UInt8}, size_t, Ptr{UInt8}) libcrypto

@c Ptr{EVP_MD} EVP_md5 () libcrypto
@c Ptr{EVP_MD} EVP_sha1 () libcrypto
@c Ptr{EVP_MD} EVP_sha256 () libcrypto

@c Ptr{EVP_MD_CTX} EVP_MD_CTX_create () libcrypto
@c Int32 EVP_DigestInit_ex (Ptr{EVP_MD_CTX}, Ptr{EVP_MD}, Ptr{ENGINE}) libcrypto
@c Int32 EVP_DigestUpdate (Ptr{EVP_MD_CTX}, Ptr{Void}, size_t) libcrypto
@c Int32 EVP_DigestFinal_ex (Ptr{EVP_MD_CTX}, Ptr{UInt8}, Ptr{UInt32}) libcrypto
@c Union{} EVP_MD_CTX_destroy (Ptr{EVP_MD_CTX},) libcrypto


hmacsha256_digest(s::AbstractString, k::Union{ASCIIString, Vector{UInt8}}) =  hmacsha_digest(s, k, EVP_sha256(), 32)
export hmacsha256_digest

hmacsha1_digest(s::AbstractString, k::Union{ASCIIString, Vector{UInt8}}) = hmacsha_digest(s, k, EVP_sha1(), 20)
export hmacsha1_digest

function hmacsha_digest(s::AbstractString, k::Union{ASCIIString, Vector{UInt8}}, evp, dgst_len)
    if evp == C_NULL error("EVP_sha1() failed!") end

    sig = zeros(UInt8, dgst_len)
    sig_len = zeros(UInt32, 1)

    if isa(k, AbstractString)
        k = convert(Array{UInt8}, k)
    end

    if HMAC(evp, k, length(k), s, length(s), sig, sig_len) == C_NULL error("HMAC() failed!") end
    if (sig_len[1] != dgst_len) error("Wrong length of signature!") end

    return sig
end

function sha256(s::AbstractString)
    sha = zeros(UInt8, 32)
    assert(SHA256(s, length(s), sha) != C_NULL)
    return sha
end


end # Module end