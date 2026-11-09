fn main() {
    tonic_prost_build::configure()
        .build_client(false)
        .compile_protos(&["../proto/orderbook.proto"], &["../proto"])
        .expect("failed to compile orderbook protobuf");
}
