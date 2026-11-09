fn main() {
    tonic_prost_build::configure()
        .compile_protos(&["proto/orderbook.proto"], &["proto"])
        .expect("failed to compile orderbook protobuf");
}
