fn main() {
    println!("cargo:rerun-if-changed=../proto/flwr/proto/fleet.proto");
    println!("cargo:rerun-if-changed=../proto/flwr/proto/driver.proto");

    tonic_build::configure()
        .compile(
            &[
                "../proto/flwr/proto/fleet.proto",
                "../proto/flwr/proto/driver.proto",
            ],
            &["../proto/"],
        )
        .unwrap();
}
