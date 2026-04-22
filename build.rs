fn main() {
	println!("cargo:rerun-if-env-changed=NO_TEST_NETWORK");
	println!("cargo:rustc-check-cfg=cfg(no_test_network)");
	if std::env::var_os("NO_TEST_NETWORK").is_some() {
		println!("cargo:rustc-cfg=no_test_network");
	}
}
