use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use std::str::FromStr;

pub fn start_metrics() {
    let builder = PrometheusBuilder::new();
    let socket_addr = SocketAddr::from_str("0.0.0.0:9000").unwrap();

    builder
        .listen_address(socket_addr)
        .add_global_label("system", "plateau")
        .install()
        .expect("failed to install Prometheus recorder");
}
