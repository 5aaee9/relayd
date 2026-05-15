use crate::runtime::facade::ListenerMetricsSnapshot;

pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

pub fn render(rows: &[ListenerMetricsSnapshot]) -> String {
    let mut out = String::new();
    out.push_str("# TYPE relayd_connections_current gauge\n");
    for row in rows {
        out.push_str(&format!(
            "relayd_connections_current{{port=\"{}\",protocol=\"{}\"}} {}\n",
            row.port,
            row.protocol.as_str(),
            row.connections_current
        ));
    }
    out.push_str("# TYPE relayd_rx_bytes_per_second gauge\n");
    for row in rows {
        out.push_str(&format!(
            "relayd_rx_bytes_per_second{{port=\"{}\",protocol=\"{}\"}} 0\n",
            row.port,
            row.protocol.as_str()
        ));
    }
    out.push_str("# TYPE relayd_tx_bytes_per_second gauge\n");
    for row in rows {
        out.push_str(&format!(
            "relayd_tx_bytes_per_second{{port=\"{}\",protocol=\"{}\"}} 0\n",
            row.port,
            row.protocol.as_str()
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Protocol;
    use crate::runtime::facade::ListenerMetricsSnapshot;

    #[test]
    fn prometheus_render_includes_type_lines_and_port_protocol_labels() {
        let rows = [
            ListenerMetricsSnapshot {
                port: 10000,
                protocol: Protocol::Tcp,
                connections_current: 7,
                rx_bytes_total: 100,
                tx_bytes_total: 200,
            },
            ListenerMetricsSnapshot {
                port: 10001,
                protocol: Protocol::Udp,
                connections_current: 3,
                rx_bytes_total: 300,
                tx_bytes_total: 400,
            },
        ];

        let output = render(&rows);

        assert!(output.contains("# TYPE relayd_connections_current gauge\n"));
        assert!(output.contains("# TYPE relayd_rx_bytes_per_second gauge\n"));
        assert!(output.contains("# TYPE relayd_tx_bytes_per_second gauge\n"));
        assert!(output.contains("relayd_connections_current{port=\"10000\",protocol=\"tcp\"} 7\n"));
        assert!(output.contains("relayd_connections_current{port=\"10001\",protocol=\"udp\"} 3\n"));
        assert!(output.contains("relayd_rx_bytes_per_second{port=\"10000\",protocol=\"tcp\"} 0\n"));
        assert!(output.contains("relayd_tx_bytes_per_second{port=\"10001\",protocol=\"udp\"} 0\n"));
        assert!(!output.contains("{\\\""));
    }
}
