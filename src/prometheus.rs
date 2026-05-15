use crate::model::Protocol;
use crate::runtime::facade::ListenerMetricsSnapshot;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

#[derive(Debug, Clone, PartialEq)]
pub struct ListenerMetricsRate {
    pub port: u16,
    pub protocol: Protocol,
    pub connections_current: u64,
    pub rx_bytes_per_second: f64,
    pub tx_bytes_per_second: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Key {
    port: u16,
    protocol: Protocol,
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.port.hash(state);
        self.protocol.as_str().hash(state);
    }
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    rx_bytes_total: u64,
    tx_bytes_total: u64,
    timestamp_ms: i64,
}

#[derive(Debug, Default)]
pub struct RateCalculator {
    samples: HashMap<Key, Sample>,
}

impl RateCalculator {
    pub fn calculate(
        &mut self,
        snapshots: &[ListenerMetricsSnapshot],
        timestamp_ms: i64,
    ) -> Vec<ListenerMetricsRate> {
        let mut rates = Vec::with_capacity(snapshots.len());
        let mut current_keys = HashSet::with_capacity(snapshots.len());
        for snapshot in snapshots {
            let key = Key {
                port: snapshot.port,
                protocol: snapshot.protocol,
            };
            current_keys.insert(key);
            let previous = self.samples.get(&key).copied();
            rates.push(calculate_rate(snapshot, previous, timestamp_ms));
            self.samples.insert(
                key,
                Sample {
                    rx_bytes_total: snapshot.rx_bytes_total,
                    tx_bytes_total: snapshot.tx_bytes_total,
                    timestamp_ms,
                },
            );
        }
        self.samples.retain(|key, _| current_keys.contains(key));
        rates
    }

    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }
}

fn calculate_rate(
    snapshot: &ListenerMetricsSnapshot,
    previous: Option<Sample>,
    timestamp_ms: i64,
) -> ListenerMetricsRate {
    let mut rx_rate = 0.0;
    let mut tx_rate = 0.0;
    if let Some(previous) = previous {
        let elapsed_ms = timestamp_ms.saturating_sub(previous.timestamp_ms);
        if elapsed_ms > 0 {
            let elapsed_seconds = elapsed_ms as f64 / 1000.0;
            rx_rate = bytes_per_second(
                snapshot.rx_bytes_total,
                previous.rx_bytes_total,
                elapsed_seconds,
            );
            tx_rate = bytes_per_second(
                snapshot.tx_bytes_total,
                previous.tx_bytes_total,
                elapsed_seconds,
            );
        }
    }
    ListenerMetricsRate {
        port: snapshot.port,
        protocol: snapshot.protocol,
        connections_current: snapshot.connections_current,
        rx_bytes_per_second: rx_rate,
        tx_bytes_per_second: tx_rate,
    }
}

fn bytes_per_second(current: u64, previous: u64, elapsed_seconds: f64) -> f64 {
    if current <= previous {
        0.0
    } else {
        (current - previous) as f64 / elapsed_seconds
    }
}

pub fn render_rates(rows: &[ListenerMetricsRate]) -> String {
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
            "relayd_rx_bytes_per_second{{port=\"{}\",protocol=\"{}\"}} {}\n",
            row.port,
            row.protocol.as_str(),
            row.rx_bytes_per_second
        ));
    }
    out.push_str("# TYPE relayd_tx_bytes_per_second gauge\n");
    for row in rows {
        out.push_str(&format!(
            "relayd_tx_bytes_per_second{{port=\"{}\",protocol=\"{}\"}} {}\n",
            row.port,
            row.protocol.as_str(),
            row.tx_bytes_per_second
        ));
    }
    out
}

pub fn render(rows: &[ListenerMetricsSnapshot]) -> String {
    let mut calculator = RateCalculator::default();
    let rates = calculator.calculate(rows, 0);
    render_rates(&rates)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(!output.contains("{\""));
    }

    #[test]
    fn rate_calculator_reports_zero_for_first_sample() {
        let mut calculator = RateCalculator::default();
        let rows = [ListenerMetricsSnapshot {
            port: 7000,
            protocol: Protocol::Tcp,
            connections_current: 2,
            rx_bytes_total: 100,
            tx_bytes_total: 50,
        }];

        let rates = calculator.calculate(&rows, 1_000);

        assert_eq!(rates.len(), 1);
        assert_eq!(rates[0].connections_current, 2);
        assert_eq!(rates[0].rx_bytes_per_second, 0.0);
        assert_eq!(rates[0].tx_bytes_per_second, 0.0);
    }

    #[test]
    fn rate_calculator_reports_positive_delta_rates() {
        let mut calculator = RateCalculator::default();
        let first = [ListenerMetricsSnapshot {
            port: 7000,
            protocol: Protocol::Tcp,
            connections_current: 1,
            rx_bytes_total: 100,
            tx_bytes_total: 50,
        }];
        calculator.calculate(&first, 1_000);

        let second = [ListenerMetricsSnapshot {
            port: 7000,
            protocol: Protocol::Tcp,
            connections_current: 1,
            rx_bytes_total: 300,
            tx_bytes_total: 150,
        }];
        let rates = calculator.calculate(&second, 2_000);

        assert_eq!(rates[0].rx_bytes_per_second, 200.0);
        assert_eq!(rates[0].tx_bytes_per_second, 100.0);
    }

    #[test]
    fn rate_calculator_clamps_zero_elapsed_and_counter_reset_to_zero() {
        let mut calculator = RateCalculator::default();
        calculator.calculate(
            &[ListenerMetricsSnapshot {
                port: 7000,
                protocol: Protocol::Udp,
                connections_current: 1,
                rx_bytes_total: 300,
                tx_bytes_total: 150,
            }],
            1_000,
        );

        let same_time = calculator.calculate(
            &[ListenerMetricsSnapshot {
                port: 7000,
                protocol: Protocol::Udp,
                connections_current: 1,
                rx_bytes_total: 500,
                tx_bytes_total: 250,
            }],
            1_000,
        );
        assert_eq!(same_time[0].rx_bytes_per_second, 0.0);
        assert_eq!(same_time[0].tx_bytes_per_second, 0.0);

        let reset = calculator.calculate(
            &[ListenerMetricsSnapshot {
                port: 7000,
                protocol: Protocol::Udp,
                connections_current: 1,
                rx_bytes_total: 100,
                tx_bytes_total: 50,
            }],
            2_000,
        );
        assert_eq!(reset[0].rx_bytes_per_second, 0.0);
        assert_eq!(reset[0].tx_bytes_per_second, 0.0);
    }

    #[test]
    fn rate_calculator_keeps_same_port_protocol_keys_independent_when_rows_reorder() {
        let mut calculator = RateCalculator::default();
        let first = [
            ListenerMetricsSnapshot {
                port: 7000,
                protocol: Protocol::Tcp,
                connections_current: 1,
                rx_bytes_total: 100,
                tx_bytes_total: 50,
            },
            ListenerMetricsSnapshot {
                port: 7000,
                protocol: Protocol::Udp,
                connections_current: 2,
                rx_bytes_total: 1000,
                tx_bytes_total: 500,
            },
        ];
        calculator.calculate(&first, 1_000);

        let second_reordered = [
            ListenerMetricsSnapshot {
                port: 7000,
                protocol: Protocol::Udp,
                connections_current: 2,
                rx_bytes_total: 1200,
                tx_bytes_total: 700,
            },
            ListenerMetricsSnapshot {
                port: 7000,
                protocol: Protocol::Tcp,
                connections_current: 1,
                rx_bytes_total: 150,
                tx_bytes_total: 75,
            },
        ];
        let rates = calculator.calculate(&second_reordered, 2_000);

        let udp = rates
            .iter()
            .find(|row| row.protocol == Protocol::Udp)
            .unwrap();
        let tcp = rates
            .iter()
            .find(|row| row.protocol == Protocol::Tcp)
            .unwrap();
        assert_eq!(udp.rx_bytes_per_second, 200.0);
        assert_eq!(udp.tx_bytes_per_second, 200.0);
        assert_eq!(tcp.rx_bytes_per_second, 50.0);
        assert_eq!(tcp.tx_bytes_per_second, 25.0);
    }

    #[test]
    fn rate_renderer_preserves_type_lines_labels_and_first_sample_zero_rates() {
        let mut calculator = RateCalculator::default();
        let snapshots = [ListenerMetricsSnapshot {
            port: 7000,
            protocol: Protocol::Tcp,
            connections_current: 3,
            rx_bytes_total: 500,
            tx_bytes_total: 250,
        }];
        let rates = calculator.calculate(&snapshots, 1_000);

        let output = render_rates(&rates);

        assert!(output.contains("# TYPE relayd_connections_current gauge\n"));
        assert!(output.contains("# TYPE relayd_rx_bytes_per_second gauge\n"));
        assert!(output.contains("# TYPE relayd_tx_bytes_per_second gauge\n"));
        assert!(output.contains("relayd_connections_current{port=\"7000\",protocol=\"tcp\"} 3\n"));
        assert!(output.contains("relayd_rx_bytes_per_second{port=\"7000\",protocol=\"tcp\"} 0\n"));
        assert!(output.contains("relayd_tx_bytes_per_second{port=\"7000\",protocol=\"tcp\"} 0\n"));
        assert!(!output.contains("{\""));
    }

    #[test]
    fn rate_calculator_removes_stale_listener_keys() {
        let mut calculator = RateCalculator::default();
        let first = [
            ListenerMetricsSnapshot {
                port: 7000,
                protocol: Protocol::Tcp,
                connections_current: 1,
                rx_bytes_total: 100,
                tx_bytes_total: 50,
            },
            ListenerMetricsSnapshot {
                port: 7001,
                protocol: Protocol::Udp,
                connections_current: 1,
                rx_bytes_total: 200,
                tx_bytes_total: 75,
            },
        ];
        calculator.calculate(&first, 1_000);
        assert_eq!(calculator.sample_count(), 2);

        let second = [ListenerMetricsSnapshot {
            port: 7000,
            protocol: Protocol::Tcp,
            connections_current: 1,
            rx_bytes_total: 150,
            tx_bytes_total: 100,
        }];
        calculator.calculate(&second, 2_000);

        assert_eq!(calculator.sample_count(), 1);
    }
}
