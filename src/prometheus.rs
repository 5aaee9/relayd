use crate::model::Protocol;
use crate::runtime::facade::ListenerMetricsSnapshot;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Write as _;
use std::hash::{BuildHasherDefault, Hasher};

pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

// Three metric lines per listener average just under this size in normal output.
const ESTIMATED_RENDERED_BYTES_PER_LISTENER: usize = 224;

#[derive(Debug, Clone, PartialEq)]
pub struct ListenerMetricsRate {
    pub port: u16,
    pub protocol: Protocol,
    pub connections_current: u64,
    pub rx_bytes_per_second: f64,
    pub tx_bytes_per_second: f64,
}

#[derive(Debug, Default)]
struct PackedKeyHasher(u64);

impl Hasher for PackedKeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        // Retain a deterministic fallback even though this map only hashes packed u32 keys.
        self.0 = bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    }

    fn write_u32(&mut self, value: u32) {
        // listener_key is injective, so the packed value itself is collision-free.
        self.0 = u64::from(value);
    }
}

type SampleMap = HashMap<u32, Sample, BuildHasherDefault<PackedKeyHasher>>;

#[derive(Debug, Clone, Copy)]
struct Sample {
    rx_bytes_total: u64,
    tx_bytes_total: u64,
    timestamp_ms: i64,
    generation: u64,
}

#[derive(Debug, Default)]
pub struct RateCalculator {
    samples: SampleMap,
    generation: u64,
}

impl RateCalculator {
    pub fn calculate(
        &mut self,
        snapshots: &[ListenerMetricsSnapshot],
        timestamp_ms: i64,
    ) -> Vec<ListenerMetricsRate> {
        self.generation = self.generation.wrapping_add(1);
        let generation = self.generation;
        let mut rates = Vec::with_capacity(snapshots.len());
        for snapshot in snapshots {
            let key = listener_key(snapshot.port, snapshot.protocol);
            let current = Sample {
                rx_bytes_total: snapshot.rx_bytes_total,
                tx_bytes_total: snapshot.tx_bytes_total,
                timestamp_ms,
                generation,
            };
            let previous = match self.samples.entry(key) {
                Entry::Occupied(mut entry) => Some(entry.insert(current)),
                Entry::Vacant(entry) => {
                    entry.insert(current);
                    None
                }
            };
            rates.push(calculate_rate(snapshot, previous, timestamp_ms));
        }
        self.samples
            .retain(|_, sample| sample.generation == generation);
        rates
    }

    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }
}

fn listener_key(port: u16, protocol: Protocol) -> u32 {
    let protocol = match protocol {
        Protocol::Tcp => 0,
        Protocol::Udp => 1,
        Protocol::Both => 2,
    };
    (u32::from(port) << 2) | protocol
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
    let mut out = String::with_capacity(
        rows.len()
            .saturating_mul(ESTIMATED_RENDERED_BYTES_PER_LISTENER),
    );
    out.push_str("# TYPE relayd_connections_current gauge\n");
    for row in rows {
        writeln!(
            out,
            "relayd_connections_current{{port=\"{}\",protocol=\"{}\"}} {}",
            row.port,
            row.protocol.as_str(),
            row.connections_current
        )
        .expect("writing to a String cannot fail");
    }
    out.push_str("# TYPE relayd_rx_bytes_per_second gauge\n");
    for row in rows {
        writeln!(
            out,
            "relayd_rx_bytes_per_second{{port=\"{}\",protocol=\"{}\"}} {}",
            row.port,
            row.protocol.as_str(),
            row.rx_bytes_per_second
        )
        .expect("writing to a String cannot fail");
    }
    out.push_str("# TYPE relayd_tx_bytes_per_second gauge\n");
    for row in rows {
        writeln!(
            out,
            "relayd_tx_bytes_per_second{{port=\"{}\",protocol=\"{}\"}} {}",
            row.port,
            row.protocol.as_str(),
            row.tx_bytes_per_second
        )
        .expect("writing to a String cannot fail");
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
    fn packed_listener_keys_keep_ports_and_protocols_distinct() {
        assert_eq!(listener_key(7000, Protocol::Tcp), 28_000);
        assert_eq!(listener_key(7000, Protocol::Udp), 28_001);
        assert_eq!(listener_key(7000, Protocol::Both), 28_002);
        assert_eq!(listener_key(7001, Protocol::Tcp), 28_004);
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
        // Scrape generations, rather than timestamps, distinguish consecutive snapshots.
        calculator.calculate(&second, 1_000);
        assert_eq!(calculator.sample_count(), 1);

        calculator.calculate(&[], 1_000);
        assert_eq!(calculator.sample_count(), 0);
    }
}
