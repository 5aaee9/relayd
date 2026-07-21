{
  pkgs,
  relaydPackage,
  runtimeMode,
  minimumRatio,
  maximumUdpLossDelta,
}:
let
  topology = import ./common.nix {
    inherit pkgs relaydPackage runtimeMode;
    portRange = "21000-21003";
    withIperf = true;
    tcpSplice = runtimeMode == "proxy";
  };
  c = topology.constants;
in
pkgs.testers.runNixOSTest {
  name = "relayd-performance-${runtimeMode}";
  nodes = topology.nodes;
  globalTimeout = 20 * 60;

  testScript = ''
    AUTH_TOKEN = "${c.token}"
    API_HOST = "${c.relayClient4}"
    API_PORT = ${toString c.apiPort}
    RELAY_HOST4 = "${c.relayClient4}"
    RELAY_HOST6 = "${c.relayClient6}"
    BACKEND_HOST4 = "${c.backend4}"
    BACKEND_HOST6 = "${c.backend6}"
    IPERF_PORT = ${toString c.iperfPort}
    MINIMUM_RATIO = ${toString minimumRatio}
    MAXIMUM_UDP_LOSS_DELTA = ${toString maximumUdpLossDelta}
  ''
  + builtins.readFile ./testlib.py
  + ''

    start_all()
    for machine in (client, relay, backend):
        machine.wait_for_unit("multi-user.target")
    relay.wait_for_unit("relayd.service")
    backend.wait_for_unit("relayd-iperf3.service")
    client.wait_for_open_port(API_PORT, API_HOST)
    client.wait_for_open_port(IPERF_PORT, BACKEND_HOST4)

    with subtest("iperf3 TCP control and UDP data use one dual-protocol relay port"):
        allocation = create_bound(client, "both", BACKEND_HOST4, IPERF_PORT)
        relay_port = allocation["port"]
        direct = {"tcp": [], "udp": []}
        relayed = {"tcp": [], "udp": []}

        for repetition in range(3):
            for protocol in ("tcp", "udp"):
                direct[protocol].append(
                    iperf_sample(client, BACKEND_HOST4, IPERF_PORT, protocol)
                )
                relayed[protocol].append(
                    iperf_sample(client, RELAY_HOST4, relay_port, protocol)
                )

        if "${runtimeMode}" == "proxy":
            deadline = time.monotonic() + 5
            while True:
                metrics = api_json(client, "GET", "/v1/metrics")
                if (
                    metrics["tcp_active_sessions"] == 0
                    and metrics["tcp_splice_success_total"]
                    == metrics["tcp_splice_attempt_total"]
                ):
                    break
                if time.monotonic() >= deadline:
                    raise AssertionError(
                        f"TCP splice sessions did not settle: {metrics!r}"
                    )
                time.sleep(0.05)
            t.assertGreaterEqual(metrics["tcp_splice_attempt_total"], 1)
            t.assertEqual(metrics["tcp_splice_hard_failure_total"], 0)
            t.assertEqual(metrics["tcp_copy_fallback_total"], 0)

        summary = {
            "runtime_mode": "${runtimeMode}",
            "minimum_ratio": MINIMUM_RATIO,
            "maximum_udp_loss_delta": MAXIMUM_UDP_LOSS_DELTA,
            "direct": direct,
            "relayed": relayed,
        }
        for protocol in ("tcp", "udp"):
            direct_median = median(
                [sample["bits_per_second"] for sample in direct[protocol]]
            )
            relay_median = median(
                [sample["bits_per_second"] for sample in relayed[protocol]]
            )
            ratio = relay_median / direct_median
            summary[f"{protocol}_direct_median_bps"] = direct_median
            summary[f"{protocol}_relay_median_bps"] = relay_median
            summary[f"{protocol}_relay_to_direct_ratio"] = ratio
            for sample in direct[protocol] + relayed[protocol]:
                t.assertGreaterEqual(
                    sample["bits_per_second"],
                    1_000_000,
                    f"{protocol} sample did not reach the 1 Mbit/s smoke floor",
                )
            t.assertGreaterEqual(
                ratio,
                MINIMUM_RATIO,
                f"{protocol} relay/direct median ratio regressed: {ratio:.3f}",
            )

        direct_udp_loss = median(
            [sample["lost_percent"] for sample in direct["udp"]]
        )
        relay_udp_loss = median(
            [sample["lost_percent"] for sample in relayed["udp"]]
        )
        loss_delta = relay_udp_loss - direct_udp_loss
        summary["udp_direct_median_loss_percent"] = direct_udp_loss
        summary["udp_relay_median_loss_percent"] = relay_udp_loss
        summary["udp_loss_delta_percentage_points"] = loss_delta
        t.assertLessEqual(
            loss_delta,
            MAXIMUM_UDP_LOSS_DELTA,
            f"UDP relay loss regressed by {loss_delta:.3f} percentage points",
        )

        print("relayd performance summary:")
        print(json.dumps(summary, indent=2, sort_keys=True))
        api(client, "DELETE", f"/v1/allocations/{allocation['id']}", expected_status=204)
        expect_probe_failure(client, "tcp", relay_port)
  '';
}
