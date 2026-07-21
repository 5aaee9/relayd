{
  pkgs,
  relaydPackage,
  runtimeMode,
}:
let
  topology = import ./common.nix {
    inherit pkgs relaydPackage runtimeMode;
    portRange = "20000-20015";
  };
  c = topology.constants;
  isNetlink = runtimeMode == "netlink";
in
pkgs.testers.runNixOSTest {
  name = "relayd-e2e-${runtimeMode}";
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
    PRIMARY_PORT = ${toString c.primaryPort}
    SECONDARY_PORT = ${toString c.secondaryPort}
    IPV6_PORT = ${toString c.ipv6Port}
    IS_NETLINK = ${if isNetlink then "True" else "False"}
  ''
  + builtins.readFile ./testlib.py
  + ''

    start_all()
    for machine in (client, relay, backend):
        machine.wait_for_unit("multi-user.target")
    relay.wait_for_unit("relayd.service")
    backend.wait_for_unit("relayd-echo-primary.service")
    backend.wait_for_unit("relayd-echo-secondary.service")
    backend.wait_for_unit("relayd-echo-ipv6.service")
    client.wait_for_open_port(API_PORT, API_HOST)

    with subtest("three-node IPv4 and IPv6 routes are live"):
        client.succeed(shell_join(["ping", "-c", "1", "-W", "2", BACKEND_HOST4]))
        client.succeed(shell_join(["ping", "-6", "-c", "1", "-W", "2", BACKEND_HOST6]))

    with subtest("every HTTP endpoint requires the bearer token"):
        for path in ("/v1/allocations", "/v1/ports", "/v1/metrics", "/metrics"):
            status, body = api(client, "GET", path, expected_status=401, authorization=None)
            t.assertEqual(body, "unauthorized")
            status, body = api(
                client,
                "GET",
                path,
                expected_status=401,
                authorization="Bearer wrong-token",
            )
            t.assertEqual(body, "unauthorized")
        for method, path, payload in (
            ("POST", "/v1/allocations", {"protocol": "tcp"}),
            (
                "PUT",
                "/v1/allocations/missing/binding",
                {"host": BACKEND_HOST4, "target_port": PRIMARY_PORT},
            ),
            ("DELETE", "/v1/allocations/missing", None),
        ):
            _, body = api(
                client,
                method,
                path,
                payload,
                expected_status=401,
                authorization=None,
            )
            t.assertEqual(body, "unauthorized")
        status, body = api(
            client,
            "GET",
            "/v1/allocations",
            expected_status=401,
            authorization="Basic bm90OnRoZS10b2tlbg==",
        )
        t.assertEqual(body, "unauthorized")

    with subtest("primary allocation and binding API drives TCP lifecycle"):
        tcp_allocation = create_allocation(client, "tcp")
        tcp_id = tcp_allocation["id"]
        tcp_port = tcp_allocation["port"]
        fetched = api_json(client, "GET", f"/v1/allocations/{tcp_id}")
        t.assertEqual(fetched, tcp_allocation)
        listed = api_json(client, "GET", "/v1/allocations")
        t.assertEqual([item["id"] for item in listed], [tcp_id])
        api(client, "GET", f"/v1/allocations/{tcp_id}/binding", expected_status=404)

        _, body = api(
            client,
            "PUT",
            f"/v1/allocations/{tcp_id}/binding",
            {"host": "backend.invalid", "target_port": PRIMARY_PORT},
            400,
        )
        t.assertEqual(body, "InvalidHost")
        api(
            client,
            "PUT",
            f"/v1/allocations/{tcp_id}/binding",
            {"host": BACKEND_HOST4, "target_port": 0},
            400,
        )

        put_binding(client, tcp_id, BACKEND_HOST4, PRIMARY_PORT)
        if IS_NETLINK:
            ruleset = relay.succeed("nft --handle list table inet relayd_e2e")
            t.assertIn("fib daddr type local", ruleset)
        t.assertEqual(probe(client, "tcp", tcp_port, "primary", "first"), "primary:first")
        put_binding(client, tcp_id, BACKEND_HOST4, SECONDARY_PORT)
        t.assertEqual(probe(client, "tcp", tcp_port, "secondary", "rebound"), "secondary:rebound")

        api(client, "DELETE", f"/v1/allocations/{tcp_id}/binding", expected_status=204)
        api(client, "GET", f"/v1/allocations/{tcp_id}", expected_status=200)
        api(client, "GET", f"/v1/allocations/{tcp_id}/binding", expected_status=404)
        expect_probe_failure(client, "tcp", tcp_port)
        api(client, "DELETE", f"/v1/allocations/{tcp_id}", expected_status=204)
        api(client, "GET", f"/v1/allocations/{tcp_id}", expected_status=404)

    with subtest("UDP and dual-protocol allocations carry real traffic"):
        udp_allocation = create_bound(client, "udp", BACKEND_HOST4, PRIMARY_PORT)
        t.assertEqual(
            probe(client, "udp", udp_allocation["port"], "primary", "datagram"),
            "primary:datagram",
        )

        both_allocation = create_bound(client, "both", BACKEND_HOST4, PRIMARY_PORT)
        both_id = both_allocation["id"]
        both_port = both_allocation["port"]
        t.assertEqual(probe(client, "tcp", both_port, "primary", "dual-tcp"), "primary:dual-tcp")
        t.assertEqual(probe(client, "udp", both_port, "primary", "dual-udp"), "primary:dual-udp")
        both_row = next(
            item for item in api_json(client, "GET", "/v1/ports") if item["id"] == both_id
        )
        t.assertEqual(both_row["protocol"], "both")
        t.assertEqual(both_row["port"], both_port)

    with subtest("IPv6 upstream uses the supported runtime data planes"):
        # A proxy UDP session socket intentionally uses PROXY_LISTEN_HOST as its
        # source family. With this topology's IPv4 listener, cross-family IPv6
        # upstreams are therefore a TCP seam. Netlink DNAT supports both.
        ipv6_protocol = "both" if IS_NETLINK else "tcp"
        ipv6_allocation = create_bound(client, ipv6_protocol, BACKEND_HOST6, IPV6_PORT)
        ipv6_relay_host = RELAY_HOST6 if IS_NETLINK else RELAY_HOST4
        t.assertEqual(
            probe(client, "tcp", ipv6_allocation["port"], "ipv6", "v6-tcp", ipv6_relay_host),
            "ipv6:v6-tcp",
        )
        if IS_NETLINK:
            t.assertEqual(
                probe(client, "udp", ipv6_allocation["port"], "ipv6", "v6-udp", ipv6_relay_host),
                "ipv6:v6-udp",
            )
        api(client, "DELETE", f"/v1/allocations/{ipv6_allocation['id']}", expected_status=204)
        expect_probe_failure(client, "tcp", ipv6_allocation["port"], ipv6_relay_host)
        if IS_NETLINK:
            expect_probe_failure(client, "udp", ipv6_allocation["port"], ipv6_relay_host)

    with subtest("Prometheus and JSON metrics expose the runtime contract"):
        metrics = api_json(client, "GET", "/v1/metrics")
        for field in (
            "allocations_total",
            "runtime_apply_total",
            "tcp_upstream_connect_total",
            "udp_packets_in_total",
        ):
            t.assertIn(field, metrics)
        if not IS_NETLINK:
            t.assertGreaterEqual(metrics["runtime_apply_total"], 1)
            t.assertGreaterEqual(metrics["tcp_upstream_connect_total"], 1)
            t.assertGreaterEqual(metrics["udp_packets_in_total"], 1)
        first_scrape = prometheus(client)
        t.assertIn("# TYPE relayd_connections_current gauge", first_scrape)
        if IS_NETLINK:
            t.assertNotIn("protocol=\"tcp\"", first_scrape)
            t.assertNotIn("protocol=\"udp\"", first_scrape)
        else:
            for protocol in ("tcp", "udp"):
                t.assertEqual(
                    prometheus_value(
                        first_scrape,
                        "relayd_rx_bytes_per_second",
                        both_port,
                        protocol,
                    ),
                    0.0,
                )
            payload = "x" * 512
            probe(client, "tcp", both_port, "primary", payload)
            probe(client, "udp", both_port, "primary", payload)
            time.sleep(1)
            second_scrape = prometheus(client)
            for protocol in ("tcp", "udp"):
                t.assertGreater(
                    prometheus_value(
                        second_scrape,
                        "relayd_rx_bytes_per_second",
                        both_port,
                        protocol,
                    ),
                    0.0,
                )
                t.assertGreater(
                    prometheus_value(
                        second_scrape,
                        "relayd_tx_bytes_per_second",
                        both_port,
                        protocol,
                    ),
                    0.0,
                )

    with subtest("legacy compatibility API remains functional"):
        legacy = api_json(
            client,
            "POST",
            "/v1/ports",
            {"protocol": "tcp", "target_port": PRIMARY_PORT},
            201,
        )
        t.assertEqual(legacy["runtime_status"], "rejecting_no_host")
        legacy = api_json(
            client,
            "POST",
            "/v1/ports/target",
            {"id": legacy["id"], "host": BACKEND_HOST4},
            200,
        )
        t.assertEqual(legacy["runtime_status"], "active")
        t.assertEqual(probe(client, "tcp", legacy["port"], "primary", "legacy"), "primary:legacy")
        legacy = api_json(
            client,
            "POST",
            f"/v1/ports/{legacy['id']}",
            {"target_port": SECONDARY_PORT},
            200,
        )
        t.assertEqual(
            probe(client, "tcp", legacy["port"], "secondary", "legacy-update"),
            "secondary:legacy-update",
        )
        _, body = api(client, "POST", f"/v1/ports/{legacy['id']}", {}, 400)
        t.assertEqual(body, "empty update")
        t.assertIn(legacy["id"], [item["id"] for item in api_json(client, "GET", "/v1/ports")])
        api(client, "DELETE", f"/v1/ports/{legacy['id']}", expected_status=204)
        expect_probe_failure(client, "tcp", legacy["port"])

    with subtest("documented validation, not-found, and conflict statuses"):
        _, body = api(client, "POST", "/v1/allocations", {"protocol": "icmp"}, 400)
        t.assertEqual(body, "invalid protocol")
        api(client, "POST", "/v1/ports", {"protocol": "tcp", "target_port": 70000}, 400)
        api(client, "GET", "/v1/allocations/missing", expected_status=404)
        api(client, "GET", "/v1/allocations/missing/binding", expected_status=404)
        api(
            client,
            "PUT",
            "/v1/allocations/missing/binding",
            {"host": BACKEND_HOST4, "target_port": PRIMARY_PORT},
            404,
        )
        api(client, "DELETE", "/v1/allocations/missing", expected_status=404)

    with subtest("SQLite restart restore preserves identity and both data planes"):
        persisted_id = both_id
        persisted_port = both_port
        relay.systemctl("restart relayd.service")
        relay.wait_for_unit("relayd.service")
        client.wait_for_open_port(API_PORT, API_HOST)
        restored = api_json(client, "GET", f"/v1/allocations/{persisted_id}")
        t.assertEqual(restored["id"], persisted_id)
        t.assertEqual(restored["port"], persisted_port)
        restored_binding = wait_for_active(client, persisted_id, timeout=30)
        t.assertEqual(restored_binding["host"], BACKEND_HOST4)
        t.assertEqual(restored_binding["target_port"], PRIMARY_PORT)
        t.assertEqual(probe(client, "tcp", persisted_port, "primary", "restored-tcp"), "primary:restored-tcp")
        t.assertEqual(probe(client, "udp", persisted_port, "primary", "restored-udp"), "primary:restored-udp")

    with subtest("finite allocation range returns 409 when exhausted"):
        exhausted_ids = []
        conflict_seen = False
        for _ in range(32):
            status, body = api(
                client,
                "POST",
                "/v1/allocations",
                {"protocol": "both"},
                expected_status=None,
            )
            if status == 201:
                exhausted_ids.append(json.loads(body)["id"])
                continue
            t.assertEqual(status, 409, body)
            t.assertEqual(body, "NoAvailablePort")
            conflict_seen = True
            break
        t.assertTrue(conflict_seen, "allocation range did not report exhaustion")
        for allocation_id in exhausted_ids:
            api(client, "DELETE", f"/v1/allocations/{allocation_id}", expected_status=204)

    with subtest("deletion removes listeners, resources, and Prometheus labels"):
        for allocation in (udp_allocation, both_allocation):
            api(client, "DELETE", f"/v1/allocations/{allocation['id']}", expected_status=204)
        expect_probe_failure(client, "tcp", persisted_port)
        expect_probe_failure(client, "udp", persisted_port)
        remaining = api_json(client, "GET", "/v1/allocations")
        t.assertEqual(remaining, [])
        final_scrape = prometheus(client)
        t.assertNotIn(f'port="{persisted_port}"', final_scrape)

    if IS_NETLINK:
        with subtest("netlink mode programmed a real inet NAT chain"):
            ruleset = relay.succeed("nft --handle list table inet relayd_e2e")
            t.assertIn("chain mapping", ruleset)

    relay.succeed("systemctl --no-pager --full status relayd.service")
  '';
}
