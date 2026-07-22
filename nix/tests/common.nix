{
  pkgs,
  relaydPackage,
  runtimeMode,
  portRange ? "20000-20031",
  withIperf ? false,
  tcpSplice ? false,
}:
let
  lib = pkgs.lib;
  isNetlink = runtimeMode == "netlink";
  echoServer = ./fixtures/echo_server.py;
  probe = pkgs.writeShellScriptBin "relayd-probe" ''
    exec ${pkgs.python3}/bin/python3 ${./fixtures/network_probe.py} "$@"
  '';

  constants = {
    token = "nixos-e2e-token";
    apiPort = 18080;
    client4 = "192.168.10.2";
    relayClient4 = "192.168.10.1";
    relayBackend4 = "192.168.20.1";
    backend4 = "192.168.20.2";
    client6 = "fd00:10::2";
    relayClient6 = "fd00:10::1";
    relayBackend6 = "fd00:20::1";
    backend6 = "fd00:20::2";
    primaryPort = 19001;
    secondaryPort = 19002;
    ipv6Port = 19003;
    iperfPort = 5201;
  };

  address = value: {
    address = value;
    prefixLength = if lib.hasInfix ":" value then 64 else 24;
  };

  defaultRoutes = gateway4: gateway6: {
    ipv4.routes = lib.mkForce [
      {
        address = "0.0.0.0";
        prefixLength = 0;
        via = gateway4;
      }
    ];
    ipv6.routes = lib.mkForce [
      {
        address = "::";
        prefixLength = 0;
        via = gateway6;
      }
    ];
  };

  mkEchoService =
    {
      host,
      port,
      label,
    }:
    {
      description = "relayd E2E ${label} echo fixture";
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ];
      serviceConfig = {
        DynamicUser = true;
        ExecStart = "${pkgs.python3}/bin/python3 ${echoServer} --host ${host} --port ${toString port} --label ${label}";
        Restart = "on-failure";
      };
    };
in
{
  inherit constants probe;

  nodes = {
    client =
      { lib, pkgs, ... }:
      {
        virtualisation = {
          vlans = [ 1 ];
          memorySize = if withIperf then 768 else 512;
          cores = if withIperf then 2 else 1;
        };
        networking = {
          hostName = "client";
          useDHCP = false;
          firewall.enable = false;
          interfaces.eth1 = lib.recursiveUpdate {
            ipv4.addresses = lib.mkForce [ (address constants.client4) ];
            ipv6.addresses = lib.mkForce [ (address constants.client6) ];
          } (defaultRoutes constants.relayClient4 constants.relayClient6);
        };
        environment.systemPackages = [
          pkgs.curl
          pkgs.iperf3
          pkgs.jq
          probe
        ];
      };

    relay =
      { lib, pkgs, ... }:
      {
        virtualisation = {
          vlans = [
            1
            2
          ];
          memorySize = if withIperf then 1024 else 768;
          cores = if withIperf then 2 else 1;
        };
        networking = {
          hostName = "relay";
          useDHCP = false;
          firewall.enable = false;
          interfaces.eth1 = {
            ipv4.addresses = lib.mkForce [ (address constants.relayClient4) ];
            ipv6.addresses = lib.mkForce [ (address constants.relayClient6) ];
          };
          interfaces.eth2 = {
            ipv4.addresses = lib.mkForce [ (address constants.relayBackend4) ];
            ipv6.addresses = lib.mkForce [ (address constants.relayBackend6) ];
          };
        };
        boot = {
          kernelModules = lib.optionals isNetlink [
            "nf_tables"
            "nft_nat"
          ];
          kernel.sysctl = {
            "net.ipv4.ip_forward" = 1;
            "net.ipv6.conf.all.forwarding" = 1;
          };
        };
        environment.systemPackages = [
          pkgs.curl
          pkgs.iproute2
          pkgs.nftables
          relaydPackage
        ];
        systemd.services.relayd = {
          description = "relayd under NixOS E2E test";
          wantedBy = [ "multi-user.target" ];
          wants = [ "network-online.target" ];
          after = [ "network-online.target" ];
          environment = {
            AUTH_TOKEN = constants.token;
            HTTP_LISTEN = "0.0.0.0:${toString constants.apiPort}";
            PROXY_LISTEN_HOST = "0.0.0.0";
            PORT_RANGE = portRange;
            SQLITE_PATH = "/var/lib/relayd/relayd.sqlite3";
            RELAYD_RUNTIME_MODE = runtimeMode;
            RELAYD_NFTABLES_TABLE = "relayd_e2e";
            RELAYD_NFTABLES_CHAIN = "mapping";
            RUNTIME_APPLY_TIMEOUT_MS = "5000";
            RESTORE_SWEEP_TIMEOUT_MS = "30000";
            TCP_SPLICE_ENABLED = if tcpSplice then "true" else "false";
            RUST_LOG = "relayd=debug";
          };
          serviceConfig = {
            ExecStart = "${relaydPackage}/bin/relayd";
            Restart = "on-failure";
            RestartSec = "1s";
            StateDirectory = "relayd";
            StateDirectoryMode = "0700";
          }
          // lib.optionalAttrs isNetlink {
            AmbientCapabilities = [ "CAP_NET_ADMIN" ];
            CapabilityBoundingSet = [ "CAP_NET_ADMIN" ];
          };
        };
      };

    backend =
      { lib, pkgs, ... }:
      {
        virtualisation = {
          vlans = [ 2 ];
          memorySize = if withIperf then 768 else 512;
          cores = if withIperf then 2 else 1;
        };
        networking = {
          hostName = "backend";
          useDHCP = false;
          firewall.enable = false;
          interfaces.eth1 = lib.recursiveUpdate {
            ipv4.addresses = lib.mkForce [ (address constants.backend4) ];
            ipv6.addresses = lib.mkForce [ (address constants.backend6) ];
          } (defaultRoutes constants.relayBackend4 constants.relayBackend6);
        };
        environment.systemPackages = [
          pkgs.iperf3
          pkgs.python3
        ];
        systemd.services = {
          "relayd-echo-primary" = mkEchoService {
            host = constants.backend4;
            port = constants.primaryPort;
            label = "primary";
          };
          "relayd-echo-secondary" = mkEchoService {
            host = constants.backend4;
            port = constants.secondaryPort;
            label = "secondary";
          };
          "relayd-echo-ipv6" = mkEchoService {
            host = constants.backend6;
            port = constants.ipv6Port;
            label = "ipv6";
          };
        }
        // lib.optionalAttrs withIperf {
          "relayd-iperf3" = {
            description = "relayd E2E iperf3 backend";
            wantedBy = [ "multi-user.target" ];
            wants = [ "network-online.target" ];
            after = [ "network-online.target" ];
            serviceConfig = {
              DynamicUser = true;
              ExecStart = "${pkgs.iperf3}/bin/iperf3 --server --port ${toString constants.iperfPort}";
              Restart = "always";
              RestartSec = "1s";
            };
          };
        };
      };
  };
}
