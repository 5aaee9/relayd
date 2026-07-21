{
  pkgs,
  relayd,
  relayd-netlink,
}:
{
  relayd-nixos-functional = import ./functional.nix {
    inherit pkgs;
    relaydPackage = relayd;
    runtimeMode = "proxy";
  };

  relayd-nixos-netlink = import ./functional.nix {
    inherit pkgs;
    relaydPackage = relayd-netlink;
    runtimeMode = "netlink";
  };

  relayd-nixos-performance-proxy = import ./performance.nix {
    inherit pkgs;
    relaydPackage = relayd;
    runtimeMode = "proxy";
    minimumRatio = 0.25;
    maximumUdpLossDelta = 25.0;
  };

  relayd-nixos-performance-netlink = import ./performance.nix {
    inherit pkgs;
    relaydPackage = relayd-netlink;
    runtimeMode = "netlink";
    minimumRatio = 0.50;
    maximumUdpLossDelta = 15.0;
  };
}
