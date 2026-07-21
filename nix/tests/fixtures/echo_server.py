#!/usr/bin/env python3
"""Small dual-protocol echo server used by the NixOS black-box tests."""

import argparse
import socket
import threading


def reply(label: str, payload: bytes) -> bytes:
    return label.encode("utf-8") + b":" + payload


def serve_tcp(sock: socket.socket, label: str) -> None:
    while True:
        connection, _ = sock.accept()
        threading.Thread(
            target=handle_tcp,
            args=(connection, label),
            daemon=True,
        ).start()


def handle_tcp(connection: socket.socket, label: str) -> None:
    with connection:
        chunks = []
        while True:
            chunk = connection.recv(65535)
            if not chunk:
                break
            chunks.append(chunk)
        payload = b"".join(chunks)
        if payload:
            connection.sendall(reply(label, payload))


def serve_udp(sock: socket.socket, label: str) -> None:
    while True:
        payload, peer = sock.recvfrom(65535)
        sock.sendto(reply(label, payload), peer)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--label", required=True)
    args = parser.parse_args()

    family = socket.AF_INET6 if ":" in args.host else socket.AF_INET
    tcp = socket.socket(family, socket.SOCK_STREAM)
    udp = socket.socket(family, socket.SOCK_DGRAM)
    for sock in (tcp, udp):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((args.host, args.port))
    tcp.listen(128)

    threading.Thread(target=serve_tcp, args=(tcp, args.label), daemon=True).start()
    serve_udp(udp, args.label)


if __name__ == "__main__":
    main()
