# Home network latency (lag in browsing and YouTube)

## Problem

Occasional lag on page loads and YouTube, felt as latency rather than low
throughput. Torrents could still reach ~10 MB/s; capping them to 3 MB/s did
not help, and the YouTube issues later appeared with no torrents running.

## Setup

```
fiber ─ Nokia G-2425G-A (ISP ONT/router, 192.168.1.1, routes; TV box on it)
          └─ in-wall cable ~10 m ─ [old switch] ─ in-wall cable ~15 m
               └─ TP-Link Archer AX55 (access point mode, acts as a switch)
                    └─ PC (enp3s0, 1 Gbps)
```

Plan: 600/400 Mbps max, 480/320 Mbps typical.

## Root cause

The old switch (SmartBit SBN 108SK, ~15 years, 10/100) only joined the two
in-wall cables. It held the Nokia LAN link at 100 Mbps, capping the 600/400
line at ~94 Mbps. Traffic bursts (YouTube segment fetches, torrent upload)
filled that link and queued, so latency jumped under load (bufferbloat).

## Investigation

- Idle: gateway ~1 ms, internet ~3 ms, no loss; DNS lookups 3–22 ms. Not the
  cause.
- YouTube: video cache mapped inside the ISP (`vivacom-sof3`); IPv4 and IPv6
  paths healthy. Not the cause.
- Latency under load (ping 1.1.1.1 while running parallel
  `speed.cloudflare.com` transfers via curl) showed the problem:

  | | Throughput | Ping under load |
  |---|---|---|
  | Download | 72–86 Mbps | avg 19–34 ms, max 58 ms |
  | Upload | 92 Mbps | avg 80 ms, max 194 ms |

- Topology: only one private hop in the trace, so the Archer was not routing
  (AP mode disables routing and QoS on all ports, wired ones too). The Nokia's
  QoS page only classifies and marks traffic and has no rate limit, so it
  cannot fix bufferbloat.
- Nokia optics healthy: Rx -20.37 dBm (alarm at -28.9), Tx 2.42 dBm,
  37 °C.
- Nokia LAN status showed both used ports at 100 Mbps. One is the TV box
  (normal); the other was the switch. That, plus the plan speed, pointed to
  the switch.

## Fix

Replaced the switch with an RJ45 coupler (Goobay 96100, Cat6, keystone
form factor; ~25 m of cable in total). Nokia link now 1000 Mbps full duplex.

| | Before | After |
|---|---|---|
| Download | 72–86 Mbps | 584 (IPv4) / 608 (IPv6) Mbps |
| Upload | 92 Mbps | 412 Mbps |
| Ping under download | avg 19–34 ms | avg ~2.8 ms |
| Ping under upload | avg 80 ms, max 194 ms | avg 2.9 ms, max 4.8 ms |

The Nokia showed 7 "Errors Received" after the cables were replugged; the
count did not grow over several GB of test traffic, so it came from the
replug. No QoS or Archer mode change needed.

## If it recurs

- Rerun the latency-under-load test; ping rising well above ~5 ms under load
  means a queue is building again.
- Check the Nokia LAN port speed (should be 1000) and its error counter
  (baseline 7).
- Fallback if bufferbloat returns at full line rate: Archer in router mode
  with QoS at ~90% of the measured speed. The AX55 may struggle to shape
  ~500 Mbps with QoS on, so measure before keeping it.
