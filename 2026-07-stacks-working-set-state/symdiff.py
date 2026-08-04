#!/usr/bin/env python3
"""Diff two jemalloc heap_v2 profiles and symbolize stacks via local lib copies."""
import re, subprocess, sys, bisect, functools
RATE = 2097152

ROOT = sys.argv[3] if len(sys.argv) > 3 else "root"

def parse(path):
    stacks = {}   # tuple(addrs) -> live bytes (scaled not needed for diff consistency)
    maps = []     # (start, end, offset, lib)
    cur = None
    in_maps = False
    for line in open(path):
        line = line.rstrip("\n")
        if line.startswith("MAPPED_LIBRARIES"):
            in_maps = True
            continue
        if in_maps:
            m = re.match(r"([0-9a-f]+)-([0-9a-f]+) (\S+) ([0-9a-f]+) \S+ \d+ +(/\S+)?", line)
            if m and m.group(5) and "x" in m.group(3):
                maps.append((int(m.group(1), 16), int(m.group(2), 16),
                             int(m.group(4), 16), m.group(5)))
            continue
        if line.startswith("@"):
            cur = tuple(line.split()[1:])
            continue
        m = re.match(r"\s+t\*: (\d+): (\d+) \[", line)
        if m and cur is not None:
            cnt, byt = int(m.group(1)), int(m.group(2))
            if cnt > 0 and byt > 0:
                # jeprof heap_v2 scaling: each sample of avg size S represents
                # 1/(1-exp(-S/R)) allocations at sample rate R
                import math
                s = byt / cnt
                scale = 1.0 / (1.0 - math.exp(-s / RATE))
                stacks[cur] = stacks.get(cur, 0) + byt * scale
            cur = None
    maps.sort()
    return stacks, maps

def find_map(maps, addr):
    i = bisect.bisect_right([m[0] for m in maps], addr) - 1
    if i >= 0 and addr < maps[i][1]:
        return maps[i]
    return None

@functools.lru_cache(maxsize=None)
def libinfo(lib):
    # is the lib ET_DYN (needs base subtraction)?
    local = ROOT + lib
    try:
        out = subprocess.run(["readelf", "-h", local], capture_output=True, text=True).stdout
        return ("DYN" in out, local)
    except Exception:
        return (True, None)

sym_cache = {}
def symbolize(maps, addr_hex):
    addr = int(addr_hex, 16)
    m = find_map(maps, addr)
    if not m:
        return f"[jit/anon {addr_hex}]"
    start, end, offset, lib = m
    is_dyn, local = libinfo(lib)
    if not local:
        return f"[{lib.split('/')[-1]}+? {addr_hex}]"
    file_off = addr - start + offset if is_dyn else addr
    key = (lib, file_off)
    if key in sym_cache:
        return sym_cache[key]
    out = subprocess.run(["addr2line", "-f", "-C", "-e", local, hex(file_off)],
                         capture_output=True, text=True).stdout.splitlines()
    func = out[0] if out else "??"
    if func == "??":
        # fall back to nearest symbol via nm
        func = nearest_nm(local, file_off) or "??"
    res = f"{lib.split('/')[-1]}!{func}"
    sym_cache[key] = res
    return res

@functools.lru_cache(maxsize=None)
def nm_table(local):
    syms = []
    for src in (["nm", "-C", "--defined-only", local], ["nm", "-D", "-C", "--defined-only", local]):
        out = subprocess.run(src, capture_output=True, text=True).stdout
        for l in out.splitlines():
            p = l.split(" ", 2)
            if len(p) == 3 and p[1].lower() in ("t", "w"):
                try:
                    syms.append((int(p[0], 16), p[2]))
                except ValueError:
                    pass
        if syms:
            break
    syms.sort()
    return syms

def nearest_nm(local, off):
    syms = nm_table(local)
    if not syms:
        return None
    i = bisect.bisect_right([s[0] for s in syms], off) - 1
    if i >= 0 and off - syms[i][0] < 0x100000:
        return syms[i][1]
    return None

base_stacks, base_maps = parse(sys.argv[1])
new_stacks, new_maps = parse(sys.argv[2])

deltas = []
for st, b in new_stacks.items():
    d = b - base_stacks.get(st, 0)
    if d != 0:
        deltas.append((d, st))
for st, b in base_stacks.items():
    if st not in new_stacks:
        deltas.append((-b, st))
deltas.sort(reverse=True)

total = sum(d for d, _ in deltas)
print(f"total delta: {total/2**20:.0f} MiB over {len(deltas)} changed stacks\n")
for d, st in deltas[:12]:
    print(f"=== {d/2**20:+.0f} MiB ===")
    for a in st[:14]:
        print("   ", symbolize(new_maps, a))
    print()
