import sys, json, re, collections
from datetime import datetime
rows=[]
for line in open(sys.argv[1]):
    try: j=json.loads(line)
    except Exception: continue
    rows.append((datetime.fromisoformat(j["timestamp"].replace("Z","+00:00")), j["level"], j["message"]))
prog=[(t,int(re.search(r'blockNumber\W+(\d+)',m).group(1)),int(re.search(r'Node block: (\d+)',m).group(1))) for t,l,m in rows if m.startswith('Progressed to position')]
print("span:", rows[0][0].strftime('%H:%M:%S'), "->", rows[-1][0].strftime('%H:%M:%S'), f"({(rows[-1][0]-rows[0][0]).total_seconds()/60:.1f} min)")
if len(prog)>1:
    mins=(prog[-1][0]-prog[0][0]).total_seconds()/60; blocks=prog[-1][1]-prog[0][1]
    print(f"progress: {prog[0][1]} -> {prog[-1][1]} = {blocks} blocks in {mins:.1f} min = {blocks/mins:.1f}/min; tip {prog[-1][2]}, behind by {prog[-1][2]-prog[-1][1]}")
lv=collections.Counter(l for _,l,_ in rows); print("levels:", dict(lv))
pauses=collections.Counter(); pause_s=collections.Counter(); kinds=collections.Counter(); conn_ev=collections.Counter(); other=[]
for t,l,m in rows:
    if l not in ('warn','error'): continue
    c=re.search(r'connection number (\d+)',m); ci=c.group(1) if c else '?'
    if 'Pausing requests' in m:
        pauses[ci]+=1; pause_s[ci]+=int(re.search(r'for (\d+) ms',m).group(1))/1000
        if 'too busy' in m: kinds[('tooBusy',ci)]+=1
        elif 'per 10s' in m: kinds[('cluster 10s',ci)]+=1
        elif 'per 60s' in m: kinds[('cluster 60s',ci)]+=1
        elif 'per 3600s' in m: kinds[('cluster 3600s',ci)]+=1
        else: kinds[('other:'+m[:60],ci)]+=1
    elif 'failed:' in m or 're-established' in m or 'failed to reconnect' in m or 'could not be opened' in m or 'could not be re-established' in m:
        k=re.sub(r'\d+ ms','N ms',re.sub(r'\(attempt.*','',m.split(': ',1)[-1]))[:70]; conn_ev[(k,ci)]+=1
    else: other.append((t.strftime('%H:%M:%S'),l,m[:200]))
print("pauses per connection (see URL order):", dict(pauses), "| paused seconds:", {k:round(v) for k,v in pause_s.items()})
print("pause kinds:"); [print(f"   {k[0]:<14} conn {k[1]}: {v}") for k,v in sorted(kinds.items())]
print("connection events:"); [print(f"   conn {k[1]}: {v}x {k[0]}") for k,v in sorted(conn_ev.items(), key=lambda x:-x[1])]
print("other warn/error lines:", len(other)); [print("  ",o) for o in other[:8]]
# batches
b=[]; cur=None
for t,l,m in rows:
    if m.startswith('Fetching transfers'): cur={'s':t,'p':0}
    elif cur and 'Pausing' in m: cur['p']+=1
    elif cur and m.startswith('Progressed'): b.append(((t-cur['s']).total_seconds(),cur['p'])); cur=None
if b:
    d=sorted(x[0] for x in b); np=[x for x in b if x[1]==0]
    print(f"batches={len(b)} dur_s min={d[0]:.0f} median={d[len(d)//2]:.0f} p90={d[int(len(d)*.9)]:.0f} max={d[-1]:.0f}; no-pause batches={len(np)} ({100*len(np)/len(b):.0f}%) median {sorted(x[0] for x in np)[len(np)//2] if np else 0:.0f}s")
q=[m for _,_,m in rows if 'quota for connection' in m]
if q: print("startup quota:", re.sub(r'\\"','"',q[0])[:400])
