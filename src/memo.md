# tc: create fake latency

## Add latency of 120ms

```
tc qdisc add dev eth0 root netem delay 120ms
```

## Remove latency

```
tc qdisc del dev eth0 root netem
```

## List rules

```
tc -s qdisc
```
