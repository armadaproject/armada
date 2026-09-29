# Node Profiles

A node profile is a configuration used by Regatta to represent real hardware. It is parsed to create kwok nodes.

# Calculating Pods

Pod calculations depend on your hardware and configuration.

For AWS, this is calculated using:
```
N * (M-1) + 2
```

- N is the number of Elastic Network Interfaces (ENI) of the instance type
- M is the number of IP addresses of a single ENI

For a p6e-gb200.36xlarge, this is 39 * (50-1) + 2 = 1913.

This is a theoretical limit of max pods.

# References

Spec sheets: [Specifications for Amazon EC2 accelerated computing instances](https://docs.aws.amazon.com/ec2/latest/instancetypes/ac.html)

## AWS
#### Network

| Instance type | Baseline / Burst bandwidth (Gbps) | EFA | ENA | ENA Express | ENA queues per interface (Default/Maximum) | Network cards | Max. network interfaces | IP addresses per interface | IPv6 |
|---|---|---|---|---|---|---|---|---|---|
| p6e-gb200.36xlarge | 3200 Gigabit | ✓ Yes | ✓ Yes | ✓ Yes | 32 | 17 | 39 | 50 | ✓ Yes |

#### EBS

| Instance type | Baseline / Maximum bandwidth (Mbps) | Baseline / Maximum throughput (MB/s, 128 KiB I/O) | Baseline / Maximum IOPS (16 KiB I/O) | NVMe | Multiple EBS cards | EBS volume limit |
|---|---|---|---|---|---|---|
| p6e-gb200.36xlarge | 60000.00 | 7500.00 | 240000.00 | ✓ Yes | ✗ No | 64 (Dedicated limit) |

#### Instance store

| Instance type | Instance store volumes | Instance store type | 100% random read IOPS / Write IOPS | Needs initialization | TRIM support |
|---|---|---|---|---|---|
| p6e-gb200.36xlarge | 3 x 7500 GB | NVMe SSD | 2,550,000 / 2,400,000 | | ✓ Yes |

## Nvidia

Spec sheet: [NVIDIA GB200 NVL72](https://www.nvidia.com/en-us/data-center/gb200-nvl72/)

GB200 NVL72 Specs

| | GB200 NVL72 | GB200 Grace Blackwell Superchip |
|---|---|---|
| Configuration | 36 Grace CPU \| 72 Blackwell GPUs | 1 Grace CPU \| 2 Blackwell GPU |
| GPU Memory / Bandwidth | 13.4 TB HBM3e / 576 TB/s | 372 GB HBM3e / 16 TB/s |
| NVLink Bandwidth | 130 TB/s | 3.6 TB/s |
| CPU Core Count | 2,592 Arm® Neoverse V2 cores | 72 Arm Neoverse V2 cores |
| CPU Memory / Bandwidth | 17 TB LPDDR5X / 14 TB/s | Up to 480 GB LPDDR5X / Up to 512 GB/s |

Spec sheet: [NVIDIA GB300 NVL72](https://www.nvidia.com/en-us/data-center/gb300-nvl72/)

GB300 NVL72 Specs

| | GB300 NVL72 | Individual Blackwell Ultra GPU |
|---|---|---|
| Configuration | 36 Grace CPUs \| 72 Blackwell Ultra GPUs | - |
| CPU Core Count | 2,592 Arm Neoverse V2 cores | - |
| GPU Memory / Bandwidth | 20 TB / Up to 576 TB/s | 279 GB HBM3e / 8 TB/s |
| NVLink Bandwidth | 130 TB/s | 1.8 TB/s (Fifth-Gen) |
| CPU Memory / Bandwidth | 17 TB LPDDR5X / 14 TB/s | - |

No AWS instance type exists yet for a GB300 NVL72 tray (unlike `p6e-gb200.36xlarge` for
GB200), so `nvidia/gb300-tray.yaml` is derived from the specs above (rack totals ÷ 18
trays, GPU memory from the per-GPU column) rather than a real cloud spec sheet. Replace
it once a real instance type ships.