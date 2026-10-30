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

Spec sheet: NVIDIA GB300 NVL72 compute tray

| | Compute tray |
|---|---|
| Overview | 1U liquid-cooled with 2 NVIDIA GB300 Grace Blackwell Superchips |
| CPU and GPU | 2 72-core NVIDIA Grace Arm Neoverse V2 CPUs, 4 NVIDIA B300 Tensor Core GPUs |
| GPU Memory | 1.15 TB HBM3e per compute tray |
| CPU Memory | 960 GB LPDDR5X per compute tray |
| Networking | 4 NVLink Switch ports; 4 integrated ConnectX-8 SuperNICs, up to 800 Gb/s; up to 2 BlueField-3 DPUs |
| Storage | Up to 8 E1.S PCIe 5.0 drives |
| Power | Shared power through 4+4 rack power shelves |

Spec sheet: [NVIDIA GB300 NVL compute tray](https://docs.nvidia.com/enterprise-reference-architectures/nvl72-ai-factory/latest/components.html#nvidia-gb300-nvl-compute-tray) ("System Hardware & Components", NVIDIA NVL72 AI Factory enterprise reference architecture; component table, read 2026-10-01)

| Component | Quantity |
|---|---|
| NVIDIA Grace processor, 72 Arm Neoverse V2 cores, connected via NVLink C2C, 1 TB aggregated LPDDR5 CPU main memory | 2 |
| NVIDIA B300 GPU, 1,152 GB aggregated HBM3 memory | 4 |
| NVIDIA ConnectX-8 Mezzanine Boards, 2 ConnectX-8 network adapters each | 2 |
| Dual-port QSFP112 NVIDIA BlueField-3 B3240 DPU | 1 |

The same document adds: 4 Blackwell Ultra GPUs and 2 Grace CPUs per tray, 1 M.2 NVMe device for the
OS, and 4 E1.S NVMe devices "typically used as a fast, local cache" (no capacity given).

No AWS instance type exists yet for a GB300 NVL72 tray (unlike `p6e-gb200.36xlarge` for
GB200), so `nvidia/gb300-tray.yaml` is derived from the two compute tray sheets above:

- CPU: 2 Grace x 72 cores = 144. The component table's "72 cores" line reads as 72 in total, but
  the other sheet ("2 72-core Grace CPUs"), the rack (36 Grace, 2,592 cores) and `aicr`'s GB300
  topology (CPUs 0-71 and 72-143) all give 72 per CPU. GPUs: 4.
- GPU memory per GPU: 1,152 GB aggregated / 4 = 288 GB exactly, written as 274658 MiB (decimal GB,
  the same conversion `gb200-tray` uses). The rack-level figure above (20 TB, 279 GB per GPU) is
  older and was not used.
- Memory: 960 GB LPDDR5X per tray, written as `960Gi` to match `gb200-tray` (both are two 480 GB
  Grace memories, and AWS lists the GB200 instance in GiB). Read as decimal GB it would be about
  894Gi. The component table's "1 TB" is a rounding of that: 18 trays x 960 GB = 17.3 TB matches
  the rack's 17 TB, where 18 x 1 TiB would be 18.4 TB. The same page lists its ARM control node
  (one Grace) at 480 GB LPDDR5X on-module, which matches two Grace at 2 x 480 GB per tray.
- Ephemeral storage and pods: neither sheet gives capacity (storage is 4 to 8 E1.S drives plus an
  M.2 OS drive, no sizes), so they are copied from `p6e-gb200.36xlarge` as placeholders.
