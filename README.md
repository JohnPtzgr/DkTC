# DkTC: Distributed k-Tree Computing

**A research platform for large-scale distributed computing and machine learning.**

## Overview

DkTC is an experimental framework exploring the limits of coordination in massive distributed networks. The core research question: **What is the minimum communication cost for global consensus across a large network of heterogeneous devices?**

## Research Contributions

### 1. Inverted Stochastic Gradient Descent

Traditional distributed SGD updates all parameters on a small batch. DkTC inverts this: each node updates **one parameter** using the **entire batch**, achieving O(1) communication per node while maintaining full-batch convergence properties.

### 2. DCG + Tree Hybrid Topology

DkTC combines a **Dense Connection Graph** (DCG) for local gossip-based consensus with a **k-ary tree** for global coordination. The DCG provides statistical variance reduction through ~20 log n random neighbors; the tree provides bias correction through hierarchical aggregation. Both channels have O(k) communication complexity, where k ≈ log n / log log n.

### 3. Optimal Degree-Diameter Tradeoff

The k-ary tree uses k ≈ log n / log log n, achieving s = Δ + D ≈ 2 log n / log log n — asymptotically matching the Moore bound for connected graphs. This is provably near-optimal.

### 4. Time-Decayed Asynchronous Consensus

Gradients and momentums are exchanged with exponential time decay, eliminating synchronization barriers and enabling fault tolerance through natural forgetting.

## Research Questions

| Question | Status |
|----------|--------|
| Can inverted SGD converge with gossip consensus? | Simulation validated |
| Does O(k) communication hold at scale? | Theoretical analysis complete |
| What is the true convergence rate? | Open (requires formal proof) |
| How does this compare to existing systems? | Pending (requires benchmarking) |

## Current Status

- **Simulation**: Converges on synthetic data
- **Theory**: O(k) communication complexity, near-optimal degree/diameter
- **Implementation**: Framework design, simulation validation
- **Open problems**: Formal convergence proof, real-world deployment, baseline comparison

## Intended Use

This is a **research platform**, not a production system. It is intended for:

- Exploring distributed optimization algorithms
- Experimenting with large-scale network topologies
- Validating theoretical bounds empirically
- Publishing novel results in distributed systems and ML

## Related Work

- GoSGD (2016): Gossip-based SGD with full parameter exchange
- DeepSpeed (2020): ZeRO-3 for large models on GPU clusters
- Federated Learning (2016): Privacy-preserving training on mobile devices
- Moore Bound: Theoretical limit for degree-diameter tradeoff
