#!/usr/bin/env python3
"""
DkTC - Distributed k-Tree Computing System

A hierarchical distributed computing system designed for billions of nodes
with optimal degree Δ ≈ k and diameter D ≈ log_k n, achieving
s = Δ + D ≈ 2 log n / log log n.

Architecture:
- Hidden Root: API server, node registry, task submission endpoint
- DCG (Dense Connection Graph): ~10k connections per node for gossip/leader election
- k-ary Tree: Task distribution and result aggregation
- Bandwidth-aware recursive tree construction

Author: DkTC Team
Version: 0.1.0
"""

import asyncio
import hashlib
import heapq
import json
import logging
import math
import random
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import (
    Any, Callable, Dict, List, Optional, Set, Tuple, Union,
    DefaultDict, Deque, Iterator, Generator
)

# ============================================================================
# Logging Configuration
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger("DkTC")


# ============================================================================
# Enums
# ============================================================================

class NodeStatus(Enum):
    """Status of a node in the system"""
    BOOTSTRAPPING = "bootstrapping"
    CONNECTING = "connecting"
    ACTIVE = "active"
    LEADER_CANDIDATE = "leader_candidate"
    LEADER = "leader"
    COMPONENT_LEADER = "component_leader"
    ROOT = "root"
    FAILED = "failed"
    LEAVING = "leaving"


class TaskStatus(Enum):
    """Status of a computational task"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTITIONED = "partitioned"
    AGGREGATING = "aggregating"


class ConnectionType(Enum):
    """Type of connection in the graph"""
    DCG = "dcg"  # Dense Connection Graph (gossip)
    TREE_PARENT = "tree_parent"  # Tree parent link
    TREE_CHILD = "tree_child"  # Tree child link
    TREE_CANDIDATE = "tree_candidate"  # Candidate for tree placement


class MessageType(Enum):
    """Types of messages in the system"""
    # Discovery & Membership
    PING = "ping"
    PONG = "pong"
    JOIN_REQUEST = "join_request"
    JOIN_RESPONSE = "join_response"
    LEAVE_NOTIFICATION = "leave_notification"

    # Leader Election
    SCORE_BROADCAST = "score_broadcast"
    LEADER_ANNOUNCE = "leader_announce"
    LEADER_CONFIRM = "leader_confirm"

    # Tree Building
    TREE_BUILD_REQUEST = "tree_build_request"
    TREE_BUILD_RESPONSE = "tree_build_response"
    TREE_ASSIGNMENT = "tree_assignment"

    # Task Distribution
    TASK_SUBMIT = "task_submit"
    TASK_PARTITION = "task_partition"
    TASK_RESULT = "task_result"
    TASK_AGGREGATE = "task_aggregate"

    # Health & Failure Detection
    HEARTBEAT = "heartbeat"
    SUSPECT_NOTIFICATION = "suspect_notification"


# ============================================================================
# Dataclasses
# ============================================================================

@dataclass
class Connection:
    """
    Represents a connection between two nodes.
    Used in DCG for gossip and bandwidth tracking.
    """
    node_id: str
    bandwidth: float
    latency: float = 0.0
    connection_type: ConnectionType = ConnectionType.DCG
    last_updated: float = field(default_factory=time.time)
    success_rate: float = 1.0  # 0.0 to 1.0
    retry_count: int = 0

    def __lt__(self, other: 'Connection') -> bool:
        """For sorting by bandwidth (higher is better)"""
        return self.bandwidth > other.bandwidth

    def update_bandwidth(self, new_bandwidth: float):
        """Update bandwidth with exponential moving average"""
        alpha = 0.3  # Smoothing factor
        self.bandwidth = alpha * new_bandwidth + (1 - alpha) * self.bandwidth
        self.last_updated = time.time()


@dataclass
class NodeInfo:
    """
    Comprehensive information about a node in the system.
    Stored at hidden root and gossiped through the network.
    """
    node_id: str
    address: str
    certificate: str
    status: NodeStatus = NodeStatus.BOOTSTRAPPING
    score: float = 0.0
    parent_id: Optional[str] = None
    children_ids: List[str] = field(default_factory=list)
    component_leader_id: Optional[str] = None
    region: str = "default"
    bandwidth_profile: Dict[str, float] = field(default_factory=dict)
    joined_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    version: int = 1

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for network transmission"""
        return {
            "node_id": self.node_id,
            "address": self.address,
            "certificate": self.certificate,
            "status": self.status.value,
            "score": self.score,
            "parent_id": self.parent_id,
            "children_ids": self.children_ids,
            "component_leader_id": self.component_leader_id,
            "region": self.region,
            "joined_at": self.joined_at,
            "version": self.version
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NodeInfo':
        """Deserialize from dictionary"""
        return cls(
            node_id=data["node_id"],
            address=data["address"],
            certificate=data["certificate"],
            status=NodeStatus(data.get("status", "bootstrapping")),
            score=data.get("score", 0.0),
            parent_id=data.get("parent_id"),
            children_ids=data.get("children_ids", []),
            component_leader_id=data.get("component_leader_id"),
            region=data.get("region", "default"),
            joined_at=data.get("joined_at", time.time())
        )


@dataclass
class Task:
    """
    Represents a computational task in the system.
    Can be partitioned into subtasks for parallel execution.
    """
    task_id: str
    task_type: str
    parameters: Dict[str, Any]
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[Any] = None
    parent_task_id: Optional[str] = None
    subtask_ids: List[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    priority: int = 0
    assigned_node: Optional[str] = None
    timeout: float = 300.0  # 5 minutes default

    def is_expired(self) -> bool:
        """Check if task has exceeded its timeout"""
        return time.time() - self.created_at > self.timeout

    def to_dict(self) -> Dict[str, Any]:
        """Serialize for network transmission"""
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "parameters": self.parameters,
            "status": self.status.value,
            "parent_task_id": self.parent_task_id,
            "subtask_ids": self.subtask_ids,
            "priority": self.priority,
            "timeout": self.timeout
        }


@dataclass
class TreeAssignment:
    """
    Represents an assignment of a candidate node to a parent during tree building.
    Used in the recursive tree construction algorithm.
    """
    parent_id: str
    child_id: str
    bandwidth: float
    level: int
    assignment_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __lt__(self, other: 'TreeAssignment') -> bool:
        """For priority queue sorting"""
        return self.bandwidth > other.bandwidth


@dataclass
class ElectionMessage:
    """
    Message exchanged during leader election.
    Contains score and candidate information.
    """
    sender_id: str
    candidate_id: str
    candidate_score: float
    round_num: int
    timestamp: float = field(default_factory=time.time)
    ttl: int = 10  # Time to live for propagation

    def is_fresh(self, max_age: float = 10.0) -> bool:
        """Check if message is not too old"""
        return time.time() - self.timestamp < max_age


# ============================================================================
# Helper Functions
# ============================================================================

def calculate_optimal_k(n: int, min_k: int = 3, max_k: int = 100) -> int:
    """
    Calculate optimal k for given n to minimize s = k + log_k n.
    Uses the equation: k * (log k)^2 = log n

    Args:
        n: Number of nodes
        min_k: Minimum allowed degree
        max_k: Maximum allowed degree

    Returns:
        Optimal integer k
    """
    if n <= min_k:
        return min_k

    log_n = math.log(n)
    log_log_n = math.log(log_n) if log_n > 1 else 1

    # Initial guess from asymptotic formula
    k = log_n / log_log_n if log_log_n > 0 else min_k

    # Round to integer and clamp
    k = max(min_k, min(max_k, int(k)))

    # Refine by checking neighbors (simple hill climb)
    def s_value(k_val: int) -> float:
        if k_val <= 1:
            return float('inf')
        return k_val + math.log(n) / math.log(k_val)

    best_k = k
    best_s = s_value(k)

    for delta in [-2, -1, 0, 1, 2]:
        candidate = k + delta
        if min_k <= candidate <= max_k:
            candidate_s = s_value(candidate)
            if candidate_s < best_s:
                best_s = candidate_s
                best_k = candidate

    return best_k


def calculate_node_score(connections: Dict[str, Connection],
                         k: int,
                         local_ratio: float = 0.8) -> float:
    """
    Calculate node score based on top-k connection bandwidths.

    Score = average bandwidth of top (local_ratio * k) local connections
            + average bandwidth of top ((1-local_ratio) * k) global connections

    Args:
        connections: Dictionary of node_id -> Connection
        k: Target number of children in final tree
        local_ratio: Proportion of connections that should be local (0.8 means 80% local)

    Returns:
        Score value (higher is better for leader election)
    """
    if not connections:
        return 0.0

    # In practice, we'd separate local vs global using region info
    # For now, just take top k connections
    k_effective = max(5, min(k, len(connections)))

    # Sort by bandwidth
    sorted_conns = sorted(connections.values(), key=lambda c: c.bandwidth, reverse=True)
    top_k = sorted_conns[:k_effective]

    if not top_k:
        return 0.0

    avg_bandwidth = sum(c.bandwidth for c in top_k) / len(top_k)
    return avg_bandwidth


def generate_certificate(node_id: str, secret_key: str) -> str:
    """
    Generate a certificate for node authentication.
    Certificate = SHA256(secret_key + node_id)
    """
    return hashlib.sha256(f"{secret_key}{node_id}".encode()).hexdigest()


def verify_certificate(node_id: str, certificate: str, secret_key: str) -> bool:
    """Verify a node's certificate"""
    expected = generate_certificate(node_id, secret_key)
    return certificate == expected


def partition_tasks(task: Task, num_partitions: int) -> List[Task]:
    """
    Partition a task into subtasks for parallel execution.

    Args:
        task: Original task
        num_partitions: Number of partitions to create

    Returns:
        List of subtasks
    """
    subtasks = []

    for i in range(num_partitions):
        subtask = Task(
            task_id=f"{task.task_id}_part_{i}",
            task_type=task.task_type,
            parameters=task.parameters.copy(),
            parent_task_id=task.task_id,
            priority=task.priority
        )
        subtask.parameters["partition"] = i
        subtask.parameters["total_partitions"] = num_partitions
        subtasks.append(subtask)

    return subtasks


def aggregate_results(results: Dict[str, Any], task_type: str) -> Any:
    """
    Aggregate results from parallel task execution.

    Args:
        results: Dictionary of partition_id -> result
        task_type: Type of task for specialized aggregation

    Returns:
        Aggregated result
    """
    if task_type == "find_seed":
        # Find best result (e.g., smallest hash value)
        best = None
        best_value = None
        for part_id, result in results.items():
            if result and isinstance(result, dict):
                val = result.get("value", result.get("result"))
                if val is not None:
                    if best_value is None or val < best_value:
                        best_value = val
                        best = result
        return {"best_result": best, "num_partitions": len(results)}

    elif task_type == "aggregate":
        # Sum or combine values
        total = 0
        for result in results.values():
            if isinstance(result, (int, float)):
                total += result
            elif isinstance(result, dict) and "value" in result:
                total += result["value"]
        return {"total": total, "num_partitions": len(results)}

    else:
        # Default: return all results
        return {"results": results, "num_partitions": len(results)}


def compute_optimal_sample_sizes(n: int, k: int) -> Tuple[int, int]:
    """
    Compute optimal local and global sample sizes.

    Args:
        n: Total number of nodes
        k: Target tree degree

    Returns:
        Tuple of (local_samples, global_samples)
    """
    # Each node should have ~5k connections total
    total_samples = 5 * k

    # Local samples: 80% of total
    local_samples = int(0.8 * total_samples)

    # Global samples: 20% of total
    global_samples = total_samples - local_samples

    return local_samples, global_samples


def estimate_network_diameter(n: int, k: int) -> float:
    """
    Estimate tree diameter given n and k.
    Diameter ≈ 2 * log_k(n) for a balanced tree.
    """
    if k <= 1:
        return float('inf')
    return 2 * math.log(n) / math.log(k)


def estimate_s_value(n: int, k: int) -> float:
    """Estimate s = Δ + D for given n and k"""
    delta = k
    diameter = estimate_network_diameter(n, k)
    return delta + diameter


# ============================================================================
# Simple Classes
# ============================================================================

class PriorityQueue:
    """Thread-safe priority queue for tasks and messages"""

    def __init__(self):
        self._queue = []
        self._lock = threading.Lock()
        self._counter = 0

    def push(self, item: Any, priority: int = 0):
        """Push item with priority (lower = higher priority)"""
        with self._lock:
            heapq.heappush(self._queue, (priority, self._counter, item))
            self._counter += 1

    def pop(self) -> Optional[Any]:
        """Pop highest priority item"""
        with self._lock:
            if self._queue:
                _, _, item = heapq.heappop(self._queue)
                return item
        return None

    def peek(self) -> Optional[Any]:
        """Peek at highest priority item without removing"""
        with self._lock:
            if self._queue:
                return self._queue[0][2]
        return None

    def size(self) -> int:
        with self._lock:
            return len(self._queue)

    def is_empty(self) -> bool:
        return self.size() == 0


class ExponentialBackoff:
    """Exponential backoff for retry logic"""

    def __init__(self, base_delay: float = 1.0, max_delay: float = 300.0,
                 multiplier: float = 2.0, jitter: float = 0.1):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.jitter = jitter
        self.attempts = 0

    def reset(self):
        """Reset attempt counter"""
        self.attempts = 0

    def wait(self) -> float:
        """Calculate next wait time and increment attempts"""
        delay = self.base_delay * (self.multiplier ** self.attempts)
        delay = min(delay, self.max_delay)

        # Add jitter
        if self.jitter > 0:
            delay = delay * (1 + random.uniform(-self.jitter, self.jitter))

        self.attempts += 1
        return delay

    def sleep(self):
        """Sleep for the calculated duration"""
        time.sleep(self.wait())


class RateLimiter:
    """Simple token bucket rate limiter"""

    def __init__(self, rate: float, capacity: float = None):
        """
        Args:
            rate: Tokens per second
            capacity: Maximum tokens (default = rate)
        """
        self.rate = rate
        self.capacity = capacity or rate
        self.tokens = self.capacity
        self.last_refill = time.time()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0, block: bool = True) -> bool:
        """Acquire tokens, returns True if acquired"""
        with self._lock:
            self._refill()

            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            elif not block:
                return False
            else:
                # Would need to wait in production
                return False

    def _refill(self):
        """Refill tokens based on elapsed time"""
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_refill = now


class NodeIdGenerator:
    """Generate unique node IDs"""

    def __init__(self, prefix: str = "node"):
        self.prefix = prefix
        self.counter = 0
        self._lock = threading.Lock()

    def generate(self) -> str:
        with self._lock:
            node_id = f"{self.prefix}_{self.counter}"
            self.counter += 1
            return node_id

    def reset(self):
        with self._lock:
            self.counter = 0


# ============================================================================
# Constants
# ============================================================================

class DkTCConstants:
    """System-wide constants"""

    # Network configuration
    DEFAULT_K = 7
    DEFAULT_MAX_DEGREE = 20  # Maximum children per node
    DCG_CONNECTIONS_MULTIPLIER = 5  # Each node has 5*k DCG connections
    LOCAL_CONNECTION_RATIO = 0.8  # 80% local, 20% global
    MAX_CONNECTIONS_PER_NODE = 10000

    # Leader election
    LEADER_ELECTION_ROUNDS = 10
    LEADER_ELECTION_TIMEOUT = 5.0  # seconds
    SCORE_PROPAGATION_TTL = 10

    # Tree building
    TREE_BUILDING_CANDIDATE_MULTIPLIER = 20  # At most 20*k candidates per level
    TREE_BUILDING_TIMEOUT = 30.0  # seconds

    # Task execution
    TASK_PARTITION_SIZE = 1000  # Default partition size
    TASK_TIMEOUT = 300  # seconds
    MAX_RETRIES = 3

    # Heartbeat and failure detection
    HEARTBEAT_INTERVAL = 1.0  # seconds
    FAILURE_DETECTION_TIMEOUT = 5.0  # seconds
    SUSPECT_TIMEOUT = 10.0  # seconds

    # Communication
    MESSAGE_TIMEOUT = 10.0  # seconds
    MAX_MESSAGE_SIZE = 10 * 1024 * 1024  # 10MB

    # Hidden root configuration
    HIDDEN_ROOT_PORT = 8000
    HIDDEN_ROOT_MAX_CHILDREN = 20000
    HIDDEN_ROOT_REPLICA_COUNT = 5

    @classmethod
    def get_optimal_k(cls, n: int) -> int:
        """Get optimal k for given number of nodes"""
        return calculate_optimal_k(n, min_k=3, max_k=cls.DEFAULT_MAX_DEGREE)


# ============================================================================
# Main Entry Point for Part 1 Test
# ============================================================================

def test_part_1_components():
    """Test the base components"""
    print("=" * 60)
    print("DkTC - Part 1: Base Components Test")
    print("=" * 60)

    # Test optimal k calculation
    print("\n1. Testing optimal k calculation:")
    for n in [100, 1000, 10000, 1000000, 1000000000]:
        k = calculate_optimal_k(n)
        s = estimate_s_value(n, k)
        print(f"   n={n:12d} -> k={k:3d}, Δ={k:3d}, D≈{estimate_network_diameter(n, k):.1f}, s≈{s:.1f}")

    # Test priority queue
    print("\n2. Testing PriorityQueue:")
    pq = PriorityQueue()
    pq.push("low_priority", priority=10)
    pq.push("high_priority", priority=1)
    pq.push("medium_priority", priority=5)
    print(f"   Pop order: {pq.pop()}, {pq.pop()}, {pq.pop()}")

    # Test rate limiter
    print("\n3. Testing RateLimiter:")
    limiter = RateLimiter(rate=10.0, capacity=5.0)
    for i in range(7):
        acquired = limiter.acquire(1.0, block=False)
        print(f"   Request {i + 1}: {'Acquired' if acquired else 'Denied'}")

    # Test task partitioning
    print("\n4. Testing task partitioning:")
    task = Task(
        task_id="test_001",
        task_type="compute",
        parameters={"value": 42, "range": 1000}
    )
    subtasks = partition_tasks(task, 5)
    print(f"   Created {len(subtasks)} subtasks")
    for subtask in subtasks[:3]:
        print(f"   - {subtask.task_id}")

    # Test node score calculation
    print("\n5. Testing node score calculation:")
    connections = {}
    for i in range(10):
        connections[f"node_{i}"] = Connection(
            node_id=f"node_{i}",
            bandwidth=random.uniform(10, 1000)
        )
    score = calculate_node_score(connections, k=7)
    print(f"   Score for node with 10 connections: {score:.2f}")

    # Test certificate generation
    print("\n6. Testing certificate generation:")
    secret = "test_secret"
    node_id = "node_test"
    cert = generate_certificate(node_id, secret)
    is_valid = verify_certificate(node_id, cert, secret)
    print(f"   Node {node_id} certificate: {cert[:16]}...")
    print(f"   Certificate valid: {is_valid}")

    print("\n" + "=" * 60)
    print("Part 1 initialization complete!")
    print("=" * 60)


"""
DkTC - Part 2: Hidden Root (API Server)

The Hidden Root serves as the stable entry point for the distributed system:
- Node registration and authentication
- Task submission endpoint
- Root election coordination
- Global node registry
- Bootstrap neighbor sampling

Dependencies: Part 1 base components
"""

from typing import Dict, List, Optional, Tuple, Any
import asyncio
import threading
from collections import deque


# ============================================================================
# Hidden Root Class
# ============================================================================

class HiddenRoot:
    """
    Hidden Root - The stable API server for the DkTC system.

    Responsibilities:
    1. Node registration with certificate-based authentication
    2. Maintaining global node registry
    3. Tracking current root and component leaders
    4. Task submission and result collection
    5. Providing bootstrap neighbor lists for new nodes
    6. Coordinating root election (optional)

    The Hidden Root is designed to be replicated for fault tolerance.
    """

    def __init__(self,
                 secret_key: str,
                 host: str = "0.0.0.0",
                 port: int = DkTCConstants.HIDDEN_ROOT_PORT,
                 max_children: int = DkTCConstants.HIDDEN_ROOT_MAX_CHILDREN,
                 enable_replication: bool = False):
        """
        Initialize the Hidden Root.

        Args:
            secret_key: Secret key for certificate generation
            host: Host to bind to
            port: Port to listen on
            max_children: Maximum children the root can have (20k default)
            enable_replication: Whether this is part of a replicated set
        """
        self.secret_key = secret_key
        self.host = host
        self.port = port
        self.max_children = max_children
        self.enable_replication = enable_replication

        # Core data structures
        self.nodes: Dict[str, NodeInfo] = {}
        self.current_root_id: Optional[str] = None
        self.root_children: List[str] = []  # Top max_children nodes
        self.component_leaders: Dict[str, List[str]] = {}  # component_id -> leaders
        self.tasks: Dict[str, Task] = {}

        # Bootstrap cache
        self.bootstrap_cache: Dict[str, List[Tuple[str, str]]] = {}
        self.bootstrap_cache_ttl = 60.0  # seconds

        # Task dispatcher: called (non-blocking) when a task is forwarded to the root.
        # Signature: fn(task_id, task_type, parameters, priority)
        self.task_dispatcher: Optional[Callable[[str, str, Dict[str, Any], int], None]] = None

        # Threading and synchronization
        self.lock = threading.RLock()
        self.event_loop = None
        self.running = False

        # Statistics
        self.stats = {
            "registrations": 0,
            "task_submissions": 0,
            "task_completions": 0,
            "root_changes": 0,
            "failed_registrations": 0
        }

        logger.info(f"Hidden Root initialized at {host}:{port}")
        logger.info(f"Max children: {max_children}")
        logger.info(f"Replication enabled: {enable_replication}")

    # ========================================================================
    # Node Registration
    # ========================================================================

    def register_node(self,
                      node_id: str,
                      address: str,
                      passphrase: str,
                      region: str = "default") -> Dict[str, Any]:
        """
        Register a new node in the system.

        Args:
            node_id: Unique node identifier
            address: Network address (ip:port)
            passphrase: Authentication passphrase
            region: Geographic/network region for locality

        Returns:
            Dictionary containing:
            - certificate: Authentication certificate
            - current_root_id: ID of current system root
            - neighbors: List of (node_id, address) for bootstrapping
            - max_children: Maximum children per node
            - k: Optimal tree degree
        """
        start_time = time.time()

        with self.lock:
            # Verify passphrase (simplified - in production, use proper auth)
            expected_pass = hashlib.sha256(f"{passphrase}{node_id}".encode()).hexdigest()
            # For now, we accept any non-empty passphrase
            if not passphrase:
                self.stats["failed_registrations"] += 1
                raise ValueError("Empty passphrase")

            # Generate certificate
            certificate = generate_certificate(node_id, self.secret_key)

            # Check if node already exists
            if node_id in self.nodes:
                logger.warning(f"Node {node_id} already registered, updating info")
                self.nodes[node_id].address = address
                self.nodes[node_id].region = region
                self.nodes[node_id].last_heartbeat = time.time()
            else:
                # Create new node entry
                node_info = NodeInfo(
                    node_id=node_id,
                    address=address,
                    certificate=certificate,
                    region=region,
                    status=NodeStatus.BOOTSTRAPPING,
                    joined_at=time.time(),
                    last_heartbeat=time.time()
                )
                self.nodes[node_id] = node_info
                self.stats["registrations"] += 1
                logger.info(f"Registered new node: {node_id} from {address} (region: {region})")

            # Sample neighbors for bootstrapping
            neighbors = self._sample_bootstrap_neighbors(node_id, region)

            # Compute optimal k for current network size
            current_size = len(self.nodes)
            k = DkTCConstants.get_optimal_k(current_size)

            response = {
                "certificate": certificate,
                "current_root_id": self.current_root_id,
                "neighbors": neighbors,
                "max_children": self.max_children,
                "k": k,
                "n": current_size,
                "timestamp": time.time()
            }

            # Cache bootstrap info for this node (for future lookups)
            self.bootstrap_cache[node_id] = neighbors
            self._cleanup_bootstrap_cache()

            duration = time.time() - start_time
            logger.debug(f"Node {node_id} registration completed in {duration:.3f}s")

            return response

    def _sample_bootstrap_neighbors(self,
                                    node_id: str,
                                    region: str,
                                    local_count: int = 4000,
                                    global_count: int = 1000) -> List[Tuple[str, str]]:
        """
        Sample bootstrap neighbors for a new node.

        Strategy:
        - 4k local nodes (same region, highest bandwidth potential)
        - 1k global nodes (different regions, for connectivity)

        Args:
            node_id: ID of node being registered
            region: Node's region
            local_count: Number of local neighbors to sample
            global_count: Number of global neighbors to sample

        Returns:
            List of (node_id, address) tuples
        """
        if len(self.nodes) <= 1:
            return []

        # Separate nodes by region
        local_nodes = []
        global_nodes = []

        for nid, info in self.nodes.items():
            if nid == node_id:
                continue
            if info.region == region:
                local_nodes.append((nid, info.address, info.score))
            else:
                global_nodes.append((nid, info.address, info.score))

        # Sort by score (higher is better) for better bootstrapping
        local_nodes.sort(key=lambda x: x[2], reverse=True)
        global_nodes.sort(key=lambda x: x[2], reverse=True)

        # Sample local nodes (take top or random if not enough)
        selected = []

        if len(local_nodes) >= local_count:
            # Take top local_count by score
            selected.extend([(nid, addr) for nid, addr, _ in local_nodes[:local_count]])
        else:
            # Not enough locals: take them all (no duplicate padding)
            selected.extend([(nid, addr) for nid, addr, _ in local_nodes])

        # Sample global nodes
        if len(global_nodes) >= global_count:
            # Take top global_count by score
            selected.extend([(nid, addr) for nid, addr, _ in global_nodes[:global_count]])
        elif global_nodes:
            # Take all available globals
            selected.extend([(nid, addr) for nid, addr, _ in global_nodes])

        # Shuffle to avoid bias
        random.shuffle(selected)

        logger.debug(f"Sampled {len(selected)} neighbors for {node_id} "
                     f"({len(local_nodes)} local, {len(global_nodes)} global available)")

        return selected

    def _cleanup_bootstrap_cache(self, max_age: float = 300.0):
        """Remove old entries from bootstrap cache"""
        current_time = time.time()
        expired = [nid for nid, info in self.bootstrap_cache.items()
                   if current_time - self._get_cache_time(nid) > max_age]
        for nid in expired:
            del self.bootstrap_cache[nid]

    def _get_cache_time(self, node_id: str) -> float:
        """Get timestamp of cached entry (stored in tuple value)"""
        # For simplicity, we don't store timestamps in cache
        # In production, store (timestamp, neighbors)
        return time.time()

    # ========================================================================
    # Heartbeat and Health Management
    # ========================================================================

    def update_heartbeat(self, node_id: str, certificate: str) -> bool:
        """
        Update node heartbeat to indicate it's alive.

        Args:
            node_id: Node identifier
            certificate: Authentication certificate

        Returns:
            True if heartbeat was accepted
        """
        with self.lock:
            if node_id not in self.nodes:
                logger.warning(f"Heartbeat from unknown node: {node_id}")
                return False

            node = self.nodes[node_id]

            # Verify certificate
            if not verify_certificate(node_id, certificate, self.secret_key):
                logger.warning(f"Invalid certificate from {node_id}")
                return False

            node.last_heartbeat = time.time()
            logger.debug(f"Heartbeat from {node_id} at {datetime.now()}")
            return True

    def get_stale_nodes(self, timeout: float = None) -> List[str]:
        """
        Get nodes that haven't sent heartbeat within timeout.

        Args:
            timeout: Timeout in seconds (default from constants)

        Returns:
            List of stale node IDs
        """
        if timeout is None:
            timeout = DkTCConstants.FAILURE_DETECTION_TIMEOUT

        current_time = time.time()
        stale = []

        with self.lock:
            for node_id, node in self.nodes.items():
                if current_time - node.last_heartbeat > timeout:
                    stale.append(node_id)

        return stale

    def remove_node(self, node_id: str, reason: str = "unknown") -> bool:
        """
        Remove a node from the system (failed or leaving).

        Args:
            node_id: Node to remove
            reason: Reason for removal

        Returns:
            True if node was removed
        """
        with self.lock:
            if node_id not in self.nodes:
                return False

            removed_node = self.nodes.pop(node_id)
            removed_node.status = NodeStatus.LEAVING

            # Remove from root children if present
            if node_id in self.root_children:
                self.root_children.remove(node_id)

            # If this was the current root, trigger election
            if node_id == self.current_root_id:
                logger.warning(f"Root {node_id} removed, triggering new election")
                self.current_root_id = None
                self._trigger_root_election()

            logger.info(f"Removed node {node_id}: {reason}")
            return True

    # ========================================================================
    # Root Management
    # ========================================================================

    def update_root(self, new_root_id: str, certificate: str) -> bool:
        """
        Update the current system root.

        Args:
            new_root_id: Proposed new root ID
            certificate: Node's certificate for authentication

        Returns:
            True if root was updated
        """
        with self.lock:
            # Verify node exists and certificate is valid
            if new_root_id not in self.nodes:
                logger.warning(f"Cannot set root {new_root_id}: node not registered")
                return False

            node = self.nodes[new_root_id]
            if not verify_certificate(new_root_id, certificate, self.secret_key):
                logger.warning(f"Invalid certificate for root candidate {new_root_id}")
                return False

            # Update root
            old_root = self.current_root_id
            self.current_root_id = new_root_id
            node.status = NodeStatus.ROOT

            # Update root's children list
            self._update_root_children()

            self.stats["root_changes"] += 1

            logger.info(f"Root updated: {old_root} -> {new_root_id}")
            return True

    def _update_root_children(self):
        """Update the list of root's children (top max_children by score)"""
        if not self.current_root_id:
            return

        # Get all nodes except root
        other_nodes = []
        for node_id, info in self.nodes.items():
            if node_id != self.current_root_id and info.status != NodeStatus.FAILED:
                other_nodes.append((node_id, info.score))

        # Sort by score descending
        other_nodes.sort(key=lambda x: x[1], reverse=True)

        # Take top max_children
        self.root_children = [nid for nid, _ in other_nodes[:self.max_children]]

        # Update parent relationships in node info
        for child_id in self.root_children:
            self.nodes[child_id].parent_id = self.current_root_id

        logger.debug(f"Root {self.current_root_id} now has {len(self.root_children)} children")

    def _trigger_root_election(self):
        """Trigger a new root election (notifies component leaders)"""
        # In production, this would send RPCs to trigger election
        # For now, we mark that election is needed
        logger.info("Root election triggered")

        # Find potential candidates (component leaders or high-score nodes)
        candidates = []
        for node_id, info in self.nodes.items():
            if info.status in [NodeStatus.LEADER, NodeStatus.COMPONENT_LEADER]:
                candidates.append((node_id, info.score))

        # Sort by score
        candidates.sort(key=lambda x: x[1], reverse=True)

        if candidates:
            # Suggest best candidate as new root
            best_candidate = candidates[0][0]
            logger.info(f"Suggested new root: {best_candidate}")
            # In practice, we'd let the system elect, not dictate

    # ========================================================================
    # Task Management
    # ========================================================================

    def submit_task(self,
                    task_type: str,
                    parameters: Dict[str, Any],
                    priority: int = 0) -> str:
        """
        Submit a new task to the distributed system.

        Args:
            task_type: Type of task (e.g., "find_seed", "aggregate")
            parameters: Task-specific parameters
            priority: Task priority (lower = higher priority)

        Returns:
            Task ID for tracking
        """
        task_id = str(uuid.uuid4())

        task = Task(
            task_id=task_id,
            task_type=task_type,
            parameters=parameters,
            priority=priority,
            created_at=time.time(),
            status=TaskStatus.PENDING
        )

        with self.lock:
            self.tasks[task_id] = task
            self.stats["task_submissions"] += 1

        # Forward to current root for distribution
        if self.current_root_id:
            self._forward_task_to_root(task_id, parameters, task_type, priority)
        else:
            logger.error(f"No root available for task {task_id}")
            task.status = TaskStatus.FAILED

        logger.info(f"Task submitted: {task_id} (type: {task_type})")
        return task_id

    def _forward_task_to_root(self,
                              task_id: str,
                              parameters: Dict[str, Any],
                              task_type: str,
                              priority: int):
        """
        Forward task to the current root node for distribution.

        In production, this would be an RPC call to the root node.
        For simulation, we log and update task status.
        """
        logger.info(f"Forwarding task {task_id} to root {self.current_root_id}")

        # In real implementation:
        # - Send RPC to root node
        # - Root partitions and distributes
        # - Results flow back up to root, then to hidden root

        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id].status = TaskStatus.RUNNING
                self.tasks[task_id].assigned_node = self.current_root_id

        if self.task_dispatcher:
            try:
                self.task_dispatcher(task_id, task_type, parameters, priority)
            except Exception as e:
                logger.error(f"Dispatcher failed for task {task_id}: {e}")
                self.fail_task(task_id, str(e))

    def set_task_dispatcher(self, fn: Callable[[str, str, Dict[str, Any], int], None]):
        """Register the callback that actually executes forwarded tasks."""
        self.task_dispatcher = fn

    def fail_task(self, task_id: str, error: str = "") -> bool:
        """Mark a task as failed."""
        with self.lock:
            task = self.tasks.get(task_id)
            if not task:
                return False
            task.status = TaskStatus.FAILED
            task.result = {"error": error} if error else None
            task.completed_at = time.time()
            return True

    def update_task_result(self, task_id: str, result: Any) -> bool:
        """
        Update task result when completed.

        Args:
            task_id: Task identifier
            result: Task result

        Returns:
            True if update was successful
        """
        with self.lock:
            if task_id not in self.tasks:
                logger.warning(f"Task {task_id} not found")
                return False

            task = self.tasks[task_id]
            task.result = result
            task.status = TaskStatus.COMPLETED
            task.completed_at = time.time()
            self.stats["task_completions"] += 1

            logger.info(f"Task {task_id} completed")
            return True

    def get_task_result(self, task_id: str) -> Optional[Any]:
        """
        Get result of a completed task.

        Args:
            task_id: Task identifier

        Returns:
            Task result if available and completed, else None
        """
        with self.lock:
            task = self.tasks.get(task_id)
            if task and task.status == TaskStatus.COMPLETED:
                return task.result
            elif task:
                logger.debug(f"Task {task_id} status: {task.status.value}")
            return None

    def get_task_status(self, task_id: str) -> Optional[str]:
        """Get current status of a task"""
        with self.lock:
            task = self.tasks.get(task_id)
            return task.status.value if task else None

    # ========================================================================
    # Node Information Queries
    # ========================================================================

    def get_node_info(self, node_id: str) -> Optional[NodeInfo]:
        """Get information about a specific node"""
        with self.lock:
            return self.nodes.get(node_id)

    def get_all_nodes(self) -> List[NodeInfo]:
        """Get information about all nodes"""
        with self.lock:
            return list(self.nodes.values())

    def get_node_count(self) -> int:
        """Get total number of registered nodes"""
        with self.lock:
            return len(self.nodes)

    def get_active_node_count(self) -> int:
        """Get number of active (non-failed) nodes"""
        with self.lock:
            return sum(1 for node in self.nodes.values()
                       if node.status != NodeStatus.FAILED)

    def get_root_info(self) -> Optional[Dict[str, Any]]:
        """Get information about current root"""
        with self.lock:
            if not self.current_root_id:
                return None

            root = self.nodes.get(self.current_root_id)
            if not root:
                return None

            return {
                "node_id": root.node_id,
                "address": root.address,
                "score": root.score,
                "children_count": len(self.root_children),
                "children": self.root_children[:10],  # Top 10 only
                "total_nodes": self.get_node_count()
            }

    # ========================================================================
    # Statistics and Monitoring
    # ========================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Get system statistics"""
        with self.lock:
            stats = self.stats.copy()
            stats.update({
                "total_nodes": len(self.nodes),
                "active_nodes": self.get_active_node_count(),
                "current_root": self.current_root_id,
                "root_children_count": len(self.root_children),
                "pending_tasks": sum(1 for t in self.tasks.values()
                                     if t.status == TaskStatus.PENDING),
                "running_tasks": sum(1 for t in self.tasks.values()
                                     if t.status == TaskStatus.RUNNING),
                "completed_tasks": self.stats["task_completions"],
                "uptime": time.time() - getattr(self, "_start_time", time.time())
            })
            return stats

    def start(self):
        """Start the hidden root server"""
        self.running = True
        self._start_time = time.time()
        logger.info(f"Hidden Root started on {self.host}:{self.port}")
        # In production, start actual network server here

    def stop(self):
        """Stop the hidden root server"""
        self.running = False
        logger.info("Hidden Root stopped")

    # ========================================================================
    # Replication Support (for multi-master setup)
    # ========================================================================

    def replicate_to_peer(self, peer_address: str) -> bool:
        """
        Replicate state to a peer hidden root.

        Args:
            peer_address: Address of peer hidden root

        Returns:
            True if replication was successful
        """
        if not self.enable_replication:
            logger.warning("Replication not enabled")
            return False

        # In production, send state to peer using RPC
        logger.info(f"Replicating to {peer_address}")
        # For now, simulate success
        return True

    def merge_replicated_state(self, state: Dict[str, Any]) -> bool:
        """
        Merge state received from a peer hidden root.

        Args:
            state: Serialized state from peer

        Returns:
            True if merge was successful
        """
        with self.lock:
            # Merge nodes
            for node_id, node_data in state.get("nodes", {}).items():
                if node_id not in self.nodes:
                    self.nodes[node_id] = NodeInfo.from_dict(node_data)

            # Update root if peer has newer root
            peer_root = state.get("current_root_id")
            peer_version = state.get("version", 0)
            local_version = getattr(self, "_version", 0)

            if peer_root and peer_version > local_version:
                self.current_root_id = peer_root
                self._version = peer_version
                logger.info(f"Root updated from peer: {peer_root}")

            return True


# ============================================================================
# Hidden Root Replica Manager (for high availability)
# ============================================================================

class HiddenRootCluster:
    """
    Manages multiple Hidden Root replicas for fault tolerance.
    Uses leader-follower replication with Raft-like consensus.
    """

    def __init__(self, replicas: List[Tuple[str, int]], secret_key: str):
        """
        Args:
            replicas: List of (host, port) for each replica
            secret_key: Shared secret for all replicas
        """
        self.replicas = replicas
        self.secret_key = secret_key
        self.leader_id = 0  # Index of leader replica
        self.replica_instances: List[HiddenRoot] = []
        self._lock = threading.Lock()

        # Initialize replicas
        for i, (host, port) in enumerate(replicas):
            replica = HiddenRoot(
                secret_key=secret_key,
                host=host,
                port=port,
                enable_replication=True
            )
            self.replica_instances.append(replica)

        logger.info(f"Hidden Root Cluster initialized with {len(replicas)} replicas")

    def get_leader(self) -> HiddenRoot:
        """Get the current leader replica"""
        with self._lock:
            return self.replica_instances[self.leader_id]

    def register_node(self, node_id: str, address: str, passphrase: str, region: str = "default"):
        """Register node via leader replica"""
        return self.get_leader().register_node(node_id, address, passphrase, region)

    def submit_task(self, task_type: str, parameters: Dict[str, Any], priority: int = 0):
        """Submit task via leader replica"""
        return self.get_leader().submit_task(task_type, parameters, priority)

    def start_all(self):
        """Start all replicas"""
        for replica in self.replica_instances:
            replica.start()
        logger.info("All hidden root replicas started")

    def stop_all(self):
        """Stop all replicas"""
        for replica in self.replica_instances:
            replica.stop()
        logger.info("All hidden root replicas stopped")


# ============================================================================
# Main Entry Point for Part 1 Test
# ============================================================================

def test_part_2_components():
    print("=" * 60)
    print("DkTC - Part 2: Hidden Root Test")
    print("=" * 60)

    # Create hidden root
    hidden_root = HiddenRoot(secret_key="test_secret_123", max_children=100)
    hidden_root.start()

    # Test node registration
    print("\n1. Testing node registration:")

    # Register first node
    result1 = hidden_root.register_node(
        node_id="node_001",
        address="192.168.1.1:8001",
        passphrase="pass123",
        region="us-east"
    )
    print(f"   Node 001 registered: root={result1['current_root_id']}, "
          f"k={result1['k']}, neighbors={len(result1['neighbors'])}")

    # Register more nodes
    for i in range(2, 11):
        result = hidden_root.register_node(
            node_id=f"node_{i:03d}",
            address=f"192.168.1.{i}:8001",
            passphrase=f"pass{i}",
            region="us-east" if i % 2 == 0 else "us-west"
        )
        if i == 10:
            print(f"   Node 010 registered: k={result['k']}, n={result['n']}")

    # Update root
    print("\n2. Testing root update:")
    hidden_root.update_root("node_001", generate_certificate("node_001", "test_secret_123"))
    root_info = hidden_root.get_root_info()
    print(f"   Current root: {root_info['node_id']}")
    print(f"   Root children: {root_info['children_count']}")

    # Test task submission
    print("\n3. Testing task submission:")
    task_id = hidden_root.submit_task(
        task_type="find_seed",
        parameters={"target": "abc123", "range": 1000000},
        priority=1
    )
    print(f"   Task ID: {task_id}")
    print(f"   Task status: {hidden_root.get_task_status(task_id)}")

    # Simulate task completion
    hidden_root.update_task_result(task_id, {"result": "found_seed_42", "time": 0.5})
    result = hidden_root.get_task_result(task_id)
    print(f"   Task result: {result}")

    # Test heartbeat
    print("\n4. Testing heartbeat:")
    cert = generate_certificate("node_001", "test_secret_123")
    heartbeat_ok = hidden_root.update_heartbeat("node_001", cert)
    print(f"   Heartbeat from node_001: {'OK' if heartbeat_ok else 'FAILED'}")

    # Get statistics
    print("\n5. System statistics:")
    stats = hidden_root.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")

    # Get node information
    print("\n6. Node information:")
    all_nodes = hidden_root.get_all_nodes()
    print(f"   Total registered nodes: {len(all_nodes)}")
    print(f"   Active nodes: {hidden_root.get_active_node_count()}")

    if all_nodes:
        sample = all_nodes[0]
        print(f"   Sample node: {sample.node_id} (region: {sample.region})")

    hidden_root.stop()

    print("\n" + "=" * 60)
    print("Part 2: Hidden Root test completed!")
    print("=" * 60)


"""
DkTC - Part 3: DCG Network (Dense Connection Graph)

The DCG provides:
- ~10k connections per node for gossip and fault tolerance
- Bandwidth measurement and tracking
- Multi-round leader election using score propagation
- Component detection and merging
- Failure detection via heartbeat

Dependencies: Part 1 and Part 2
"""

from typing import Dict, List, Optional, Set, Tuple, Any
import threading
import random
from collections import defaultdict


# ============================================================================
# DCG Network Node
# ============================================================================

class DCGNode:
    """
    Dense Connection Graph Node - maintains connections for gossip/leader election.

    Each node maintains ~5k-10k connections and participates in:
    - Bandwidth measurement and tracking
    - Leader election via score propagation
    - Component detection
    - Failure detection
    """

    def __init__(self,
                 node_id: str,
                 hidden_root: HiddenRoot,
                 k: int = None,
                 max_connections: int = 10000):
        """
        Initialize DCG node.

        Args:
            node_id: Unique node identifier
            hidden_root: Reference to hidden root for registration
            k: Target tree degree (used for score calculation)
            max_connections: Maximum DCG connections (default 10k)
        """
        self.node_id = node_id
        self.hidden_root = hidden_root
        self.k = k or DkTCConstants.DEFAULT_K
        self.max_connections = max_connections

        # Connection management
        self.connections: Dict[str, Connection] = {}  # node_id -> Connection
        self.pending_connections: Set[str] = set()
        self.connection_lock = threading.RLock()

        # Leader election state
        self.leader_id: Optional[str] = None
        self.component_leader_id: Optional[str] = None
        self.component_id: Optional[str] = None
        self.score: float = 0.0
        self.scores: Dict[str, float] = {}  # node_id -> score cache
        self.election_round: int = 0
        self.election_results: Dict[int, Dict[str, float]] = {}  # round -> {candidate: score}

        # Component detection
        self.component_nodes: Set[str] = set()
        self.component_version: int = 0

        # Failure detection
        self.failure_suspects: Dict[str, float] = {}  # node_id -> suspect_time
        self.last_heartbeat_sent: float = time.time()
        self.last_heartbeat_received: Dict[str, float] = {}

        # Optional back-reference to the DCGNetwork (lets the simulation read real neighbor state)
        self.network: Optional['DCGNetwork'] = None

        # Threading
        self.lock = threading.RLock()
        self.running = False
        self.gossip_thread = None

        # Statistics
        self.stats = {
            "connections_established": 0,
            "gossip_messages_sent": 0,
            "gossip_messages_received": 0,
            "leader_elections_completed": 0,
            "component_merges": 0
        }

        logger.info(f"DCGNode {node_id} initialized (k={self.k}, max_conn={max_connections})")

    # ========================================================================
    # Connection Management
    # ========================================================================

    def add_connection_symmetric(self, target_id: str, bandwidth: float, initiator: bool = True) -> bool:
        """
        Add a bidirectional connection between this node and target.

        Args:
            target_id: Target node ID
            bandwidth: Measured bandwidth
            initiator: True if this node initiated the connection, False if responding

        Returns:
            True if connection was added successfully
        """
        with self.connection_lock:
            # Add forward connection
            if target_id not in self.connections:
                self.connections[target_id] = Connection(
                    node_id=target_id,
                    bandwidth=bandwidth,
                    last_updated=time.time()
                )
                self.stats["connections_established"] += 1

                # If we're the initiator, we need to notify the target to add reverse connection
                if initiator:
                    self._send_reverse_connection_request(target_id, bandwidth)

                logger.debug(
                    f"Node {self.node_id} added {'initiated' if initiator else 'reverse'} connection to {target_id} (bw={bandwidth:.2f})")
                return True

        return False

    def _send_reverse_connection_request(self, target_id: str, bandwidth: float):
        """
        Send request to target to add reverse connection.

        In production, this would be an RPC call.
        For simulation, we directly call the target's method if available.
        """
        # In a real distributed system, this would be a network message
        # For our simulation, we need to access the target DCGNode
        # This requires a registry of DCGNode instances

        # Placeholder - actual implementation depends on node registry
        logger.debug(f"Node {self.node_id} requesting reverse connection from {target_id}")

        # In the simulation, we'll rely on the establish_connections method
        # to call add_connection_symmetric with initiator=False on the target

    def establish_connections(self, neighbors: List[Tuple[str, str]]) -> int:
        """
        Establish connections to bootstrap neighbors with bidirectional symmetry.

        Args:
            neighbors: List of (node_id, address) tuples

        Returns:
            Number of successful connections
        """
        successful = 0

        for nid, address in neighbors:
            if nid == self.node_id:
                continue

            with self.connection_lock:
                if nid in self.connections:
                    continue

                if len(self.connections) >= self.max_connections:
                    logger.warning(f"Node {self.node_id} at max connections ({self.max_connections})")
                    break

            # Measure bandwidth
            bandwidth = self._measure_bandwidth(nid, address)

            if bandwidth > 0:
                # Add forward connection
                with self.connection_lock:
                    self.connections[nid] = Connection(
                        node_id=nid,
                        bandwidth=bandwidth,
                        last_updated=time.time()
                    )
                    self.stats["connections_established"] += 1
                    successful += 1
                    self.last_heartbeat_received[nid] = time.time()

                # CRITICAL: Notify target to add reverse connection
                self._request_reverse_connection(nid, bandwidth)

                logger.debug(f"Node {self.node_id} connected to {nid} (bw={bandwidth:.2f})")

        logger.info(f"Node {self.node_id} established {successful} bidirectional connections")
        return successful

    def _request_reverse_connection(self, target_id: str, bandwidth: float):
        """
        Request target node to add reverse connection.

        In a real distributed system, this sends a network message.
        For simulation, we need a way to find the target DCGNode.
        """
        # This method will be called by the DCGNetwork manager which has node registry
        # We'll implement the actual logic in DCGNetwork
        logger.debug(f"Node {self.node_id} requesting reverse connection to {target_id}")

    def handle_reverse_connection_request(self, requester_id: str, bandwidth: float) -> bool:
        """
        Handle a request from another node to add a reverse connection.

        Args:
            requester_id: Node that initiated the original connection
            bandwidth: Measured bandwidth

        Returns:
            True if reverse connection was added
        """
        with self.connection_lock:
            if requester_id in self.connections:
                # Already connected
                return True

            if len(self.connections) >= self.max_connections:
                logger.warning(
                    f"Node {self.node_id} cannot add reverse connection to {requester_id}: max connections reached")
                return False

            # Add reverse connection
            self.connections[requester_id] = Connection(
                node_id=requester_id,
                bandwidth=bandwidth,
                last_updated=time.time()
            )
            self.stats["connections_established"] += 1
            self.last_heartbeat_received[requester_id] = time.time()

            logger.debug(f"Node {self.node_id} added reverse connection from {requester_id} (bw={bandwidth:.2f})")
            return True

    def _measure_bandwidth(self, target_id: str, target_address: str) -> float:
        """
        Measure bandwidth to target node.

        In production, this would perform actual network measurements.
        For simulation, returns random bandwidth between 10-1000 Mbps.

        Args:
            target_id: Target node ID
            target_address: Target address

        Returns:
            Bandwidth in Mbps
        """
        # Simulate bandwidth based on node ID hash (consistent for same pair)
        # This ensures symmetry: bw(a,b) ≈ bw(b,a)
        pair = ":".join(sorted([self.node_id, target_id]))
        seed = int(hashlib.md5(pair.encode()).hexdigest()[:8], 16)
        return random.Random(seed).uniform(10, 1000)

    def update_bandwidth(self, neighbor_id: str, new_bandwidth: float):
        """
        Update bandwidth measurement for a neighbor.

        Args:
            neighbor_id: Neighbor node ID
            new_bandwidth: New bandwidth measurement
        """
        with self.connection_lock:
            if neighbor_id in self.connections:
                self.connections[neighbor_id].update_bandwidth(new_bandwidth)
                logger.debug(f"Updated bandwidth {self.node_id}->{neighbor_id}: {new_bandwidth:.2f} Mbps")

    def get_best_connections(self, n: int) -> List[Connection]:
        """
        Get top n connections by bandwidth.

        Args:
            n: Number of connections to return

        Returns:
            List of top connections
        """
        with self.connection_lock:
            sorted_conns = sorted(
                self.connections.values(),
                key=lambda c: c.bandwidth,
                reverse=True
            )
            return sorted_conns[:n]

    def get_connection_count(self) -> int:
        """Get number of active connections"""
        with self.connection_lock:
            return len(self.connections)

    # ========================================================================
    # Score Calculation
    # ========================================================================

    def calculate_score(self, local_ratio: float = 0.8) -> float:
        """
        Calculate node's score for leader election.

        Score = average bandwidth of top (local_ratio*k) connections
                + average of top ((1-local_ratio)*k) global connections

        Args:
            local_ratio: Proportion of connections that should be local

        Returns:
            Score value (higher is better)
        """
        with self.connection_lock:
            if not self.connections:
                return 0.0

            # Get all connections sorted by bandwidth
            sorted_conns = sorted(
                self.connections.values(),
                key=lambda c: c.bandwidth,
                reverse=True
            )

            # In production, separate local vs global using region info
            # For simulation, just take top k
            k_effective = max(3, min(self.k, len(sorted_conns)))
            top_k = sorted_conns[:k_effective]

            if not top_k:
                return 0.0

            avg_bandwidth = sum(c.bandwidth for c in top_k) / len(top_k)

            # Store for later use
            self.score = avg_bandwidth

            return avg_bandwidth

    # ========================================================================
    # Leader Election (Gossip-based)
    # ========================================================================

    def run_leader_election(self, rounds: int = None, timeout: float = None) -> str:
        """
        Run multi-round leader election using gossip protocol.

        Algorithm:
        1. Each node calculates its score
        2. Nodes exchange best-known candidates with neighbors
        3. After convergence, the highest-score node becomes leader
        4. Multiple rounds ensure propagation through the network

        Args:
            rounds: Number of election rounds (default from constants)
            timeout: Timeout per round in seconds

        Returns:
            Elected leader ID
        """
        if rounds is None:
            rounds = DkTCConstants.LEADER_ELECTION_ROUNDS

        if timeout is None:
            timeout = DkTCConstants.LEADER_ELECTION_TIMEOUT / rounds

        logger.info(f"Node {self.node_id} starting leader election ({rounds} rounds)")

        # Calculate local score
        self.score = self.calculate_score()

        # Initialize best candidate
        best_candidate = self.node_id
        best_score = self.score
        self.scores[self.node_id] = best_score

        # Multi-round election
        for round_num in range(rounds):
            # Exchange scores with neighbors
            self._exchange_scores(round_num)

            # Update best candidate based on neighbor scores
            with self.lock:
                for nid, score in self.scores.items():
                    if score > best_score:
                        best_score = score
                        best_candidate = nid
                    elif score == best_score and nid < best_candidate:
                        best_candidate = nid

            # Store election result for this round
            self.election_results[round_num] = {best_candidate: best_score}

            # Small delay for propagation
            time.sleep(timeout)

            logger.debug(f"Round {round_num + 1}/{rounds}: candidate={best_candidate}, score={best_score:.2f}")

        # Set final leader
        self.leader_id = best_candidate

        # Determine if this node is the leader
        is_leader = (best_candidate == self.node_id)

        if is_leader:
            self.component_leader_id = self.node_id
            # Register with hidden root
            cert = generate_certificate(self.node_id, self.hidden_root.secret_key)
            self.hidden_root.update_root(self.node_id, cert)
            logger.info(f"Node {self.node_id} elected as leader (score: {best_score:.2f})")

        self.stats["leader_elections_completed"] += 1

        return best_candidate

    def _exchange_scores(self, round_num: int):
        """
        Exchange score information with neighbors.

        This simulates gossip where each node sends its best-known
        candidate to all neighbors, and receives theirs.
        """
        with self.connection_lock:
            neighbors = list(self.connections.keys())

        # Simulate sending scores to neighbors
        # In production, this would send actual network messages
        for neighbor_id in neighbors:
            neighbor = self.network.get_node(neighbor_id) if self.network else None
            if neighbor is not None:
                # Real gossip: learn the neighbor's own score plus everything it already knows
                self.scores[neighbor_id] = neighbor.calculate_score()
                for nid, sc in list(neighbor.scores.items()):
                    self.scores.setdefault(nid, sc)
            elif neighbor_id not in self.scores:
                # Simulate neighbor's score (in production, would receive via RPC)
                neighbor_score = random.uniform(10, 1000)
                self.scores[neighbor_id] = neighbor_score

            self.stats["gossip_messages_sent"] += 1

    def propagate_component_leader(self, leader_id: str, leader_score: float, ttl: int = 10):
        """
        Propagate component leader information through the network.

        Args:
            leader_id: ID of the elected component leader
            leader_score: Score of the leader
            ttl: Time-to-live for propagation
        """
        if ttl <= 0:
            return

        self.component_leader_id = leader_id

        # Propagate to neighbors
        with self.connection_lock:
            for neighbor_id in self.connections:
                # In production, send RPC to neighbor
                # For simulation, we just log
                self.stats["gossip_messages_sent"] += 1

        logger.debug(f"Propagated leader {leader_id} (score: {leader_score:.2f}) from {self.node_id}")

    # ========================================================================
    # Component Detection and Merging
    # ========================================================================

    def detect_component(self) -> str:
        """
        Detect the connected component this node belongs to.

        Uses BFS over DCG connections to discover all nodes in component.

        Returns:
            Component ID (hash of sorted node IDs)
        """
        visited = set()
        queue = deque([self.node_id])
        visited.add(self.node_id)

        with self.connection_lock:
            while queue:
                current = queue.popleft()

                # Get neighbors of current node
                # In production, would need to query neighbors' connections
                # For simulation, we use local connections
                if current == self.node_id:
                    neighbors = list(self.connections.keys())
                else:
                    # Simulate: assume symmetric connections
                    neighbors = [n for n in self.connections.keys() if n != current]

                for neighbor in neighbors:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append(neighbor)

        # Create component ID from sorted node list
        sorted_nodes = sorted(visited)
        component_id = hashlib.md5(','.join(sorted_nodes).encode()).hexdigest()[:16]

        self.component_id = component_id
        self.component_nodes = visited
        self.component_version += 1

        logger.debug(f"Node {self.node_id} in component {component_id} ({len(visited)} nodes)")

        return component_id

    def merge_components(self, other_component_id: str, other_leader_id: str):
        """
        Merge this component with another component.

        Called when two components discover each other.

        Args:
            other_component_id: ID of other component
            other_leader_id: Leader of other component
        """
        # Determine which leader has higher score
        other_score = self.scores.get(other_leader_id, 0)

        if other_score > self.score:
            # Other component's leader is better, merge into theirs
            logger.info(f"Component {self.component_id} merging into {other_component_id}")
            self.component_leader_id = other_leader_id
            self.component_id = other_component_id
        elif other_score < self.score:
            # This component's leader is better, other should merge here
            logger.info(f"Component {other_component_id} merging into {self.component_id}")
            # In production, would notify other component to merge
        else:
            # Tie break by node ID
            if other_leader_id < self.node_id:
                self.component_leader_id = other_leader_id
                self.component_id = other_component_id

        self.stats["component_merges"] += 1

    # ========================================================================
    # Failure Detection
    # ========================================================================

    def send_heartbeat(self):
        """Send heartbeat to all neighbors."""
        current_time = time.time()
        self.last_heartbeat_sent = current_time

        with self.connection_lock:
            for neighbor_id in self.connections:
                # In production, send actual heartbeat message
                self.stats["gossip_messages_sent"] += 1

        logger.debug(f"Node {self.node_id} sent heartbeats to {len(self.connections)} neighbors")

    def receive_heartbeat(self, from_node_id: str):
        """Process received heartbeat from neighbor."""
        self.last_heartbeat_received[from_node_id] = time.time()

        # Clear from suspects if previously suspected
        if from_node_id in self.failure_suspects:
            del self.failure_suspects[from_node_id]

    def detect_failures(self, timeout: float = None) -> List[str]:
        """
        Detect failed neighbors based on missing heartbeats.

        Args:
            timeout: Timeout in seconds (default from constants)

        Returns:
            List of suspected failed node IDs
        """
        if timeout is None:
            timeout = DkTCConstants.FAILURE_DETECTION_TIMEOUT

        current_time = time.time()
        failed_nodes = []

        with self.connection_lock:
            for neighbor_id, last_heartbeat in list(self.last_heartbeat_received.items()):
                if current_time - last_heartbeat > timeout:
                    if neighbor_id not in self.failure_suspects:
                        self.failure_suspects[neighbor_id] = current_time
                        logger.warning(f"Node {self.node_id} suspects {neighbor_id} failed")
                        failed_nodes.append(neighbor_id)

        return failed_nodes

    def handle_failure(self, failed_node_id: str):
        """
        Handle a node failure by removing connections and updating state.

        Args:
            failed_node_id: ID of failed node
        """
        with self.connection_lock:
            if failed_node_id in self.connections:
                del self.connections[failed_node_id]
                logger.info(f"Node {self.node_id} removed failed connection to {failed_node_id}")

            if failed_node_id in self.last_heartbeat_received:
                del self.last_heartbeat_received[failed_node_id]

            if failed_node_id in self.scores:
                del self.scores[failed_node_id]

        # Notify hidden root
        cert = generate_certificate(self.node_id, self.hidden_root.secret_key)
        self.hidden_root.update_heartbeat(self.node_id, cert)

    # ========================================================================
    # Gossip Protocol
    # ========================================================================

    def gossip(self, data: Dict[str, Any], ttl: int = 5):
        """
        Gossip data to random subset of neighbors.

        Args:
            data: Data to gossip
            ttl: Time-to-live
        """
        if ttl <= 0:
            return

        # Select random subset of neighbors (gossip fanout)
        with self.connection_lock:
            neighbors = list(self.connections.keys())

        # Typically gossip to sqrt(N) or fixed number
        fanout = min(3, len(neighbors))
        selected = random.sample(neighbors, min(fanout, len(neighbors)))

        for neighbor_id in selected:
            # In production, send gossip message
            self.stats["gossip_messages_sent"] += 1
            logger.debug(f"Gossiped to {neighbor_id} (ttl={ttl})")

        self.stats["gossip_messages_received"] += 1

    # ========================================================================
    # Threading and Main Loop
    # ========================================================================

    def start(self):
        """Start DCG node operations."""
        self.running = True

        # Start background gossip and heartbeat thread
        self.gossip_thread = threading.Thread(target=self._background_loop, daemon=True)
        self.gossip_thread.start()

        logger.info(f"DCGNode {self.node_id} started")

    def stop(self):
        """Stop DCG node operations."""
        self.running = False
        if self.gossip_thread:
            self.gossip_thread.join(timeout=5)
        logger.info(f"DCGNode {self.node_id} stopped")

    def _background_loop(self):
        """Background loop for periodic tasks."""
        last_heartbeat_time = time.time()
        last_failure_check = time.time()

        while self.running:
            current_time = time.time()

            # Send periodic heartbeats
            if current_time - last_heartbeat_time >= DkTCConstants.HEARTBEAT_INTERVAL:
                self.send_heartbeat()
                last_heartbeat_time = current_time

            # Periodic failure detection
            if current_time - last_failure_check >= DkTCConstants.HEARTBEAT_INTERVAL * 3:
                failed = self.detect_failures()
                for node_id in failed:
                    self.handle_failure(node_id)
                last_failure_check = current_time

            time.sleep(0.1)  # Small sleep to avoid busy loop

    # ========================================================================
    # Statistics and Monitoring
    # ========================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Get DCG node statistics."""
        with self.lock:
            stats = self.stats.copy()
            stats.update({
                "node_id": self.node_id,
                "connections": len(self.connections),
                "leader_id": self.leader_id,
                "component_leader_id": self.component_leader_id,
                "component_id": self.component_id,
                "component_size": len(self.component_nodes),
                "score": self.score,
                "election_round": self.election_round
            })
            return stats

    def get_connection_summary(self) -> Dict[str, Any]:
        """Get summary of connections (top N by bandwidth)."""
        with self.connection_lock:
            top_10 = self.get_best_connections(10)
            return {
                "total_connections": len(self.connections),
                "top_connections": [(c.node_id, f"{c.bandwidth:.2f}") for c in top_10],
                "avg_bandwidth": sum(c.bandwidth for c in self.connections.values()) / max(1, len(self.connections))
            }


# ============================================================================
# DCG Network Manager (coordinates multiple DCG nodes)
# ============================================================================

class DCGNetwork:
    """
    Manages the Dense Connection Graph across multiple nodes.

    This is a container that coordinates DCGNode instances and provides
    network-level operations.
    """

    def __init__(self, hidden_root: HiddenRoot, k: int = None):
        """
        Initialize DCG Network.

        Args:
            hidden_root: Reference to hidden root for registration
            k: Target tree degree
        """
        self.hidden_root = hidden_root
        self.k = k or DkTCConstants.DEFAULT_K
        self.nodes: Dict[str, DCGNode] = {}
        self.node_registry: Dict[str, 'DCGNode'] = {}  # Alias for clarity
        self.lock = threading.RLock()

        logger.info(f"DCGNetwork initialized (k={self.k})")

    def get_node(self, node_id: str) -> Optional[DCGNode]:
        """Get DCG node by ID."""
        with self.lock:
            return self.nodes.get(node_id)

    def request_reverse_connection(self, from_node_id: str, to_node_id: str, bandwidth: float) -> bool:
        """
        Request that to_node add a reverse connection from from_node.

        Args:
            from_node_id: Node that initiated the original connection
            to_node_id: Node that should add the reverse connection
            bandwidth: Measured bandwidth

        Returns:
            True if reverse connection was added successfully
        """
        with self.lock:
            target_node = self.nodes.get(to_node_id)
            if not target_node:
                logger.warning(f"Cannot add reverse connection: node {to_node_id} not found")
                return False

            return target_node.handle_reverse_connection_request(from_node_id, bandwidth)

    def create_node(self, node_id: str) -> DCGNode:
        """
        Create a new DCG node.

        Args:
            node_id: Unique node identifier

        Returns:
            Created DCGNode instance
        """
        with self.lock:
            if node_id in self.nodes:
                logger.warning(f"Node {node_id} already exists")
                return self.nodes[node_id]

            node = DCGNode(
                node_id=node_id,
                hidden_root=self.hidden_root,
                k=self.k,
                max_connections=DkTCConstants.MAX_CONNECTIONS_PER_NODE
            )
            node.network = self
            self.nodes[node_id] = node
            logger.info(f"Created DCG node {node_id}")
            return node

    def bootstrap_node(self, node_id: str, neighbors: List[Tuple[str, str]]) -> int:
        """
        Bootstrap a node with initial neighbors (bidirectional).

        Args:
            node_id: Node to bootstrap
            neighbors: List of (node_id, address) to connect to

        Returns:
            Number of successful connections
        """
        if node_id not in self.nodes:
            raise ValueError(f"Node {node_id} not found")

        node = self.nodes[node_id]
        successful = 0

        for neighbor_id, address in neighbors:
            if neighbor_id == node_id:
                continue

            # Measure bandwidth
            bandwidth = node._measure_bandwidth(neighbor_id, address)

            if bandwidth > 0:
                # Add forward connection on this node
                with node.connection_lock:
                    if neighbor_id not in node.connections:
                        node.connections[neighbor_id] = Connection(
                            node_id=neighbor_id,
                            bandwidth=bandwidth,
                            last_updated=time.time()
                        )
                        node.stats["connections_established"] += 1
                        successful += 1
                        node.last_heartbeat_received[neighbor_id] = time.time()

                # CRITICAL: Request reverse connection on the neighbor
                # This requires the neighbor's DCGNode to exist
                neighbor_node = self.nodes.get(neighbor_id)
                if neighbor_node:
                    neighbor_node.handle_reverse_connection_request(node_id, bandwidth)
                else:
                    # If neighbor doesn't exist yet (joining later), store pending request
                    logger.debug(f"Neighbor {neighbor_id} not yet available, will retry later")

        logger.info(f"Node {node_id} bootstrapped with {successful} bidirectional connections")
        return successful

    def run_leader_election(self, node_id: str, rounds: int = None) -> str:
        """
        Run leader election from a specific node.

        Args:
            node_id: Node to run election from
            rounds: Number of election rounds

        Returns:
            Elected leader ID
        """
        if node_id not in self.nodes:
            raise ValueError(f"Node {node_id} not found")

        node = self.nodes[node_id]
        return node.run_leader_election(rounds)

    def get_leader(self) -> Optional[str]:
        """
        Get current system leader (aggregated across nodes).

        Returns:
            Leader node ID or None
        """
        # In production, would aggregate from all nodes
        # For simulation, return first node's leader
        for node in self.nodes.values():
            if node.leader_id:
                return node.leader_id
        return None

    def start_all(self):
        """Start all DCG nodes."""
        for node in self.nodes.values():
            node.start()
        logger.info(f"Started {len(self.nodes)} DCG nodes")

    def stop_all(self):
        """Stop all DCG nodes."""
        for node in self.nodes.values():
            node.stop()
        logger.info(f"Stopped {len(self.nodes)} DCG nodes")

    def get_network_stats(self) -> Dict[str, Any]:
        """Get overall network statistics."""
        with self.lock:
            total_connections = sum(node.get_connection_count() for node in self.nodes.values())
            avg_connections = total_connections / max(1, len(self.nodes))

            return {
                "total_nodes": len(self.nodes),
                "total_connections": total_connections,
                "avg_connections_per_node": avg_connections,
                "leader": self.get_leader(),
                "node_stats": {node_id: node.get_stats() for node_id, node in self.nodes.items()}
            }


# ============================================================================
# Main Entry Point for Part 3 Test
# ============================================================================

def test_part_3_components():
    print("=" * 60)
    print("DkTC - Part 3: DCG Network Test")
    print("=" * 60)

    # Create hidden root
    hidden_root = HiddenRoot(secret_key="test_secret_123", max_children=100)
    hidden_root.start()

    # Create DCG network
    dcg_network = DCGNetwork(hidden_root=hidden_root, k=7)

    # Register nodes with hidden root and create DCG nodes
    print("\n1. Creating and bootstrapping nodes:")

    nodes_data = []
    for i in range(20):
        node_id = f"node_{i:03d}"
        address = f"192.168.1.{i}:8001"

        # Register with hidden root
        result = hidden_root.register_node(
            node_id=node_id,
            address=address,
            passphrase=f"pass{i}",
            region="us-east" if i % 2 == 0 else "us-west"
        )

        # Create DCG node
        dcg_node = dcg_network.create_node(node_id)

        # Bootstrap connections
        neighbors = result["neighbors"]
        conn_count = dcg_network.bootstrap_node(node_id, neighbors)

        nodes_data.append({
            "node_id": node_id,
            "neighbors": conn_count,
            "k": result["k"]
        })

        if i % 5 == 4:
            print(f"   Created {i + 1} nodes...")

    print(f"\n   Total nodes created: {len(dcg_network.nodes)}")

    # Start all DCG nodes
    print("\n2. Starting DCG nodes:")
    dcg_network.start_all()
    time.sleep(1)

    # Run leader election from a node
    print("\n3. Running leader election:")
    test_node = "node_000"
    leader = dcg_network.run_leader_election(test_node, rounds=3)
    print(f"   Election completed, leader: {leader}")

    # Check node statistics
    print("\n4. Node statistics:")
    for node_id in ["node_000", "node_005", "node_010"]:
        if node_id in dcg_network.nodes:
            node = dcg_network.nodes[node_id]
            stats = node.get_stats()
            conn_summary = node.get_connection_summary()
            print(f"\n   Node {node_id}:")
            print(f"     Score: {stats['score']:.2f}")
            print(f"     Connections: {stats['connections']}")
            print(f"     Leader: {stats['leader_id']}")
            print(f"     Component: {stats['component_id']}")
            print(f"     Component size: {stats['component_size']}")
            print(f"     Avg bandwidth: {conn_summary['avg_bandwidth']:.2f} Mbps")

    # Get network statistics
    print("\n5. Network statistics:")
    net_stats = dcg_network.get_network_stats()
    print(f"   Total nodes: {net_stats['total_nodes']}")
    print(f"   Total connections: {net_stats['total_connections']}")
    print(f"   Avg connections per node: {net_stats['avg_connections_per_node']:.2f}")
    print(f"   System leader: {net_stats['leader']}")

    # Test component detection
    print("\n6. Testing component detection:")
    for node_id in ["node_000", "node_015"]:
        if node_id in dcg_network.nodes:
            component_id = dcg_network.nodes[node_id].detect_component()
            print(f"   Node {node_id} in component: {component_id}")

    # Clean up
    print("\n7. Cleaning up:")
    dcg_network.stop_all()
    hidden_root.stop()

    print("\n" + "=" * 60)
    print("Part 3: DCG Network test completed!")
    print("=" * 60)


"""
DkTC - Part 4: Tree Network (k-ary Task Distribution Tree)

The Tree Network provides:
- Recursive bandwidth-aware tree construction
- k-ary tree with optimal degree Δ ≈ log n / log log n
- Task distribution (broadcast down the tree)
- Result aggregation (convergecast up the tree)
- Load balancing and fault tolerance

Dependencies: Part 1, Part 2, Part 3
"""

from typing import Dict, List, Optional, Set, Tuple, Any, Callable
import threading
import heapq
from collections import deque


# ============================================================================
# Tree Node
# ============================================================================

class TreeNode:
    """
    Node in the k-ary task distribution tree.

    Each node knows:
    - Parent (for result aggregation)
    - Children (for task distribution)
    - Bandwidth to parent and children
    - Task queue and results
    """

    def __init__(self,
                 node_id: str,
                 k: int = None,
                 max_candidates_per_level: int = None):
        """
        Initialize tree node.

        Args:
            node_id: Unique node identifier
            k: Target degree (max children)
            max_candidates_per_level: Max candidates received at each level (20*k default)
        """
        self.node_id = node_id
        self.k = k or DkTCConstants.DEFAULT_K
        self.max_candidates_per_level = max_candidates_per_level or (20 * self.k)

        # Tree structure
        self.parent_id: Optional[str] = None
        self.children_ids: List[str] = []
        self.children_bandwidth: Dict[str, float] = {}  # child_id -> bandwidth

        # Tree building state
        self.tree_level: int = -1
        self.is_root: bool = False
        self.candidates: List[Tuple[float, str]] = []  # (bandwidth, node_id)

        # Registry of peer TreeNodes (set by TreeNetwork) so the simulation can recurse down the tree
        self.network_nodes: Optional[Dict[str, 'TreeNode']] = None

        # Task management
        self.task_queue: PriorityQueue = PriorityQueue()
        self.pending_results: Dict[str, Dict[str, Any]] = {}  # task_id -> {child_id: result}
        self.completed_tasks: Dict[str, Any] = {}

        # Result aggregation
        self.aggregation_callbacks: Dict[str, Callable] = {}

        # Threading
        self.lock = threading.RLock()
        self.running = False

        # Statistics
        self.stats = {
            "tasks_distributed": 0,
            "tasks_processed": 0,
            "results_aggregated": 0,
            "tree_builds": 0
        }

        logger.info(f"TreeNode {node_id} initialized (k={self.k}, max_candidates={self.max_candidates_per_level})")

    # ========================================================================
    # Tree Building (Recursive Bandwidth-Aware Construction)
    # ========================================================================

    def build_tree(self,
                   parent_id: Optional[str],
                   candidates: List[Tuple[float, str]],
                   level: int,
                   bandwidth_matrix: Callable[[str, str], float],
                   marked: Set[str]) -> List[str]:
        """
        Recursive tree building algorithm.

        Algorithm:
        1. Parent picks top-k children by bandwidth from candidates
        2. Marks them as children
        3. Remaining candidates distributed among children based on bandwidth
        4. Each child recurses with its assigned candidates

        Args:
            parent_id: Parent node ID (None for root)
            candidates: List of (bandwidth, node_id) for candidate children
            level: Current tree level
            bandwidth_matrix: Function to get bandwidth between two nodes
            marked: Set of already assigned node IDs

        Returns:
            List of assigned child IDs for this node
        """

        self.tree_level = level
        self.parent_id = parent_id

        if parent_id is None:
            self.is_root = True
            logger.info(f"Building tree from root {self.node_id} at level {level}")
        else:
            self.is_root = False
            logger.debug(f"Node {self.node_id} building at level {level} with {len(candidates)} candidates")

        # If no candidates, this is a leaf
        if not candidates:
            logger.debug(f"Node {self.node_id} is a leaf (no candidates)")
            return []

        # Sort candidates by bandwidth (higher is better)
        sorted_candidates = sorted(candidates, key=lambda x: x[0], reverse=True)

        # Step 1: Pick top-k as direct children
        top_k = sorted_candidates[:self.k]
        remaining = sorted_candidates[self.k:]

        # Record children and their bandwidths
        children = []
        for bandwidth, child_id in top_k:
            if child_id not in marked:
                children.append(child_id)
                self.children_bandwidth[child_id] = bandwidth
                marked.add(child_id)

        self.children_ids = children
        logger.info(f"Node {self.node_id} picked {len(children)} direct children (level {level})")

        # Step 2: Distribute remaining candidates among children based on bandwidth
        if remaining and children:
            # Calculate total bandwidth of children
            total_bw = sum(self.children_bandwidth[child] for child in children)

            if total_bw > 0:
                # Distribute candidates proportionally to child bandwidth
                child_assignments = {child: [] for child in children}
                candidate_idx = 0

                for child in children:
                    child_bw = self.children_bandwidth[child]
                    proportion = child_bw / total_bw
                    num_to_assign = int(len(remaining) * proportion)

                    # Assign candidates to this child
                    assignments = remaining[candidate_idx:candidate_idx + num_to_assign]
                    child_assignments[child] = assignments
                    candidate_idx += num_to_assign

                # Assign any remaining candidates (due to rounding) to highest bandwidth child
                remaining_candidates = remaining[candidate_idx:]
                if remaining_candidates and children:
                    best_child = children[0]  # Already sorted by bandwidth
                    child_assignments[best_child].extend(remaining_candidates)

                # Step 3: Recursively build for each child
                # Note: Each child will receive its assignments and continue
                # In practice, this would be done in parallel via RPCs
                for child_id in children:
                    child_assignments_list = child_assignments.get(child_id, [])
                    if child_assignments_list:
                        logger.debug(
                            f"Node {self.node_id} assigned {len(child_assignments_list)} candidates to child {child_id}")
                        # In production, send RPC to child to continue building
                        # For simulation, we assume children will be built by their own nodes

        self.stats["tree_builds"] += 1

        return children

    def build_tree_with_assignments(self, all_nodes: List[str],
                                    bandwidth_matrix: Callable[[str, str], float]) -> Tuple[
        Dict[str, List[str]], Dict[str, Dict]]:
        """
        Build complete tree recursively and return full structure with assignments.

        Returns:
            Tuple of (tree_structure, assignments)
            tree_structure: Dict[node_id] -> list of children
            assignments: Dict[child_id, {"parent": parent_id, "level": level, "bandwidth": bandwidth}]
        """
        logger.info(f"Building tree from root {self.node_id} with {len(all_nodes)} total nodes")

        # Initialize
        marked = {self.node_id}
        tree_structure = {self.node_id: []}
        assignments = {}

        # Calculate bandwidth from root to all candidates
        candidates = []
        for node_id in all_nodes:
            if node_id != self.node_id:
                bandwidth = bandwidth_matrix(self.node_id, node_id)
                candidates.append((bandwidth, node_id))

        # Recursively build
        self._build_tree_recursive(
            parent_id=self.node_id,
            candidates=candidates,
            level=0,
            bandwidth_matrix=bandwidth_matrix,
            marked=marked,
            tree_structure=tree_structure,
            assignments=assignments
        )

        logger.info(f"Tree built with {len(tree_structure)} nodes")
        return tree_structure, assignments

    def _build_tree_recursive(self, parent_id: str, candidates: List[Tuple[float, str]],
                              level: int, bandwidth_matrix: Callable[[str, str], float],
                              marked: Set[str], tree_structure: Dict[str, List[str]],
                              assignments: Dict[str, Dict]):
        """
        Recursively build tree from parent node.

        Args:
            parent_id: Current parent node
            candidates: List of (bandwidth, node_id) candidates for children
            level: Current tree level
            bandwidth_matrix: Function to get bandwidth between nodes
            marked: Set of already assigned node IDs
            tree_structure: Dictionary being built (node_id -> children)
            assignments: Dictionary of assignments for distribution
        """
        if not candidates:
            return

        # Sort candidates by bandwidth (higher is better)
        sorted_candidates = sorted(candidates, key=lambda x: x[0], reverse=True)

        # Pick top-k as direct children
        k = self.k
        top_k = sorted_candidates[:k]
        remaining = sorted_candidates[k:]

        children = []
        for bandwidth, child_id in top_k:
            if child_id not in marked:
                children.append(child_id)
                marked.add(child_id)

                # Store in tree structure
                if parent_id not in tree_structure:
                    tree_structure[parent_id] = []
                tree_structure[parent_id].append(child_id)
                tree_structure[child_id] = []

                # Store assignment
                assignments[child_id] = {
                    "parent": parent_id,
                    "level": level + 1,
                    "bandwidth": bandwidth
                }

                logger.debug(f"Node {parent_id} assigned child {child_id} at level {level + 1} (bw={bandwidth:.2f})")

        if not children:
            return

        logger.info(f"Node {parent_id} picked {len(children)} direct children at level {level}")

        # Distribute remaining candidates to children based on bandwidth
        if remaining:
            # Calculate child bandwidths
            child_bandwidths = []
            for child_id in children:
                # Find bandwidth from parent to child
                bw = 0
                for b, cid in top_k:
                    if cid == child_id:
                        bw = b
                        break
                child_bandwidths.append((bw, child_id))

            total_bw = sum(bw for bw, _ in child_bandwidths)

            if total_bw > 0:
                # Distribute candidates proportionally
                child_assignments = {child: [] for _, child in child_bandwidths}
                candidate_idx = 0

                for bw, child_id in child_bandwidths:
                    proportion = bw / total_bw
                    num_to_assign = int(len(remaining) * proportion)
                    assigned = remaining[candidate_idx:candidate_idx + num_to_assign]
                    child_assignments[child_id] = assigned
                    candidate_idx += num_to_assign

                # Assign any remaining to highest bandwidth child
                remaining_candidates = remaining[candidate_idx:]
                if remaining_candidates and child_bandwidths:
                    best_child = child_bandwidths[0][1]  # Highest bandwidth
                    child_assignments[best_child].extend(remaining_candidates)

                # Recursively build for each child
                for child_id in children:
                    child_candidates = child_assignments.get(child_id, [])
                    if child_candidates:
                        # Get the actual child node (in production, this would be an RPC)
                        # For simulation, we'll continue recursively using the current instance
                        # but with child as parent
                        logger.debug(f"Node {parent_id} passing {len(child_candidates)} candidates to child {child_id}")

                        # Recursive call for child
                        self._build_tree_recursive(
                            parent_id=child_id,
                            candidates=child_candidates,
                            level=level + 1,
                            bandwidth_matrix=bandwidth_matrix,
                            marked=marked,
                            tree_structure=tree_structure,
                            assignments=assignments
                        )

    def build_tree_from_root(self,
                             all_nodes: List[str],
                             bandwidth_matrix: Callable[[str, str], float]) -> Dict[str, List[str]]:
        """
        Build the entire tree starting from this node as root.

        Args:
            all_nodes: List of all node IDs to include in tree
            bandwidth_matrix: Function to get bandwidth between two nodes

        Returns:
            Dictionary mapping node_id -> list of children
        """
        logger.info(f"Building tree from root {self.node_id} with {len(all_nodes)} total nodes")

        # Initialize marked set with root
        marked = {self.node_id}
        remaining = [n for n in all_nodes if n != self.node_id]

        # Calculate bandwidth from root to all candidates
        candidates = []
        for node_id in remaining:
            bandwidth = bandwidth_matrix(self.node_id, node_id)
            if bandwidth > 0:
                candidates.append((bandwidth, node_id))

        # Build tree recursively
        children = self.build_tree(
            parent_id=None,
            candidates=candidates,
            level=0,
            bandwidth_matrix=bandwidth_matrix,
            marked=marked
        )

        # Build result map
        result_map = {self.node_id: children}

        # In production, would collect results from all children
        # For simulation, we return just the root's view
        logger.info(f"Tree built with root {self.node_id}, {len(children)} direct children")

        return result_map

    # ========================================================================
    # Task Distribution
    # ========================================================================

    PARTITIONABLE_TASKS = ("aggregate", "find_seed")

    def distribute_task(self, task: Task) -> Any:
        """
        Distribute task down the tree (broadcast) and collect results (convergecast).

        - Leaf: processes its slice directly.
        - Internal node: splits the work across children (weighted by bandwidth),
          recurses, and aggregates what comes back.
        - Task types that can't be split run locally on this node.
        """
        if not self.children_ids or task.task_type not in self.PARTITIONABLE_TASKS:
            logger.debug(f"Node {self.node_id} processing task {task.task_id} locally")
            result = self._process_task(task)
            self.stats["tasks_processed"] += 1
            return result

        logger.debug(f"Node {self.node_id} distributing task {task.task_id} to {len(self.children_ids)} children")
        subtasks = self._partition_task_for_children(task)

        child_results = {}
        for child_id, subtask in subtasks.items():
            child_results[child_id] = self._send_to_child(child_id, subtask)

        aggregated = self._aggregate_results(
            child_results, task.task_type, task.parameters.get("operation", "sum"))

        self.stats["tasks_distributed"] += 1
        self.stats["results_aggregated"] += 1
        return aggregated

    @staticmethod
    def _split_counts(total: int, weights: List[float]) -> List[int]:
        """Split `total` items by weights into integer counts that sum exactly to total."""
        wsum = sum(weights)
        if wsum <= 0:
            weights, wsum = [1.0] * len(weights), float(len(weights))
        counts, cum, prev = [], 0.0, 0
        for i, w in enumerate(weights):
            cum += w
            boundary = total if i == len(weights) - 1 else int(round(total * cum / wsum))
            counts.append(boundary - prev)
            prev = boundary
        return counts

    def _partition_task_for_children(self, task: Task) -> Dict[str, Task]:
        """
        Split a task into one real sub-slice per child, weighted by child bandwidth.
        """
        children = list(self.children_ids)
        weights = [self.children_bandwidth.get(c, 0.0) for c in children]
        subtasks: Dict[str, Task] = {}

        if task.task_type == "aggregate":
            values = list(task.parameters.get("values", []))
            counts = self._split_counts(len(values), weights)
            offset = 0
            slices = []
            for n in counts:
                slices.append(values[offset:offset + n])
                offset += n
        else:  # find_seed
            start = task.parameters.get("seed", 0)
            total = task.parameters.get("range", task.parameters.get("seed_range", 1000))
            counts = self._split_counts(total, weights)
            offset = 0
            slices = []
            for n in counts:
                slices.append((start + offset, n))
                offset += n

        for child_id, piece in zip(children, slices):
            params = task.parameters.copy()
            if task.task_type == "aggregate":
                params["values"] = piece
            else:
                params["seed"], params["range"] = piece
            subtasks[child_id] = Task(
                task_id=f"{task.task_id}_{child_id}",
                task_type=task.task_type,
                parameters=params,
                parent_task_id=task.task_id,
                priority=task.priority
            )
        return subtasks

    def _send_to_child(self, child_id: str, subtask: Task) -> Any:
        """
        Send task to child node. In production this is an RPC; in the simulation we
        call the child's TreeNode directly so the whole tree participates.
        """
        logger.debug(f"Sending task {subtask.task_id} to child {child_id}")
        child = self.network_nodes.get(child_id) if self.network_nodes else None
        if child is not None:
            return child.distribute_task(subtask)
        return self._simulate_child_processing(child_id, subtask)

    def _simulate_child_processing(self, child_id: str, subtask: Task) -> Any:
        """Fallback when the child's TreeNode isn't reachable: run the slice locally."""
        return self._process_task(subtask)

    def _process_task(self, task: Task) -> Any:
        """
        Process a task locally (leaf node or fallback).

        Args:
            task: Task to process

        Returns:
            Task result
        """
        # Simulate task processing based on type
        if task.task_type == "find_seed":
            # Simulate seed search
            target = task.parameters.get("target", "unknown")
            seed = task.parameters.get("seed", 0)
            range_val = task.parameters.get("range", 1000)

            # Simple simulation: hash-based search
            for i in range(range_val):
                test_seed = seed + i
                hash_val = hashlib.sha256(f"{target}_{test_seed}".encode()).hexdigest()
                if hash_val.startswith(task.parameters.get("target_hash", "0000")):  # Found a "good" seed
                    return {
                        "found": True,
                        "seed": test_seed,
                        "hash": hash_val,
                        "node": self.node_id
                    }

            return {
                "found": False,
                "node": self.node_id,
                "searches": range_val
            }

        elif task.task_type == "aggregate":
            # Simple aggregation task
            values = task.parameters.get("values", [])
            operation = task.parameters.get("operation", "sum")

            if operation == "sum":
                return sum(values)
            elif operation == "max":
                return max(values) if values else None
            elif operation == "min":
                return min(values) if values else None
            else:
                return values

        elif task.task_type == "compute":
            value = task.parameters.get("value")
            op = task.parameters.get("operation")
            if isinstance(value, (int, float)) and op == "square":
                return value * value
            return {"status": "completed", "node": self.node_id,
                    "expression": task.parameters.get("expression")}

        else:
            # Generic task processing
            return {
                "status": "completed",
                "node": self.node_id,
                "task_type": task.task_type,
                "parameters": task.parameters
            }

    def _aggregate_results(self, child_results: Dict[str, Any], task_type: str,
                           operation: str = "sum") -> Any:
        """
        Aggregate results from children.

        Args:
            child_results: Dictionary mapping child_id -> result
            task_type: Type of task for specialized aggregation
            operation: For "aggregate" tasks: sum | max | min
        """
        if task_type == "aggregate":
            nums = [r for r in child_results.values()
                    if isinstance(r, (int, float)) and not isinstance(r, bool)]
            if operation == "max":
                return max(nums) if nums else None
            if operation == "min":
                return min(nums) if nums else None
            return sum(nums)

        elif task_type == "find_seed":
            # First child that found a seed wins; otherwise report total searched
            searched = 0
            for result in child_results.values():
                if isinstance(result, dict):
                    if result.get("found", False):
                        return result
                    searched += result.get("searches", 0)
            return {"found": False, "searches": searched}

        else:
            return {
                "children": child_results,
                "aggregator": self.node_id,
                "num_children": len(child_results)
            }

    # ========================================================================
    # Result Aggregation (Convergecast)
    # ========================================================================

    def aggregate_results(self, task_id: str, child_id: str, result: Any):
        """
        Aggregate results flowing up the tree.

        Args:
            task_id: Task identifier
            child_id: Child node that sent the result
            result: Result from child
        """
        with self.lock:
            if task_id not in self.pending_results:
                self.pending_results[task_id] = {}

            self.pending_results[task_id][child_id] = result

            # Check if we have results from all children
            if len(self.pending_results[task_id]) >= len(self.children_ids):
                # All children responded, aggregate and send to parent
                aggregated = self._aggregate_results(
                    self.pending_results[task_id],
                    task_id.split('_')[0]  # Extract task type
                )

                # Send to parent (if not root)
                if self.parent_id:
                    # In production, send RPC to parent
                    logger.debug(
                        f"Node {self.node_id} sending aggregated result for {task_id} to parent {self.parent_id}")

                # Clean up
                del self.pending_results[task_id]

                # Store completed task
                self.completed_tasks[task_id] = aggregated

                logger.info(f"Node {self.node_id} completed aggregation for task {task_id}")

    # ========================================================================
    # Tree Maintenance
    # ========================================================================

    def update_bandwidth(self, child_id: str, new_bandwidth: float):
        """
        Update bandwidth measurement to a child.

        Args:
            child_id: Child node ID
            new_bandwidth: New bandwidth value
        """
        with self.lock:
            if child_id in self.children_bandwidth:
                # Exponential moving average
                alpha = 0.3
                self.children_bandwidth[child_id] = (
                        alpha * new_bandwidth +
                        (1 - alpha) * self.children_bandwidth[child_id]
                )
                logger.debug(f"Updated bandwidth to {child_id}: {self.children_bandwidth[child_id]:.2f} Mbps")

    def handle_child_failure(self, failed_child_id: str):
        """
        Handle failure of a child node.

        Args:
            failed_child_id: ID of failed child
        """
        with self.lock:
            if failed_child_id in self.children_ids:
                self.children_ids.remove(failed_child_id)
                if failed_child_id in self.children_bandwidth:
                    del self.children_bandwidth[failed_child_id]
                logger.warning(f"Node {self.node_id} lost child {failed_child_id}")

                # In production, would trigger rebalancing
                self._rebalance_children()

    def _rebalance_children(self):
        """
        Rebalance children after failure or topology change.
        """
        # Simple rebalancing: if we have too few children, request more
        if len(self.children_ids) < self.k // 2:
            logger.info(f"Node {self.node_id} requesting more children (currently {len(self.children_ids)})")
            # In production, would send request to parent or hidden root

    # ========================================================================
    # Statistics
    # ========================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Get tree node statistics."""
        with self.lock:
            return {
                "node_id": self.node_id,
                "is_root": self.is_root,
                "tree_level": self.tree_level,
                "parent_id": self.parent_id,
                "children_count": len(self.children_ids),
                "children_ids": self.children_ids[:10],  # First 10 only
                "k": self.k,
                "stats": self.stats.copy(),
                "pending_tasks": len(self.pending_results),
                "completed_tasks": len(self.completed_tasks)
            }


# ============================================================================
# Tree Network Manager
# ============================================================================

class TreeNetwork:
    """
    Manages the k-ary tree network across multiple nodes.
    """

    def __init__(self, hidden_root: HiddenRoot, dcg_network: DCGNetwork, k: int = None):
        """
        Initialize tree network.

        Args:
            hidden_root: Reference to hidden root
            dcg_network: Reference to DCG network for bandwidth info
            k: Target tree degree
        """
        self.hidden_root = hidden_root
        self.dcg_network = dcg_network
        self.k = k or DkTCConstants.DEFAULT_K

        self.nodes: Dict[str, TreeNode] = {}
        self.root_id: Optional[str] = None
        self.tree_structure: Dict[str, List[str]] = {}  # node_id -> children

        self.lock = threading.RLock()

        logger.info(f"TreeNetwork initialized (k={self.k})")

    def create_node(self, node_id: str) -> TreeNode:
        """
        Create a new tree node.

        Args:
            node_id: Unique node identifier

        Returns:
            Created TreeNode instance
        """
        with self.lock:
            if node_id in self.nodes:
                return self.nodes[node_id]

            node = TreeNode(node_id=node_id, k=self.k)
            node.network_nodes = self.nodes  # shared dict: peers become visible as they're created
            self.nodes[node_id] = node
            logger.info(f"Created tree node {node_id}")
            return node

    def build_tree(self, root_id: str) -> Dict[str, List[str]]:
        """Build the complete tree starting from root."""
        if root_id not in self.nodes:
            raise ValueError(f"Root node {root_id} not found")

        self.root_id = root_id
        root_node = self.nodes[root_id]

        # Get bandwidth function with fallback
        def get_bandwidth(node1: str, node2: str) -> float:
            dcg_node1 = self.dcg_network.nodes.get(node1)
            if dcg_node1 and node2 in dcg_node1.connections:
                return dcg_node1.connections[node2].bandwidth

            # Fallback based on region
            node1_info = self.hidden_root.get_node_info(node1)
            node2_info = self.hidden_root.get_node_info(node2)
            if node1_info and node2_info:
                return 100.0 if node1_info.region == node2_info.region else 50.0
            return 10.0

        # Get all nodes
        all_nodes = [info.node_id for info in self.hidden_root.get_all_nodes()]

        # Build tree recursively
        self.tree_structure, self.pending_assignments = root_node.build_tree_with_assignments(
            all_nodes, get_bandwidth
        )

        # Update hidden root with parent-child relationships
        for node_id, children in self.tree_structure.items():
            node_info = self.hidden_root.get_node_info(node_id)
            if node_info:
                node_info.children_ids = children
                if node_id == root_id:
                    node_info.parent_id = None
                    node_info.status = NodeStatus.ROOT
                else:
                    # Find parent from assignments
                    if node_id in self.pending_assignments:
                        node_info.parent_id = self.pending_assignments[node_id]["parent"]

        # Update tree nodes with parent/child info
        for node_id, node in self.nodes.items():
            if node_id in self.tree_structure:
                node.children_ids = self.tree_structure[node_id]
            if node_id in self.pending_assignments:
                node.parent_id = self.pending_assignments[node_id]["parent"]
                node.tree_level = self.pending_assignments[node_id]["level"]
            elif node_id == root_id:
                node.is_root = True
                node.tree_level = 0

        logger.info(f"Tree built with root {root_id}, {len(self.tree_structure)} nodes")
        return self.tree_structure

    def distribute_tree_assignments(self):
        """Send tree assignments to all nodes"""
        if not hasattr(self, 'pending_assignments'):
            return

        for child_id, assignment in self.pending_assignments.items():
            # In production, send RPC to child
            logger.debug(
                f"Would send assignment to {child_id}: parent={assignment['parent']}, level={assignment['level']}")

    def get_root(self) -> Optional[TreeNode]:
        """Get the root tree node."""
        if self.root_id and self.root_id in self.nodes:
            return self.nodes[self.root_id]
        return None

    def get_path_to_root(self, node_id: str) -> List[str]:
        """
        Get path from node to root.

        Args:
            node_id: Starting node ID

        Returns:
            List of node IDs from node to root
        """
        path = []
        current = node_id

        while current:
            path.append(current)
            node_info = self.hidden_root.get_node_info(current)
            if node_info:
                current = node_info.parent_id
            else:
                break

        return path

    def distribute_task(self, task: Task) -> Any:
        """
        Distribute task from root down the tree.

        Args:
            task: Task to distribute

        Returns:
            Aggregated result
        """
        root = self.get_root()
        if not root:
            logger.error("No root available for task distribution")
            return None

        logger.info(f"Distributing task {task.task_id} from root {root.node_id}")
        return root.distribute_task(task)

    def get_stats(self) -> Dict[str, Any]:
        """Get tree network statistics."""
        with self.lock:
            return {
                "total_nodes": len(self.nodes),
                "root_id": self.root_id,
                "tree_depth": self._calculate_depth(),
                "node_stats": {node_id: node.get_stats() for node_id, node in self.nodes.items()}
            }

    def _calculate_depth(self) -> int:
        """Calculate maximum depth of the tree."""
        if not self.root_id:
            return 0

        max_depth = 0
        for node_id in self.nodes:
            path_len = len(self.get_path_to_root(node_id))
            max_depth = max(max_depth, path_len)

        return max_depth


# ============================================================================
# Main Entry Point for Part 4 Test
# ============================================================================

def test_part_4_components():
    print("=" * 60)
    print("DkTC - Part 4: Tree Network Test")
    print("=" * 60)

    # Create hidden root
    hidden_root = HiddenRoot(secret_key="test_secret_123", max_children=100)
    hidden_root.start()

    # Register nodes with hidden root
    print("\n1. Registering nodes:")
    for i in range(15):
        node_id = f"node_{i:03d}"
        address = f"192.168.1.{i}:8001"
        hidden_root.register_node(
            node_id=node_id,
            address=address,
            passphrase=f"pass{i}",
            region="us-east" if i % 2 == 0 else "us-west"
        )
        if i % 5 == 4:
            print(f"   Registered {i + 1} nodes...")

    # Create DCG network (for bandwidth info)
    dcg_network = DCGNetwork(hidden_root=hidden_root, k=7)

    # Create DCG nodes and establish connections
    print("\n2. Creating DCG network for bandwidth measurements:")
    all_nodes = hidden_root.get_all_nodes()
    for node_info in all_nodes:
        dcg_node = dcg_network.create_node(node_info.node_id)
        # Simulate some connections
        for other_info in all_nodes:
            if other_info.node_id != node_info.node_id:
                # Add connection with simulated bandwidth
                dcg_node.connections[other_info.node_id] = Connection(
                    node_id=other_info.node_id,
                    bandwidth=random.uniform(10, 1000),
                    last_updated=time.time()
                )
        print(f"   Created DCG node {node_info.node_id} with {dcg_node.get_connection_count()} connections")

    # Create tree network
    print("\n3. Creating tree network:")
    tree_network = TreeNetwork(hidden_root=hidden_root, dcg_network=dcg_network, k=5)

    # Create tree nodes
    for node_info in all_nodes:
        tree_network.create_node(node_info.node_id)
    print(f"   Created {len(tree_network.nodes)} tree nodes")

    # Build tree from root
    print("\n4. Building tree from root node_000:")
    tree_structure = tree_network.build_tree(root_id="node_000")
    print(f"   Tree built with {len(tree_structure)} nodes in structure")

    # Display tree structure
    print("\n5. Tree structure:")
    for node_id, children in list(tree_structure.items())[:5]:  # Show first 5
        print(f"   {node_id} -> {children[:3]}..." if len(children) > 3 else f"   {node_id} -> {children}")

    # Get tree statistics
    print("\n6. Tree statistics:")
    tree_stats = tree_network.get_stats()
    print(f"   Total nodes: {tree_stats['total_nodes']}")
    print(f"   Root: {tree_stats['root_id']}")
    print(f"   Tree depth: {tree_stats['tree_depth']}")

    # Test task distribution
    print("\n7. Testing task distribution:")
    test_task = Task(
        task_id="test_001",
        task_type="aggregate",
        parameters={"values": [1, 2, 3, 4, 5], "operation": "sum"},
        priority=1
    )

    result = tree_network.distribute_task(test_task)
    print(f"   Task result: {result}")

    # Test find_seed task
    print("\n8. Testing find_seed task:")
    seed_task = Task(
        task_id="seed_001",
        task_type="find_seed",
        parameters={"target": "abc123", "seed": 0, "range": 5000},
        priority=0
    )

    seed_result = tree_network.distribute_task(seed_task)
    print(f"   Seed search result: {seed_result}")

    # Get node-specific stats
    print("\n9. Node statistics:")
    for node_id in ["node_000", "node_001", "node_002"]:
        if node_id in tree_network.nodes:
            node_stats = tree_network.nodes[node_id].get_stats()
            print(f"\n   Node {node_id}:")
            print(f"     Is root: {node_stats['is_root']}")
            print(f"     Level: {node_stats['tree_level']}")
            print(f"     Children: {node_stats['children_count']}")
            print(f"     Tasks processed: {node_stats['stats']['tasks_processed']}")

    # Clean up
    print("\n10. Cleaning up:")
    hidden_root.stop()

    print("\n" + "=" * 60)
    print("Part 4: Tree Network test completed!")
    print("=" * 60)


"""
DkTC - Part 5: Worker Node

The Worker Node integrates:
- Hidden Root registration and communication
- DCG network (gossip, leader election, failure detection)
- Tree network (task distribution, result aggregation)
- Task execution and result reporting

Dependencies: Part 1, Part 2, Part 3, Part 4
"""

from typing import Dict, List, Optional, Set, Tuple, Any, Callable
import threading
import asyncio
import queue
from enum import Enum


# ============================================================================
# Worker Node Status
# ============================================================================

class WorkerStatus(Enum):
    """Status of a worker node in the system"""
    INITIALIZING = "initializing"
    REGISTERING = "registering"
    BOOTSTRAPPING = "bootstrapping"
    ELECTING = "electing"
    JOINING_TREE = "joining_tree"
    ACTIVE = "active"
    ROOT = "root"
    DEGRADED = "degraded"
    LEAVING = "leaving"
    FAILED = "failed"


# ============================================================================
# Message Queue for Async Communication
# ============================================================================

class MessageQueue:
    """Thread-safe message queue for inter-node communication"""

    def __init__(self):
        self.queue = queue.Queue()
        self.priority_queue = PriorityQueue()
        self.lock = threading.RLock()

    def send(self, to_node: str, msg_type: MessageType, payload: Any, priority: int = 5):
        """Send a message to another node"""
        message = {
            "to": to_node,
            "type": msg_type.value,
            "payload": payload,
            "timestamp": time.time(),
            "priority": priority
        }
        self.priority_queue.push(message, priority)

    def receive(self, timeout: float = None) -> Optional[Dict]:
        """Receive a message"""
        if timeout:
            # Would implement timeout, for now just pop
            pass
        return self.priority_queue.pop()

    def has_messages(self) -> bool:
        """Check if there are messages to process"""
        return not self.priority_queue.is_empty()


# ============================================================================
# Worker Node - Complete Implementation
# ============================================================================

class WorkerNode:
    """
    Complete worker node implementation.

    Integrates:
    1. Hidden Root communication (registration, heartbeat, task submission)
    2. DCG network (connections, gossip, leader election)
    3. Tree network (parent/child relationships, task distribution)
    4. Task execution engine
    5. Message processing
    """

    def __init__(self,
                 node_id: str,
                 address: str,
                 passphrase: str,
                 hidden_root: HiddenRoot,
                 k: int = None,
                 region: str = "default"):
        """
        Initialize worker node.

        Args:
            node_id: Unique node identifier
            address: Network address (ip:port)
            passphrase: Authentication passphrase
            hidden_root: Reference to hidden root
            k: Target tree degree
            region: Geographic/network region
        """
        self.node_id = node_id
        self.address = address
        self.passphrase = passphrase
        self.hidden_root = hidden_root
        self.region = region
        self.k = k or DkTCConstants.DEFAULT_K

        # Status tracking
        self.status = WorkerStatus.INITIALIZING
        self.certificate: Optional[str] = None
        self.start_time = time.time()
        self.is_root: bool = False
        self._elected_root_id = None

        # Networks
        self.dcg_node: Optional[DCGNode] = None
        self.tree_node: Optional[TreeNode] = None

        # Parent/child relationships
        self.parent_id: Optional[str] = None
        self.children_ids: List[str] = []
        self.children_bandwidth: Dict[str, float] = {}

        # Task management
        self.task_executor: Optional[TaskExecutor] = None
        self.active_tasks: Dict[str, Task] = {}
        self.task_results: Dict[str, Any] = {}
        self.task_lock = threading.RLock()

        # Communication
        self.message_queue = MessageQueue()
        self.message_handlers: Dict[MessageType, Callable] = {}
        self._register_message_handlers()

        # Threading
        self.lock = threading.RLock()
        self.running = False
        self.threads: List[threading.Thread] = []

        # Statistics
        self.stats = {
            "messages_sent": 0,
            "messages_received": 0,
            "tasks_executed": 0,
            "tasks_forwarded": 0,
            "heartbeats_sent": 0,
            "election_rounds": 0
        }

        logger.info(f"WorkerNode {node_id} initialized (k={self.k}, region={region})")

    def _register_message_handlers(self):
        """Register handlers for different message types"""
        self.message_handlers[MessageType.PING] = self._handle_ping
        self.message_handlers[MessageType.PONG] = self._handle_pong
        self.message_handlers[MessageType.JOIN_REQUEST] = self._handle_join_request
        self.message_handlers[MessageType.LEADER_ANNOUNCE] = self._handle_leader_announce
        self.message_handlers[MessageType.TREE_ASSIGNMENT] = self._handle_tree_assignment
        self.message_handlers[MessageType.TASK_SUBMIT] = self._handle_task_submit
        self.message_handlers[MessageType.TASK_RESULT] = self._handle_task_result
        self.message_handlers[MessageType.HEARTBEAT] = self._handle_heartbeat
        self.message_handlers[MessageType.TREE_ASSIGNMENT] = self._handle_tree_assignment

    # ========================================================================
    # Initialization and Registration
    # ========================================================================

    def initialize(self) -> bool:
        """
        Initialize the worker node.

        Steps:
        1. Register with hidden root
        2. Get bootstrap neighbors
        3. Create DCG node and establish bi-connections
        4. Create tree node
        5. Start background threads

        Returns:
            True if initialization successful
        """
        logger.info(f"Worker {self.node_id} initializing...")

        # Step 1: Register with hidden root
        self.status = WorkerStatus.REGISTERING

        registration = self.hidden_root.register_node(
            node_id=self.node_id,
            address=self.address,
            passphrase=self.passphrase,
            region=self.region
        )

        self.certificate = registration["certificate"]
        neighbors = registration["neighbors"]
        self.k = registration.get("k", self.k)

        logger.info(f"Worker {self.node_id} registered, got {len(neighbors)} neighbors")

        # Step 2: Create DCG node
        self.status = WorkerStatus.BOOTSTRAPPING
        self.dcg_node = DCGNode(
            node_id=self.node_id,
            hidden_root=self.hidden_root,
            k=self.k,
            max_connections=DkTCConstants.MAX_CONNECTIONS_PER_NODE
        )

        # Step 3: Establish bidirectional connections
        # We need to get the DCGNetwork reference
        # This requires passing dcg_network to WorkerNode
        # For now, we'll simulate by establishing connections manually

        for neighbor_id, address in neighbors:
            bandwidth = self.dcg_node._measure_bandwidth(neighbor_id, address)
            if bandwidth > 0:
                # Add forward connection
                self.dcg_node.connections[neighbor_id] = Connection(
                    node_id=neighbor_id,
                    bandwidth=bandwidth,
                    last_updated=time.time()
                )
                # Note: Reverse connection must be added by neighbor
                # This requires coordination through DCGNetwork

        conn_count = len(self.dcg_node.connections)
        logger.info(f"Worker {self.node_id} established {conn_count} DCG connections")

        # Step 4: Create tree node
        self.tree_node = TreeNode(node_id=self.node_id, k=self.k)

        # Step 5: Create task executor
        self.task_executor = TaskExecutor(self.node_id)

        # Step 6: Start background threads
        self.start()

        self.status = WorkerStatus.ACTIVE
        logger.info(f"Worker {self.node_id} initialization complete")

        return True

    # ========================================================================
    # Leader Election Participation
    # ========================================================================

    def run_leader_election(self, rounds: int = None) -> str:
        """
        Run leader election through DCG.

        Args:
            rounds: Number of election rounds

        Returns:
            Elected leader ID
        """
        if not self.dcg_node:
            logger.error(f"Worker {self.node_id} has no DCG node")
            return self.node_id

        self.status = WorkerStatus.ELECTING
        self.stats["election_rounds"] += 1

        leader_id = self.dcg_node.run_leader_election(rounds)

        # Store the elected root ID for health check
        self._elected_root_id = leader_id

        if leader_id == self.node_id:
            logger.info(f"Worker {self.node_id} elected as leader!")
            # Register as root with hidden root
            self.hidden_root.update_root(self.node_id, self.certificate)
            # Mark this node as root in tree_node
            if self.tree_node:
                self.tree_node.is_root = True
                self.tree_node.tree_level = 0
            self.is_root = True
            self.parent_id = None  # Root has no parent
            self.status = WorkerStatus.ROOT
        else:
            logger.info(f"Worker {self.node_id} acknowledged leader {leader_id}")
            self.is_root = False
            if self.tree_node:
                self.tree_node.is_root = False
            self.status = WorkerStatus.ACTIVE

        return leader_id

    def broadcast_root(self, root_id: int):
        self._elected_root_id = root_id

    # ========================================================================
    # Tree Joining
    # ========================================================================

    def join_tree(self, parent_id: str, bandwidth: float, level: int):
        """
        Join the tree as a child of parent.

        Args:
            parent_id: Parent node ID
            bandwidth: Bandwidth to parent
            level: Level in the tree
        """
        self.status = WorkerStatus.JOINING_TREE
        self.parent_id = parent_id

        if self.tree_node:
            self.tree_node.parent_id = parent_id
            self.tree_node.tree_level = level
            self.tree_node.is_root = False

        logger.info(f"Worker {self.node_id} joined tree as child of {parent_id} (level={level}, bw={bandwidth:.2f})")
        self.status = WorkerStatus.ACTIVE

    def add_child(self, child_id: str, bandwidth: float):
        """
        Add a child node.

        Args:
            child_id: Child node ID
            bandwidth: Bandwidth to child
        """
        if child_id not in self.children_ids:
            self.children_ids.append(child_id)
            self.children_bandwidth[child_id] = bandwidth

            if self.tree_node:
                self.tree_node.children_ids = self.children_ids
                self.tree_node.children_bandwidth[child_id] = bandwidth

            logger.info(f"Worker {self.node_id} added child {child_id} (bw={bandwidth:.2f})")

    def remove_child(self, child_id: str):
        """
        Remove a child node (failure or leave).

        Args:
            child_id: Child node ID to remove
        """
        if child_id in self.children_ids:
            self.children_ids.remove(child_id)
            if child_id in self.children_bandwidth:
                del self.children_bandwidth[child_id]

            if self.tree_node and hasattr(self.tree_node, 'handle_child_failure'):
                self.tree_node.handle_child_failure(child_id)

            logger.warning(f"Worker {self.node_id} removed child {child_id}")

    # ========================================================================
    # Task Processing
    # ========================================================================

    def submit_task(self, task_type: str, parameters: Dict[str, Any],
                    priority: int = 0) -> str:
        """
        Submit a task to the system (forward to root).

        Args:
            task_type: Type of task
            parameters: Task parameters
            priority: Task priority

        Returns:
            Task ID
        """
        # Forward to hidden root (which forwards to system root)
        task_id = self.hidden_root.submit_task(task_type, parameters, priority)
        logger.info(f"Worker {self.node_id} submitted task {task_id} (type={task_type})")
        return task_id

    def process_task(self, task: Task) -> Any:
        """
        Process a task (called by parent or root).

        Args:
            task: Task to process

        Returns:
            Task result
        """
        with self.task_lock:
            self.active_tasks[task.task_id] = task
            self.stats["tasks_executed"] += 1

        logger.debug(f"Worker {self.node_id} processing task {task.task_id} (type={task.task_type})")

        # Determine if we need to distribute or process locally
        if self.children_ids and self.should_distribute_task(task):
            # Distribute to children
            result = self.distribute_to_children(task)
        else:
            # Process locally (leaf or small task)
            result = self.execute_local_task(task)

        with self.task_lock:
            if task.task_id in self.active_tasks:
                del self.active_tasks[task.task_id]
            self.task_results[task.task_id] = result

        return result

    def should_distribute_task(self, task: Task) -> bool:
        """
        Determine if task should be distributed to children.

        Args:
            task: Task to check

        Returns:
            True if should distribute
        """
        # Distribute if we have children and task is large enough
        if len(self.children_ids) == 0:
            return False

        # Check task complexity
        complexity = task.parameters.get("complexity", 0)
        threshold = task.parameters.get("distribution_threshold", 100)

        return complexity > threshold

    def distribute_to_children(self, task: Task) -> Dict[str, Any]:
        """
        Distribute task to children and aggregate results.

        Args:
            task: Task to distribute

        Returns:
            Aggregated results
        """
        if self.tree_node:
            result = self.tree_node.distribute_task(task)
            self.stats["tasks_forwarded"] += 1
            return result

        # Fallback: simple distribution
        results = {}
        for child_id in self.children_ids:
            # In production, send RPC to child
            results[child_id] = self._send_task_to_child(child_id, task)

        return self._aggregate_child_results(results, task.task_type)

    def execute_local_task(self, task: Task) -> Any:
        """
        Execute task locally.

        Args:
            task: Task to execute

        Returns:
            Task result
        """
        if self.task_executor:
            return self.task_executor.execute(task)

        # Fallback: simple execution
        if task.task_type == "aggregate":
            values = task.parameters.get("values", [])
            return sum(values)
        elif task.task_type == "find_seed":
            target = task.parameters.get("target", "")
            seed_range = task.parameters.get("range", 1000)
            # Simple simulation
            return {"found": False, "node": self.node_id}
        else:
            return {"status": "completed", "node": self.node_id}

    def _send_task_to_child(self, child_id: str, task: Task) -> Any:
        """
        Send task to child node (simulated).

        Args:
            child_id: Child node ID
            task: Task to send

        Returns:
            Child's result
        """
        # In production: RPC call
        # For simulation: simulate child processing
        compute_time = random.uniform(0.001, 0.01)
        time.sleep(compute_time)

        return {
            "node_id": child_id,
            "task_id": task.task_id,
            "result": f"processed_by_{child_id}",
            "compute_time": compute_time
        }

    def _aggregate_child_results(self, results: Dict[str, Any], task_type: str) -> Any:
        """
        Aggregate results from children.

        Args:
            results: Child results
            task_type: Task type

        Returns:
            Aggregated result
        """
        if task_type == "aggregate":
            total = 0
            for result in results.values():
                if isinstance(result, (int, float)):
                    total += result
                elif isinstance(result, dict) and "result" in result:
                    total += result["result"] if isinstance(result["result"], (int, float)) else 0
            return total
        else:
            return {"children_results": results, "aggregator": self.node_id}

    # ========================================================================
    # Message Handling
    # ========================================================================

    def _handle_ping(self, sender: str, payload: Dict):
        """Handle ping message"""
        self._send_message(sender, MessageType.PONG, {"timestamp": time.time()})
        self.stats["messages_sent"] += 1

    def _handle_pong(self, sender: str, payload: Dict):
        """Handle pong message"""
        latency = time.time() - payload.get("timestamp", 0)
        logger.debug(f"Pong from {sender}: latency={latency:.3f}s")

    def _handle_join_request(self, sender: str, payload: Dict):
        """Handle join request from new node"""
        child_id = payload.get("node_id")
        bandwidth = payload.get("bandwidth", 0)

        if len(self.children_ids) < self.k:
            self.add_child(child_id, bandwidth)
            self._send_message(sender, MessageType.JOIN_RESPONSE, {
                "status": "accepted",
                "parent_id": self.node_id,
                "level": self.tree_node.tree_level + 1 if self.tree_node else 1
            })
        else:
            self._send_message(sender, MessageType.JOIN_RESPONSE, {
                "status": "rejected",
                "reason": "max_children_reached"
            })

    def _handle_leader_announce(self, sender: str, payload: Dict):
        """Handle leader announcement"""
        leader_id = payload.get("leader_id")
        leader_score = payload.get("score", 0)

        if self.dcg_node:
            self.dcg_node.component_leader_id = leader_id
            self.dcg_node.leader_id = leader_id

        logger.info(f"Worker {self.node_id} acknowledged leader {leader_id} (score={leader_score:.2f})")

    def _handle_tree_assignment(self, sender: str, payload: Dict):
        """Handle tree assignment from parent"""
        parent_id = payload.get("parent_id")
        bandwidth = payload.get("bandwidth", 0)
        level = payload.get("level", 0)
        candidates = payload.get("candidates", [])  # List of (bandwidth, node_id) for subtrees
        all_nodes = payload.get("all_nodes", [])

        # Set parent relationship
        self.parent_id = parent_id

        # Update tree node
        if self.tree_node:
            self.tree_node.parent_id = parent_id
            self.tree_node.tree_level = level
            self.tree_node.is_root = False

        logger.info(f"Worker {self.node_id} assigned as child of {parent_id} (level={level})")

        # If this node has candidates, build its subtree
        if candidates and self.tree_node:
            # Build subtree recursively
            self._build_subtree(candidates, all_nodes, level + 1)

    def _build_subtree(self, candidates: List[Tuple[float, str]], all_nodes: List[str], level: int):
        """Build subtree from assigned candidates"""
        # Mark self as visited
        marked = {self.node_id}

        # Build tree from this node as root of its subtree
        self.tree_node.build_tree(
            parent_id=self.node_id,
            candidates=candidates,
            level=level,
            bandwidth_matrix=self._get_bandwidth_function(),
            marked=marked
        )

        # Notify children of their assignments (in production, send RPCs)
        for child_id in self.tree_node.children_ids:
            # In production, send TREE_ASSIGNMENT message to child
            logger.debug(f"Worker {self.node_id} would assign child {child_id}")

    def _get_bandwidth_function(self):
        """Return bandwidth function for tree building"""

        def get_bandwidth(node1: str, node2: str) -> float:
            if self.dcg_node and node2 in self.dcg_node.connections:
                return self.dcg_node.connections[node2].bandwidth
            # Fallback based on region
            return 50.0

        return get_bandwidth

    def _handle_task_submit(self, sender: str, payload: Dict):
        """Handle task submission from parent"""
        task_data = payload.get("task")
        task = Task(
            task_id=task_data["task_id"],
            task_type=task_data["task_type"],
            parameters=task_data["parameters"],
            priority=task_data.get("priority", 0)
        )

        result = self.process_task(task)

        # Send result back
        self._send_message(sender, MessageType.TASK_RESULT, {
            "task_id": task.task_id,
            "result": result,
            "node_id": self.node_id
        })

    def _handle_task_result(self, sender: str, payload: Dict):
        """Handle task result from child"""
        task_id = payload.get("task_id")
        result = payload.get("result")

        if self.tree_node:
            self.tree_node.aggregate_results(task_id, sender, result)

    def _handle_heartbeat(self, sender: str, payload: Dict):
        """Handle heartbeat message"""
        if self.dcg_node:
            self.dcg_node.receive_heartbeat(sender)

    def _send_message(self, to_node: str, msg_type: MessageType, payload: Dict):
        """Send a message to another node"""
        self.message_queue.send(to_node, msg_type, payload)
        self.stats["messages_sent"] += 1

    def _process_incoming_messages(self):
        """Process incoming messages from queue"""
        while self.running:
            try:
                msg = self.message_queue.receive(timeout=0.1)
                if msg:
                    msg_type = MessageType(msg["type"])
                    sender = msg["payload"].get("sender", "unknown")
                    handler = self.message_handlers.get(msg_type)

                    if handler:
                        handler(sender, msg["payload"])
                        self.stats["messages_received"] += 1
                    else:
                        logger.warning(f"Unknown message type: {msg_type}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")

            time.sleep(0.01)

    # ========================================================================
    # Heartbeat and Health
    # ========================================================================

    def send_heartbeat(self):
        """Send heartbeat to hidden root and neighbors"""
        if self.dcg_node:
            self.dcg_node.send_heartbeat()
            self.stats["heartbeats_sent"] += 1

        # Send to hidden root with certificate
        if self.certificate:
            self.hidden_root.update_heartbeat(self.node_id, self.certificate)

    def check_health(self) -> bool:
        """
        Check node health.

        Returns:
            True if node is healthy
        """
        # Check if we have enough connections
        if self.dcg_node and self.dcg_node.get_connection_count() < 10:
            logger.warning(f"Worker {self.node_id} has low connection count")
            self.status = WorkerStatus.DEGRADED
            return False

        # Determine if this node is the root
        is_root_node = False
        if self.tree_node:
            is_root_node = self.tree_node.is_root
        elif self.is_root:
            is_root_node = True
        elif self._elected_root_id is not None and self.node_id == self._elected_root_id:
            is_root_node = True
            self.is_root = True

        # Check if we have a parent (root nodes don't need parents)
        if self.parent_id is None and not is_root_node:
            # Non-root with no parent is disconnected
            logger.warning(f"Worker {self.node_id} has no parent and is not root")
            return False

        return True

    # ========================================================================
    # Main Loop and Threading
    # ========================================================================

    def start(self):
        """Start worker node threads"""
        self.running = True

        # Message processing thread
        msg_thread = threading.Thread(target=self._process_incoming_messages, daemon=True)
        msg_thread.start()
        self.threads.append(msg_thread)

        # Heartbeat thread
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        self.threads.append(heartbeat_thread)

        # Health check thread
        health_thread = threading.Thread(target=self._health_loop, daemon=True)
        health_thread.start()
        self.threads.append(health_thread)

        logger.info(f"Worker {self.node_id} started {len(self.threads)} background threads")

    def _heartbeat_loop(self):
        """Periodic heartbeat sending"""
        while self.running:
            time.sleep(DkTCConstants.HEARTBEAT_INTERVAL)
            self.send_heartbeat()

    def _health_loop(self):
        """Periodic health checking"""
        while self.running:
            time.sleep(DkTCConstants.HEARTBEAT_INTERVAL * 3)
            self.check_health()

    def stop(self):
        """Stop worker node"""
        logger.info(f"Worker {self.node_id} stopping...")
        self.running = False
        self.status = WorkerStatus.LEAVING

        # Notify hidden root
        self.hidden_root.remove_node(self.node_id, "leaving")

        # Stop threads
        for thread in self.threads:
            thread.join(timeout=2)

        logger.info(f"Worker {self.node_id} stopped")

    # ========================================================================
    # Statistics
    # ========================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Get worker node statistics"""
        with self.lock:
            stats = {
                "node_id": self.node_id,
                "status": self.status.value,
                "uptime": time.time() - self.start_time,
                "parent_id": self.parent_id,
                "children_count": len(self.children_ids),
                "k": self.k,
                "stats": self.stats.copy(),
                "tree_level": self.tree_node.tree_level if self.tree_node else -1
            }

            if self.dcg_node:
                stats["dcg"] = self.dcg_node.get_stats()

            return stats


# ============================================================================
# Task Executor
# ============================================================================

class TaskExecutor:
    """
    Executes tasks locally on a worker node.
    Supports various task types and parallel execution.
    """

    def __init__(self, node_id: str, max_workers: int = 4):
        """
        Initialize task executor.

        Args:
            node_id: Node ID for logging
            max_workers: Maximum concurrent tasks
        """
        self.node_id = node_id
        self.max_workers = max_workers
        self.active_workers = 0
        self.lock = threading.RLock()
        self.task_handlers: Dict[str, Callable] = {}
        self._register_handlers()

        logger.info(f"TaskExecutor for {node_id} initialized with {max_workers} workers")

    def _register_handlers(self):
        """Register task type handlers"""
        self.task_handlers["aggregate"] = self._handle_aggregate
        self.task_handlers["find_seed"] = self._handle_find_seed
        self.task_handlers["compute"] = self._handle_compute
        self.task_handlers["filter"] = self._handle_filter
        self.task_handlers["map"] = self._handle_map

    def execute(self, task: Task) -> Any:
        """
        Execute a task.

        Args:
            task: Task to execute

        Returns:
            Task result
        """
        handler = self.task_handlers.get(task.task_type)

        if handler:
            return handler(task)
        else:
            logger.warning(f"Unknown task type: {task.task_type}")
            return {"error": f"Unknown task type: {task.task_type}", "node": self.node_id}

    def _handle_aggregate(self, task: Task) -> Any:
        """Handle aggregate task"""
        values = task.parameters.get("values", [])
        operation = task.parameters.get("operation", "sum")

        if operation == "sum":
            return sum(values)
        elif operation == "max":
            return max(values) if values else None
        elif operation == "min":
            return min(values) if values else None
        elif operation == "avg":
            return sum(values) / len(values) if values else 0
        else:
            return values

    def _handle_find_seed(self, task: Task) -> Dict[str, Any]:
        """Handle find_seed task"""
        target = task.parameters.get("target", "")
        start_seed = task.parameters.get("seed", 0)
        end_seed = start_seed + task.parameters.get("range", 1000)
        target_hash = task.parameters.get("target_hash", "0000")

        for seed in range(start_seed, end_seed):
            test_input = f"{target}_{seed}"
            hash_val = hashlib.sha256(test_input.encode()).hexdigest()

            if hash_val.startswith(target_hash):
                return {
                    "found": True,
                    "seed": seed,
                    "hash": hash_val,
                    "node": self.node_id
                }

        return {
            "found": False,
            "start_seed": start_seed,
            "end_seed": end_seed,
            "node": self.node_id
        }

    def _handle_compute(self, task: Task) -> Dict[str, Any]:
        """Handle generic compute task"""
        expression = task.parameters.get("expression", "")
        # Simple simulation
        return {
            "result": f"computed_{expression}",
            "node": self.node_id,
            "timestamp": time.time()
        }

    def _handle_filter(self, task: Task) -> List[Any]:
        """Handle filter task"""
        items = task.parameters.get("items", [])
        predicate = task.parameters.get("predicate", lambda x: True)

        # In production, evaluate predicate
        # For simulation, return all items
        return items

    def _handle_map(self, task: Task) -> List[Any]:
        """Handle map task"""
        items = task.parameters.get("items", [])
        mapper = task.parameters.get("mapper", lambda x: x)

        # In production, apply mapper
        # For simulation, return items unchanged
        return items


# ============================================================================
# Node Factory
# ============================================================================

class WorkerNodeFactory:
    """
    Factory for creating and managing worker nodes.
    """

    def __init__(self, hidden_root: HiddenRoot):
        """
        Initialize factory.

        Args:
            hidden_root: Reference to hidden root
        """
        self.hidden_root = hidden_root
        self.nodes: Dict[str, WorkerNode] = {}
        self.node_counter = 0
        self.lock = threading.RLock()

        logger.info("WorkerNodeFactory initialized")

    def create_node(self, region: str = "default") -> WorkerNode:
        """
        Create a new worker node.

        Args:
            region: Node region

        Returns:
            Created worker node
        """
        with self.lock:
            node_id = f"worker_{self.node_counter:05d}"
            address = f"127.0.0.1:{8000 + self.node_counter}"
            passphrase = f"pass_{self.node_counter}"

            node = WorkerNode(
                node_id=node_id,
                address=address,
                passphrase=passphrase,
                hidden_root=self.hidden_root,
                region=region
            )

            self.nodes[node_id] = node
            self.node_counter += 1

            logger.info(f"Created worker node {node_id} in region {region}")
            return node

    def initialize_all(self, count: int, region: str = "default") -> List[WorkerNode]:
        """
        Create and initialize multiple nodes.

        Args:
            count: Number of nodes to create
            region: Region for all nodes

        Returns:
            List of initialized nodes
        """
        nodes = []

        for i in range(count):
            node = self.create_node(region if i % 2 == 0 else "other")
            node.initialize()
            nodes.append(node)

            if (i + 1) % 10 == 0:
                logger.info(f"Initialized {i + 1}/{count} nodes")

        logger.info(f"Initialized {len(nodes)} worker nodes")
        return nodes

    def get_node(self, node_id: str) -> Optional[WorkerNode]:
        """Get node by ID"""
        return self.nodes.get(node_id)

    def get_all_nodes(self) -> List[WorkerNode]:
        """Get all nodes"""
        return list(self.nodes.values())

    def stop_all(self):
        """Stop all worker nodes"""
        for node in self.nodes.values():
            node.stop()
        logger.info(f"Stopped {len(self.nodes)} worker nodes")


# ============================================================================
# Main Entry Point for Part 5 Test
# ============================================================================

def test_part_5_components():
    print("=" * 60)
    print("DkTC - Part 5: Worker Node Test")
    print("=" * 60)

    # Create hidden root
    hidden_root = HiddenRoot(secret_key="test_secret_123", max_children=100)
    hidden_root.start()

    # Create worker node factory
    factory = WorkerNodeFactory(hidden_root)

    # Create and initialize worker nodes
    print("\n1. Creating and initializing worker nodes:")
    nodes = factory.initialize_all(count=10, region="us-east")
    print(f"   Created {len(nodes)} worker nodes")

    # Display node status
    print("\n2. Node status:")
    for node in nodes[:3]:  # Show first 3
        stats = node.get_stats()
        print(f"   {stats['node_id']}: {stats['status']}, children={stats['children_count']}")

    # Run leader election
    print("\n3. Running leader election:")
    first_node = nodes[0]
    leader_id = first_node.run_leader_election(rounds=3)
    print(f"   Election completed, leader: {leader_id}")

    # Test task submission
    print("\n4. Testing task submission:")
    test_node = nodes[0]
    task_id = test_node.submit_task(
        task_type="aggregate",
        parameters={"values": [1, 2, 3, 4, 5], "operation": "sum"},
        priority=1
    )
    print(f"   Submitted task: {task_id}")

    # Test direct task processing
    print("\n5. Testing task processing:")
    task = Task(
        task_id="direct_test_001",
        task_type="find_seed",
        parameters={"target": "test", "seed": 0, "range": 1000},
        priority=0
    )
    result = test_node.process_task(task)
    print(f"   Task result: {result}")

    # Test aggregation task
    print("\n6. Testing aggregation task:")
    agg_task = Task(
        task_id="agg_test_001",
        task_type="aggregate",
        parameters={"values": [10, 20, 30, 40, 50], "operation": "sum"},
        priority=0
    )
    agg_result = test_node.process_task(agg_task)
    print(f"   Aggregate result: {agg_result}")

    # Get detailed statistics
    print("\n7. Detailed node statistics:")
    for node in nodes[:2]:
        stats = node.get_stats()
        print(f"\n   Node {stats['node_id']}:")
        print(f"     Status: {stats['status']}")
        print(f"     Uptime: {stats['uptime']:.2f}s")
        print(f"     Children: {stats['children_count']}")
        print(f"     Tasks executed: {stats['stats']['tasks_executed']}")
        print(f"     Messages sent: {stats['stats']['messages_sent']}")
        print(f"     Heartbeats sent: {stats['stats']['heartbeats_sent']}")

    # Clean up
    print("\n8. Cleaning up:")
    factory.stop_all()
    hidden_root.stop()

    print("\n" + "=" * 60)
    print("Part 5: Worker Node test completed!")
    print("=" * 60)


"""
DkTC - Part 6: Complete System Integration

This module brings all components together into a cohesive distributed computing system:
- System orchestrator for managing the entire lifecycle
- Configuration management
- Deployment coordination
- Monitoring and observability
- Fault recovery and auto-scaling
- Job scheduling and management

Dependencies: Part 1-5 (all previous components)
"""

from typing import Dict, List, Optional, Set, Tuple, Any, Callable
import threading
import json
import signal
import sys
from dataclasses import dataclass
from datetime import datetime


# ============================================================================
# System Configuration
# ============================================================================

@dataclass
class DkTCConfig:
    """Complete system configuration for DkTC"""

    # Node configuration
    num_workers: int = 100
    k: int = None  # Auto-calculated if None
    regions: List[str] = field(default_factory=lambda: ["us-east", "us-west", "eu-central", "asia-east"])

    # Hidden root configuration
    hidden_root_secret: str = "dktc_secret_key_2024"
    hidden_root_port: int = 8000
    hidden_root_replicas: int = 3

    # Network configuration
    max_connections_per_node: int = 10000
    local_connections_ratio: float = 0.8
    gossip_fanout: int = 3

    # Leader election
    election_rounds: int = 10
    election_timeout: float = 5.0

    # Tree configuration
    max_children_per_node: int = 20
    tree_building_timeout: float = 30.0

    # Task configuration
    task_timeout: float = 300.0
    max_retries: int = 3
    partition_size: int = 1000

    # Fault tolerance
    heartbeat_interval: float = 1.0
    failure_timeout: float = 5.0
    auto_rebalance: bool = True

    # Monitoring
    enable_metrics: bool = True
    metrics_interval: float = 10.0
    log_level: str = "INFO"

    def __post_init__(self):
        """Auto-calculate k if not provided"""
        if self.k is None:
            self.k = calculate_optimal_k(self.num_workers)

        # Apply constants
        DkTCConstants.DEFAULT_K = self.k
        DkTCConstants.MAX_CONNECTIONS_PER_NODE = self.max_connections_per_node
        DkTCConstants.LEADER_ELECTION_ROUNDS = self.election_rounds
        DkTCConstants.HEARTBEAT_INTERVAL = self.heartbeat_interval
        DkTCConstants.FAILURE_DETECTION_TIMEOUT = self.failure_timeout

    @classmethod
    def from_file(cls, filepath: str) -> 'DkTCConfig':
        """Load configuration from JSON or YAML file"""
        with open(filepath, 'r') as f:
            if filepath.endswith('.yaml') or filepath.endswith('.yml'):
                import yaml  # optional dependency, only needed for YAML configs
                data = yaml.safe_load(f)
            else:
                data = json.load(f)
        return cls(**data)

    def to_file(self, filepath: str):
        """Save configuration to file"""
        data = {
            "num_workers": self.num_workers,
            "k": self.k,
            "regions": self.regions,
            "hidden_root_secret": self.hidden_root_secret,
            "hidden_root_port": self.hidden_root_port,
            "hidden_root_replicas": self.hidden_root_replicas,
            "max_connections_per_node": self.max_connections_per_node,
            "local_connections_ratio": self.local_connections_ratio,
            "election_rounds": self.election_rounds,
            "max_children_per_node": self.max_children_per_node,
            "task_timeout": self.task_timeout,
            "heartbeat_interval": self.heartbeat_interval,
            "failure_timeout": self.failure_timeout,
            "enable_metrics": self.enable_metrics
        }

        with open(filepath, 'w') as f:
            if filepath.endswith('.yaml') or filepath.endswith('.yml'):
                import yaml  # optional dependency
                yaml.dump(data, f)
            else:
                json.dump(data, f, indent=2)


# ============================================================================
# System Metrics Collector
# ============================================================================

class SystemMetrics:
    """Collects and aggregates system-wide metrics"""

    def __init__(self, config: DkTCConfig):
        self.config = config
        self.metrics: Dict[str, Any] = {
            "system": {
                "start_time": time.time(),
                "total_tasks": 0,
                "completed_tasks": 0,
                "failed_tasks": 0,
                "avg_task_latency": 0.0
            },
            "network": {
                "total_nodes": 0,
                "active_nodes": 0,
                "total_connections": 0,
                "avg_degree": 0.0,
                "diameter": 0,
                "root_changes": 0
            },
            "workers": {},
            "timeline": []
        }
        self.lock = threading.RLock()
        self.running = False
        self.collector_thread = None

    def start(self):
        """Start metrics collection"""
        self.running = True
        self.collector_thread = threading.Thread(target=self._collect_loop, daemon=True)
        self.collector_thread.start()
        logger.info("System metrics collection started")

    def stop(self):
        """Stop metrics collection"""
        self.running = False
        if self.collector_thread:
            self.collector_thread.join(timeout=5)

    def _collect_loop(self):
        """Background metrics collection"""
        while self.running:
            time.sleep(self.config.metrics_interval)
            self._record_snapshot()

    def _record_snapshot(self):
        """Record a metrics snapshot"""
        with self.lock:
            snapshot = {
                "timestamp": time.time(),
                "metrics": {
                    "total_tasks": self.metrics["system"]["total_tasks"],
                    "completed_tasks": self.metrics["system"]["completed_tasks"],
                    "active_nodes": self.metrics["network"]["active_nodes"],
                    "total_connections": self.metrics["network"]["total_connections"]
                }
            }
            self.metrics["timeline"].append(snapshot)

            # Keep last 1000 snapshots
            if len(self.metrics["timeline"]) > 1000:
                self.metrics["timeline"] = self.metrics["timeline"][-1000:]

    def update_node_metrics(self, node_id: str, stats: Dict[str, Any]):
        """Update metrics for a specific node"""
        with self.lock:
            self.metrics["workers"][node_id] = stats

    def update_system_metrics(self, **kwargs):
        """Update system-level metrics"""
        with self.lock:
            for key, value in kwargs.items():
                if key in self.metrics["system"]:
                    self.metrics["system"][key] = value
                elif key in self.metrics["network"]:
                    self.metrics["network"][key] = value

    def record_task_completion(self, task_id: str, latency: float, success: bool = True):
        """Record task completion metrics"""
        with self.lock:
            self.metrics["system"]["total_tasks"] += 1
            if success:
                self.metrics["system"]["completed_tasks"] += 1
            else:
                self.metrics["system"]["failed_tasks"] += 1

            # Update rolling average latency
            total = self.metrics["system"]["total_tasks"]
            current_avg = self.metrics["system"]["avg_task_latency"]
            self.metrics["system"]["avg_task_latency"] = (
                    (current_avg * (total - 1) + latency) / total
            )

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        with self.lock:
            # Calculate derived metrics
            active = self.metrics["network"]["active_nodes"]
            connections = self.metrics["network"]["total_connections"]

            return {
                "system": self.metrics["system"].copy(),
                "network": {
                    **self.metrics["network"].copy(),
                    "avg_degree": connections / active if active > 0 else 0
                },
                "workers_count": len(self.metrics["workers"]),
                "uptime": time.time() - self.metrics["system"]["start_time"]
            }

    def print_summary(self):
        """Print a summary of current metrics"""
        metrics = self.get_metrics()

        print("\n" + "=" * 60)
        print("DKTC SYSTEM METRICS SUMMARY")
        print("=" * 60)
        print(f"Uptime: {metrics['uptime']:.2f}s")
        print(f"Total Nodes: {metrics['network']['total_nodes']}")
        print(f"Active Nodes: {metrics['network']['active_nodes']}")
        print(f"Total Connections: {metrics['network']['total_connections']}")
        print(f"Avg Degree: {metrics['network']['avg_degree']:.2f}")
        print(f"Diameter: {metrics['network']['diameter']}")
        print(f"Root Changes: {metrics['network']['root_changes']}")
        print(f"Total Tasks: {metrics['system']['total_tasks']}")
        print(f"Completed Tasks: {metrics['system']['completed_tasks']}")
        print(f"Failed Tasks: {metrics['system']['failed_tasks']}")
        print(f"Avg Task Latency: {metrics['system']['avg_task_latency']:.3f}s")
        print("=" * 60)


# ============================================================================
# Job Scheduler
# ============================================================================

class JobScheduler:
    """Schedules and manages jobs across the distributed system"""

    def __init__(self, system: 'DkTCSystem'):
        self.system = system
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.scheduled_jobs: PriorityQueue = PriorityQueue()
        self.running = False
        self.scheduler_thread = None
        self.lock = threading.RLock()

    def start(self):
        """Start the job scheduler"""
        self.running = True
        self.scheduler_thread = threading.Thread(target=self._schedule_loop, daemon=True)
        self.scheduler_thread.start()
        logger.info("Job scheduler started")

    def stop(self):
        """Stop the job scheduler"""
        self.running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)

    def submit_job(self, job_type: str, parameters: Dict[str, Any],
                   priority: int = 0, schedule_time: float = None) -> str:
        """
        Submit a job for execution.

        Args:
            job_type: Type of job
            parameters: Job parameters
            priority: Priority (lower = higher)
            schedule_time: Unix timestamp for scheduled execution

        Returns:
            Job ID
        """
        job_id = str(uuid.uuid4())

        job = {
            "job_id": job_id,
            "job_type": job_type,
            "parameters": parameters,
            "priority": priority,
            "schedule_time": schedule_time or time.time(),
            "submitted_at": time.time(),
            "status": "scheduled"
        }

        with self.lock:
            self.jobs[job_id] = job
            self.scheduled_jobs.push(job, priority)

        logger.info(f"Job {job_id} submitted (type={job_type}, priority={priority})")
        return job_id

    def _schedule_loop(self):
        """Main scheduling loop"""
        while self.running:
            try:
                job = self.scheduled_jobs.pop()
                if job:
                    if job["schedule_time"] <= time.time():
                        self._execute_job(job)
                    else:
                        # Re-queue for later
                        self.scheduled_jobs.push(job, job["priority"])
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")

    def _execute_job(self, job: Dict[str, Any]):
        """Execute a job"""
        job_id = job["job_id"]

        with self.lock:
            self.jobs[job_id]["status"] = "running"
            self.jobs[job_id]["started_at"] = time.time()

        try:
            # Submit task to system
            task_id = self.system.submit_task(
                task_type=job["job_type"],
                parameters=job["parameters"],
                priority=job["priority"]
            )

            # Wait for result (async in production)
            result = self.system.get_task_result(task_id, timeout=300)

            with self.lock:
                self.jobs[job_id]["status"] = "completed"
                self.jobs[job_id]["completed_at"] = time.time()
                self.jobs[job_id]["result"] = result
                self.jobs[job_id]["task_id"] = task_id

            logger.info(f"Job {job_id} completed successfully")

        except Exception as e:
            with self.lock:
                self.jobs[job_id]["status"] = "failed"
                self.jobs[job_id]["error"] = str(e)

            logger.error(f"Job {job_id} failed: {e}")

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job status"""
        with self.lock:
            job = self.jobs.get(job_id)
            if job:
                return {
                    "job_id": job["job_id"],
                    "status": job["status"],
                    "submitted_at": job["submitted_at"],
                    "started_at": job.get("started_at"),
                    "completed_at": job.get("completed_at")
                }
        return None

    def get_job_result(self, job_id: str) -> Optional[Any]:
        """Get job result"""
        with self.lock:
            job = self.jobs.get(job_id)
            if job and job["status"] == "completed":
                return job.get("result")
        return None


# ============================================================================
# DkTC System - Main Orchestrator
# ============================================================================

class DkTCSystem:
    """
    Main DkTC System Orchestrator.

    Brings together all components and provides a unified interface for:
    - System initialization and deployment
    - Node management
    - Task submission and monitoring
    - Fault recovery
    - Metrics and observability
    """

    def __init__(self, config: DkTCConfig = None):
        """
        Initialize the DkTC system.

        Args:
            config: System configuration (default if None)
        """
        self.config = config or DkTCConfig()

        # Initialize components
        self.hidden_root = HiddenRoot(
            secret_key=self.config.hidden_root_secret,
            port=self.config.hidden_root_port,
            max_children=self.config.max_children_per_node
        )

        self.worker_factory = WorkerNodeFactory(self.hidden_root)
        self.dcg_network = DCGNetwork(self.hidden_root, k=self.config.k)
        self.tree_network = None  # Created after root election

        self.metrics = SystemMetrics(self.config)
        self.job_scheduler = JobScheduler(self)

        # System state
        self.workers: Dict[str, WorkerNode] = {}
        self.root_id: Optional[str] = None
        self.running = False

        # Threading
        self.lock = threading.RLock()
        self.recovery_thread = None

        # Signal handlers
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        except ValueError:
            pass  # not in the main thread; skip signal handlers

        logger.info("DkTC System initialized")
        logger.info(f"Configuration: {self.config.num_workers} workers, k={self.config.k}")

    # ========================================================================
    # System Lifecycle
    # ========================================================================

    def start(self):
        """Start the DkTC system"""
        logger.info("Starting DkTC system...")

        # Start hidden root
        self.hidden_root.start()

        # Start metrics collection
        if self.config.enable_metrics:
            self.metrics.start()

        # Start job scheduler
        self.job_scheduler.start()

        # Create and initialize workers
        self._deploy_workers()

        # Run leader election
        self._elect_root()

        # Build tree
        self._build_tree()

        # Let the hidden root actually execute submitted tasks on the tree
        self.hidden_root.set_task_dispatcher(self._dispatch_task)

        # Start recovery thread
        self.running = True
        self.recovery_thread = threading.Thread(target=self._recovery_loop, daemon=True)
        self.recovery_thread.start()

        logger.info("DkTC system started successfully")
        self._print_system_info()

    def stop(self):
        """Stop the DkTC system gracefully"""
        logger.info("Stopping DkTC system...")
        self.running = False

        # Stop components
        self.job_scheduler.stop()
        self.metrics.stop()

        # Stop workers
        self.worker_factory.stop_all()

        # Stop hidden root
        self.hidden_root.stop()

        logger.info("DkTC system stopped")

    def _deploy_workers(self):
        """Deploy and initialize worker nodes with post-deployment connection setup."""
        logger.info(f"Deploying {self.config.num_workers} worker nodes...")

        # First, create all nodes without connections
        nodes_per_region, remainder = divmod(self.config.num_workers, len(self.config.regions))
        all_worker_ids = []

        for r_idx, region in enumerate(self.config.regions):
            count = nodes_per_region + (1 if r_idx < remainder else 0)
            for i in range(count):
                node_id = f"worker_{len(all_worker_ids):05d}"
                address = f"127.0.0.1:{8000 + len(all_worker_ids)}"
                passphrase = f"pass_{len(all_worker_ids)}"

                # Register with hidden root (just creates entry, no connections yet)
                registration = self.hidden_root.register_node(
                    node_id=node_id,
                    address=address,
                    passphrase=passphrase,
                    region=region
                )

                # Create worker node
                worker = WorkerNode(
                    node_id=node_id,
                    address=address,
                    passphrase=passphrase,
                    hidden_root=self.hidden_root,
                    region=region
                )

                # Store certificate in worker
                worker.certificate = registration["certificate"]

                # Create DCG node and ATTACH to worker
                dcg_node = self.dcg_network.create_node(node_id)
                worker.dcg_node = dcg_node  # CRITICAL FIX

                # Store references
                self.workers[node_id] = worker
                all_worker_ids.append(node_id)

                logger.debug(f"Created node {node_id} in region {region}")

        logger.info(f"Created {len(self.workers)} worker nodes")

        # SECOND PHASE: Establish connections between all nodes
        logger.info("Establishing bidirectional connections...")

        # Each node connects to 5k others (4k local + 1k global)
        for node_id, worker in self.workers.items():
            node_info = self.hidden_root.get_node_info(node_id)
            neighbors = self.hidden_root._sample_bootstrap_neighbors(node_id, node_info.region)

            # Establish connections through DCG network (handles bidirectionality)
            for neighbor_id, address in neighbors:
                if neighbor_id in self.dcg_network.nodes:
                    # Measure bandwidth
                    bandwidth = random.uniform(10, 1000)  # Simulated

                    # Add forward connection on this node
                    dcg_node = self.dcg_network.nodes[node_id]
                    with dcg_node.connection_lock:
                        if neighbor_id not in dcg_node.connections:
                            dcg_node.connections[neighbor_id] = Connection(
                                node_id=neighbor_id,
                                bandwidth=bandwidth,
                                last_updated=time.time()
                            )
                            dcg_node.stats["connections_established"] += 1

                    # Add reverse connection on neighbor
                    neighbor_dcg = self.dcg_network.nodes.get(neighbor_id)
                    if neighbor_dcg:
                        with neighbor_dcg.connection_lock:
                            if node_id not in neighbor_dcg.connections:
                                neighbor_dcg.connections[node_id] = Connection(
                                    node_id=node_id,
                                    bandwidth=bandwidth,
                                    last_updated=time.time()
                                )
                                neighbor_dcg.stats["connections_established"] += 1

            # Start the worker after connections are established
            worker.start()

        logger.info(f"Deployed {len(self.workers)} workers with bidirectional connections")

        # Update metrics
        self.metrics.update_system_metrics(
            total_nodes=len(self.workers),
            active_nodes=len(self.workers),
            total_connections=sum(n.get_connection_count() for n in self.dcg_network.nodes.values())
        )

    def _elect_root(self):
        """Run leader election to select system root"""
        logger.info("Running leader election...")

        # Run election from first worker
        first_worker = list(self.workers.values())[0]
        self.root_id = first_worker.run_leader_election(rounds=self.config.election_rounds)

        # CRITICAL: Update hidden root with the elected root
        if self.root_id:
            worker = self.workers.get(self.root_id)
            if worker:
                worker.is_root = True
                if worker.certificate:
                    self.hidden_root.update_root(self.root_id, worker.certificate)

        logger.info(f"Leader election completed. Root: {self.root_id}")

        # Update metrics
        self.metrics.update_system_metrics(root_changes=1)

    def _build_tree(self):
        """Build the k-ary tree from root and distribute assignments."""
        if not self.root_id:
            logger.error("Cannot build tree: no root elected")
            return

        logger.info(f"Building tree with root {self.root_id}...")

        # Create tree network
        self.tree_network = TreeNetwork(
            hidden_root=self.hidden_root,
            dcg_network=self.dcg_network,
            k=self.config.k
        )

        # Create tree nodes for all workers
        for worker_id in self.workers:
            self.tree_network.create_node(worker_id)

        # CRITICAL: Set the root in tree_network before building
        self.tree_network.root_id = self.root_id

        # Mark the root worker
        root_worker = self.workers.get(self.root_id)
        if root_worker and root_worker.tree_node:
            root_worker.tree_node.is_root = True
            root_worker.tree_node.tree_level = 0
            root_worker.is_root = True

        # Get the root node
        root_node = self.tree_network.get_root()
        if root_node is None:
            logger.error(f"Root node {self.root_id} not found in tree network")
            return

        # Get bandwidth function
        def get_bandwidth(node1: str, node2: str) -> float:
            dcg_node1 = self.dcg_network.nodes.get(node1)
            if dcg_node1 and node2 in dcg_node1.connections:
                return dcg_node1.connections[node2].bandwidth
            return 50.0

        # Get all node IDs
        all_nodes = [info.node_id for info in self.hidden_root.get_all_nodes()]

        # Build tree with assignments
        tree_structure, assignments = root_node.build_tree_with_assignments(
            all_nodes, get_bandwidth
        )

        # Store tree structure and wire it into every TreeNode so tasks can recurse down the tree
        self.tree_network.tree_structure = tree_structure
        for node_id, children in tree_structure.items():
            tnode = self.tree_network.nodes.get(node_id)
            if tnode is None:
                continue
            tnode.children_ids = list(children)
            tnode.children_bandwidth = {c: assignments[c]["bandwidth"] for c in children if c in assignments}
            if node_id in assignments:
                tnode.parent_id = assignments[node_id]["parent"]
                tnode.tree_level = assignments[node_id]["level"]

        # Distribute assignments to children (in production, send RPCs)
        for child_id, assignment in assignments.items():
            worker = self.workers.get(child_id)
            if worker:
                worker.parent_id = assignment["parent"]
                if worker.tree_node:
                    worker.tree_node.parent_id = assignment["parent"]
                    worker.tree_node.tree_level = assignment["level"]

                # Add to parent's children list
                parent_worker = self.workers.get(assignment["parent"])
                if parent_worker:
                    parent_worker.children_ids.append(child_id)
                    parent_worker.children_bandwidth[child_id] = assignment["bandwidth"]

                logger.debug(f"Assigned {child_id} as child of {assignment['parent']} at level {assignment['level']}")

        # Update hidden root
        for node_id, children in tree_structure.items():
            node_info = self.hidden_root.get_node_info(node_id)
            if node_info:
                node_info.children_ids = children
                if node_id == self.root_id:
                    node_info.parent_id = None
                    node_info.status = NodeStatus.ROOT

        # Calculate approximate depth
        depth = max([a["level"] for a in assignments.values()], default=0) + 1
        self.metrics.update_system_metrics(diameter=depth)

        logger.info(f"Tree built with root {self.root_id}, depth={depth}, {len(tree_structure)} nodes in tree")

    def _get_bandwidth_function(self):
        """Helper to get bandwidth between nodes"""

        def get_bandwidth(node1: str, node2: str) -> float:
            dcg_node = self.dcg_network.nodes.get(node1)
            if dcg_node and node2 in dcg_node.connections:
                return dcg_node.connections[node2].bandwidth
            return 50.0  # Default fallback

        return get_bandwidth

    def _calculate_tree_depth(self) -> int:
        """Calculate maximum depth of the tree"""
        max_depth = 0
        for worker in self.workers.values():
            if worker.tree_node and worker.tree_node.tree_level > max_depth:
                max_depth = worker.tree_node.tree_level
        return max_depth + 1

    def _recovery_loop(self):
        """Background loop for fault recovery"""
        while self.running:
            time.sleep(self.config.heartbeat_interval * 5)

            # Check for stale nodes
            stale_nodes = self.hidden_root.get_stale_nodes()

            if stale_nodes:
                logger.warning(f"Detected {len(stale_nodes)} stale nodes")

                for node_id in stale_nodes:
                    self._handle_node_failure(node_id)

            # Update metrics
            active_nodes = self.hidden_root.get_active_node_count()
            self.metrics.update_system_metrics(active_nodes=active_nodes)

            # Check if root is still alive
            if self.root_id and self.root_id in stale_nodes:
                logger.error(f"Root {self.root_id} failed! Triggering re-election")
                self._recover_root()

    def _handle_node_failure(self, node_id: str):
        """Handle failure of a worker node"""
        logger.warning(f"Handling failure of node {node_id}")

        # Remove from system
        if node_id in self.workers:
            worker = self.workers.pop(node_id)
            worker.stop()

        # Update parent's children
        node_info = self.hidden_root.get_node_info(node_id)
        if node_info and node_info.parent_id:
            parent = self.workers.get(node_info.parent_id)
            if parent:
                parent.remove_child(node_id)

        # Update metrics
        self.metrics.update_system_metrics(
            total_nodes=len(self.workers),
            active_nodes=self.hidden_root.get_active_node_count()
        )

    def _recover_root(self):
        """Recover from root failure"""
        # Reset root
        self.root_id = None

        # Force new leader election
        active_workers = [w for w in self.workers.values() if w.status == WorkerStatus.ACTIVE]

        if active_workers:
            # Run election on first active worker
            self.root_id = active_workers[0].run_leader_election()
            self._build_tree()
            logger.info(f"Root recovered: {self.root_id}")
        else:
            logger.critical("No active workers available for root election!")

    # ========================================================================
    # Task Management
    # ========================================================================

    def submit_task(self, task_type: str, parameters: Dict[str, Any],
                    priority: int = 0, async_mode: bool = False) -> str:
        """
        Submit a task for execution.

        Args:
            task_type: Type of task
            parameters: Task parameters
            priority: Task priority
            async_mode: If True, return immediately; if False, wait for result

        Returns:
            Task ID (or result if async_mode=False)
        """
        start_time = time.time()

        # Submit to hidden root (forwards to system root)
        task_id = self.hidden_root.submit_task(task_type, parameters, priority)

        if async_mode:
            return task_id

        # Wait for result (simplified - in production, implement polling)
        timeout = parameters.get("timeout", 300)
        result = self._wait_for_task(task_id, timeout)

        latency = time.time() - start_time
        self.metrics.record_task_completion(task_id, latency, success=(result is not None))

        return result

    def _dispatch_task(self, task_id: str, task_type: str,
                       parameters: Dict[str, Any], priority: int):
        """Hidden-root dispatcher: run the task on the tree in a background thread."""
        def run():
            try:
                if not self.tree_network or not self.tree_network.get_root():
                    raise RuntimeError("tree not ready")
                task = Task(task_id=task_id, task_type=task_type,
                            parameters=parameters, priority=priority)
                result = self.tree_network.distribute_task(task)
                self.hidden_root.update_task_result(task_id, result)
            except Exception as e:
                logger.error(f"Task {task_id} failed: {e}")
                self.hidden_root.fail_task(task_id, str(e))

        threading.Thread(target=run, daemon=True, name=f"task-{task_id[:8]}").start()

    def _wait_for_task(self, task_id: str, timeout: float) -> Optional[Any]:
        """Wait for task completion (polls task status, so a None result still counts)."""
        start = time.time()

        while time.time() - start < timeout:
            status = self.hidden_root.get_task_status(task_id)
            if status == TaskStatus.COMPLETED.value:
                return self.hidden_root.get_task_result(task_id)
            if status == TaskStatus.FAILED.value:
                return None
            time.sleep(0.05)

        logger.warning(f"Task {task_id} timed out after {timeout}s")
        return None

    def get_task_result(self, task_id: str, timeout: float = 300) -> Optional[Any]:
        """Get task result"""
        return self._wait_for_task(task_id, timeout)

    # ========================================================================
    # Job Management
    # ========================================================================

    def submit_job(self, job_type: str, parameters: Dict[str, Any],
                   priority: int = 0, schedule_time: float = None) -> str:
        """
        Submit a job to the scheduler.

        Args:
            job_type: Type of job
            parameters: Job parameters
            priority: Priority (lower = higher)
            schedule_time: Scheduled execution time

        Returns:
            Job ID
        """
        return self.job_scheduler.submit_job(job_type, parameters, priority, schedule_time)

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job status"""
        return self.job_scheduler.get_job_status(job_id)

    def get_job_result(self, job_id: str) -> Optional[Any]:
        """Get job result"""
        return self.job_scheduler.get_job_result(job_id)

    # ========================================================================
    # Monitoring and Diagnostics
    # ========================================================================

    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        return {
            "system": {
                "running": self.running,
                "uptime": time.time() - getattr(self, "_start_time", time.time()),
                "root_id": self.root_id
            },
            "metrics": self.metrics.get_metrics(),
            "workers": {
                "total": len(self.workers),
                "active": self.hidden_root.get_active_node_count(),
                "by_status": self._count_workers_by_status()
            },
            "hidden_root": self.hidden_root.get_stats(),
            "tree": self.tree_network.get_stats() if self.tree_network else None
        }

    def _count_workers_by_status(self) -> Dict[str, int]:
        """Count workers by status"""
        counts = {}
        for worker in self.workers.values():
            status = worker.status.value
            counts[status] = counts.get(status, 0) + 1
        return counts

    def _print_system_info(self):
        """Print system information"""
        print("\n" + "=" * 60)
        print("DKTC SYSTEM INFORMATION")
        print("=" * 60)
        print(f"Total Workers: {len(self.workers)}")
        print(f"Root Node: {self.root_id}")
        print(f"k (tree degree): {self.config.k}")
        print(f"Expected Diameter: {estimate_network_diameter(len(self.workers), self.config.k):.2f}")
        print(f"Expected s (Δ+D): {estimate_s_value(len(self.workers), self.config.k):.2f}")
        print(f"Hidden Root Port: {self.config.hidden_root_port}")
        print(f"Regions: {', '.join(self.config.regions)}")
        print("=" * 60)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)

    # ========================================================================
    # Example Workloads
    # ========================================================================

    def run_example_workload(self):
        """Run example workloads to demonstrate system capabilities"""
        print("\n" + "=" * 60)
        print("RUNNING EXAMPLE WORKLOADS")
        print("=" * 60)

        # Workload 1: Parallel aggregation
        print("\n1. Running parallel aggregation across all nodes...")
        agg_params = {
            "operation": "sum",
            "values": list(range(1000)),
            "partition_size": 100
        }

        agg_result = self.submit_task("aggregate", agg_params, async_mode=False)
        print(f"   Aggregation result: {agg_result}")

        # Workload 2: Distributed seed search
        print("\n2. Running distributed seed search...")
        seed_params = {
            "target": "dktc_demo",
            "seed_range": 100000,
            "target_hash": "0000",
            "partitions": 10
        }

        seed_result = self.submit_task("find_seed", seed_params, async_mode=False)
        print(f"   Seed search result: {seed_result}")

        # Workload 3: Multiple concurrent tasks
        print("\n3. Running multiple concurrent tasks...")
        import concurrent.futures

        def run_task(i):
            params = {"value": i, "operation": "square"}
            return self.submit_task("compute", params, async_mode=False)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(run_task, i) for i in range(10)]
            results = [f.result() for f in futures]

        print(f"   Completed {len(results)} concurrent tasks")

        # Print final metrics
        print("\n" + "=" * 60)
        print("WORKLOAD COMPLETED - FINAL METRICS")
        print("=" * 60)
        self.metrics.print_summary()


# ============================================================================
# Main Entry Point
# ============================================================================

def run_selftests():
    """Run the per-part smoke tests (quiet logging)."""
    logging.getLogger("DkTC").setLevel(logging.WARNING)
    failures = 0
    for i in range(1, 6):
        try:
            globals()[f"test_part_{i}_components"]()
        except Exception as e:
            failures += 1
            print(f"!! part {i} FAILED: {type(e).__name__}: {e}")
    print(f"\nSelf-tests finished: {5 - failures}/5 parts passed")
    return failures


def main():
    """Main entry point for DkTC system"""
    import argparse
    parser = argparse.ArgumentParser(description="DkTC - Distributed k-Tree Computing (simulation)")
    parser.add_argument("--workers", type=int, default=50, help="number of simulated workers")
    parser.add_argument("--k", type=int, default=5, help="tree degree")
    parser.add_argument("--once", action="store_true", help="run the demo workloads then exit")
    parser.add_argument("--selftest", action="store_true", help="run per-part self-tests and exit")
    args = parser.parse_args()

    if args.selftest:
        sys.exit(1 if run_selftests() else 0)

    print("=" * 60)
    print("DKTC - Distributed k-Tree Computing System")
    print("=" * 60)

    config = DkTCConfig(
        num_workers=args.workers,
        k=args.k,
        enable_metrics=True,
        heartbeat_interval=2.0
    )

    system = DkTCSystem(config)
    system._start_time = time.time()

    try:
        system.start()
        system.run_example_workload()

        if args.once:
            return

        print("\n" + "=" * 60)
        print("DKTC SYSTEM IS RUNNING - press Ctrl+C to shut down")
        print("=" * 60)

        last_status = 0.0
        while system.running:
            time.sleep(1)
            if time.time() - last_status >= 30:
                status = system.get_system_status()
                print(f"\n[STATUS] Active nodes: {status['metrics']['network']['active_nodes']}, "
                      f"Total tasks: {status['metrics']['system']['total_tasks']}")
                last_status = time.time()

    except KeyboardInterrupt:
        print("\n\nShutting down DkTC system...")
    finally:
        system.stop()
        print("DkTC system shutdown complete")


if __name__ == "__main__":
    main()
