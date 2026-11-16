"""Deterministic partitioning and consumer-group assignment.

Partition choice is a stable SHA-256 hash of the record key modulo the
partition count, so the same key always lands in the same partition and
records sharing a key are consumed in offset order. Assignment of partitions
to group members is a deterministic round-robin over sorted member ids, so a
rebalance is reproducible and testable.
"""

from __future__ import annotations

import hashlib


def partition_for_key(key: str | None, num_partitions: int) -> int:
    """Stable partition for a key (``None`` keys hash as the empty string)."""
    if num_partitions < 1:
        raise ValueError("num_partitions must be >= 1")
    digest = hashlib.sha256((key or "").encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % num_partitions


def assign_partitions(
    member_ids: list[str], partition_ids: list[int]
) -> dict[str, list[int]]:
    """Round-robin assignment of partitions to members (both sorted first).

    Returns ``{member_id: [partition, ...]}``; every partition is assigned to
    exactly one member, and members with no partitions get an empty list.
    """
    members = sorted(member_ids)
    partitions = sorted(partition_ids)
    assignment: dict[str, list[int]] = {m: [] for m in members}
    if not members:
        return assignment
    for index, partition in enumerate(partitions):
        assignment[members[index % len(members)]].append(partition)
    return assignment
