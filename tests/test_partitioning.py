from eventstream.partitioning import assign_partitions, partition_for_key


def test_same_key_same_partition():
    assert partition_for_key("ORD-000001", 4) == partition_for_key("ORD-000001", 4)


def test_none_key_is_deterministic():
    assert partition_for_key(None, 4) == partition_for_key(None, 4)
    assert 0 <= partition_for_key(None, 4) < 4


def test_keys_spread_across_all_partitions():
    seen = {partition_for_key(f"key-{i}", 4) for i in range(200)}
    assert seen == {0, 1, 2, 3}


def test_partition_in_range():
    for i in range(50):
        assert 0 <= partition_for_key(f"k-{i}", 7) < 7


def test_round_robin_assignment_covers_all_partitions():
    assignment = assign_partitions(["m1", "m2", "m3"], list(range(8)))
    assert assignment == {"m1": [0, 3, 6], "m2": [1, 4, 7], "m3": [2, 5]}


def test_assignment_is_disjoint_and_complete():
    assignment = assign_partitions(["a", "b"], list(range(5)))
    flat = [p for parts in assignment.values() for p in parts]
    assert sorted(flat) == [0, 1, 2, 3, 4]


def test_more_members_than_partitions_get_empty_lists():
    assignment = assign_partitions(["a", "b", "c"], [0, 1])
    assert assignment == {"a": [0], "b": [1], "c": []}


def test_assignment_is_sorted_and_deterministic():
    first = assign_partitions(["z", "a", "m"], list(range(6)))
    second = assign_partitions(["m", "z", "a"], list(range(6)))
    assert first == second
