from shepherd_utils.arax.xcrg import utilities


def test_partition():
    even, odd = utilities.partition([1, 2, 3, 4, 5], lambda x: x % 2 == 0)
    assert even == [2, 4]
    assert odd == [1, 3, 5]
