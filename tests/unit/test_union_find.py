"""
Tests for Union-Find cluster assignment in pub_sync.
"""

import unittest
from services.publisher.pub_sync import UnionFind, build_clusters


class TestUnionFind(unittest.TestCase):
    """UnionFind data structure tests."""

    def test_single_pair(self):
        uf = UnionFind()
        uf.union(1, 2)
        self.assertEqual(uf.find(1), uf.find(2))

    def test_transitive_chain(self):
        """A↔B, B↔C → A, B, C in same cluster."""
        uf = UnionFind()
        uf.union(1, 2)
        uf.union(2, 3)
        self.assertEqual(uf.find(1), uf.find(3))

    def test_disconnected_clusters(self):
        """Two separate groups stay separate."""
        uf = UnionFind()
        uf.union(1, 2)
        uf.union(3, 4)
        self.assertNotEqual(uf.find(1), uf.find(3))

    def test_no_unions(self):
        uf = UnionFind()
        # Each element is its own root
        self.assertEqual(uf.find(10), 10)
        self.assertEqual(uf.find(20), 20)
        self.assertNotEqual(uf.find(10), uf.find(20))

    def test_duplicate_union(self):
        """Union called twice on same pair — no error."""
        uf = UnionFind()
        uf.union(1, 2)
        uf.union(1, 2)
        self.assertEqual(uf.find(1), uf.find(2))

    def test_large_chain(self):
        """Chain of 100 elements → all same cluster."""
        uf = UnionFind()
        for i in range(99):
            uf.union(i, i + 1)
        root = uf.find(0)
        for i in range(100):
            self.assertEqual(uf.find(i), root)


class TestBuildClusters(unittest.TestCase):
    """build_clusters() function tests."""

    def test_simple_pairs(self):
        pairs = [(10, 20), (30, 40)]
        clusters = build_clusters(pairs)
        # Two clusters
        self.assertEqual(clusters[10], clusters[20])
        self.assertEqual(clusters[30], clusters[40])
        self.assertNotEqual(clusters[10], clusters[30])

    def test_transitive(self):
        pairs = [(10, 20), (20, 30)]
        clusters = build_clusters(pairs)
        self.assertEqual(clusters[10], clusters[20])
        self.assertEqual(clusters[20], clusters[30])

    def test_empty_pairs(self):
        clusters = build_clusters([])
        self.assertEqual(clusters, {})

    def test_cluster_ids_are_ints(self):
        pairs = [(1, 2)]
        clusters = build_clusters(pairs)
        for v in clusters.values():
            self.assertIsInstance(v, int)


if __name__ == "__main__":
    unittest.main()
