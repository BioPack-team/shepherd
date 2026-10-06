"""The slot-decomposed first layer must match the full one (workers/score_paths).

score_paths no longer multiplies the 8448-wide concatenated embeddings by the
first-layer weight; it sums 11 cached per-slot projections instead. These pin
that the two agree, including across cache growth and eviction. The worker
itself needs torch and lmdb, so this exercises the numpy-only projector with a
numpy matmul standing in for the worker's torch one.
"""

import os
import sys

import numpy as np
import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "..", "workers", "score_paths")
)

from slot_projection import EMBEDDING_DIM, NUM_SLOTS, SlotProjector  # noqa: E402

HIDDEN = 32


@pytest.fixture
def layer():
    rng = np.random.default_rng(0)
    weight = rng.standard_normal((HIDDEN, NUM_SLOTS * EMBEDDING_DIM)).astype(np.float32)
    bias = rng.standard_normal(HIDDEN).astype(np.float32)
    return weight, bias


@pytest.fixture
def embeddings():
    rng = np.random.default_rng(1)
    vocab = [f"key{i}" for i in range(40)]
    # float16, as the LMDB cache stores them.
    return {k: rng.standard_normal(EMBEDDING_DIM).astype(np.float16) for k in vocab}


def make_projector(layer, max_cached_rows=10_000, calls=None):
    weight, bias = layer

    def project(slot, embs):
        if calls is not None:
            calls.append((slot, len(embs)))
        start = slot * EMBEDDING_DIM
        return embs @ weight[:, start : start + EMBEDDING_DIM].T

    return SlotProjector(project, bias, max_cached_rows)


def full_layer(layer, embeddings, paths):
    weight, bias = layer
    features = np.array(
        [np.concatenate([embeddings[k] for k in p]) for p in paths],
        dtype=np.float32,
    )
    return features @ weight.T + bias


def random_paths(embeddings, n, seed):
    rng = np.random.default_rng(seed)
    vocab = sorted(embeddings)
    return [list(rng.choice(vocab, NUM_SLOTS)) for _ in range(n)]


def test_matches_full_first_layer(layer, embeddings):
    paths = random_paths(embeddings, 200, seed=2)
    projector = make_projector(layer)
    rows = [projector.index_path(p, embeddings.__getitem__) for p in paths]
    got = projector.hidden(np.asarray(rows))
    np.testing.assert_allclose(
        got, full_layer(layer, embeddings, paths), rtol=1e-4, atol=1e-3
    )


def test_each_slot_key_is_projected_once_across_chunks(layer, embeddings):
    calls = []
    projector = make_projector(layer, calls=calls)
    paths = random_paths(embeddings, 300, seed=3)
    for chunk in (paths[:100], paths[100:200], paths[200:]):
        rows = [projector.index_path(p, embeddings.__getitem__) for p in chunk]
        got = projector.hidden(np.asarray(rows))
        np.testing.assert_allclose(
            got, full_layer(layer, embeddings, chunk), rtol=1e-4, atol=1e-3
        )
    distinct = sum(len({p[s] for p in paths}) for s in range(NUM_SLOTS))
    assert sum(n for _, n in calls) == distinct
    assert projector.cached_rows == distinct


def test_eviction_resets_cache_and_stays_correct(layer, embeddings):
    projector = make_projector(layer, max_cached_rows=50)
    paths = random_paths(embeddings, 120, seed=4)
    evicted = False
    for start in range(0, len(paths), 30):
        chunk = paths[start : start + 30]
        rows = [projector.index_path(p, embeddings.__getitem__) for p in chunk]
        got = projector.hidden(np.asarray(rows))
        np.testing.assert_allclose(
            got, full_layer(layer, embeddings, chunk), rtol=1e-4, atol=1e-3
        )
        evicted |= projector.evict_if_full()
    assert evicted


def test_missing_key_raises_and_leaves_cache_consistent(layer, embeddings):
    projector = make_projector(layer)
    good = random_paths(embeddings, 5, seed=5)
    bad = list(good[0])
    bad[6] = "no-such-key"
    with pytest.raises(KeyError):
        projector.index_path(bad, embeddings.__getitem__)
    rows = [projector.index_path(p, embeddings.__getitem__) for p in good]
    got = projector.hidden(np.asarray(rows))
    np.testing.assert_allclose(
        got, full_layer(layer, embeddings, good), rtol=1e-4, atol=1e-3
    )
