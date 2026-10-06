"""First-layer projection cache for the path-scoring MLP.

The scorer's input is the concatenation of 11 embeddings (4 node names, 4
categories, 3 hop phrases), and its first layer is linear. So its pre-activation
splits into one term per slot::

    W @ concat(e_0, ..., e_10) + b  ==  b + sum_k W[:, k*768:(k+1)*768] @ e_k

Each term depends only on the slot and the key that fills it, and a message's
paths share keys heavily: the source and target (slots 0-1, 9-10) are the same
for every analysis of a result, categories and hop phrases come from small
vocabularies, and intermediate nodes repeat across many paths. Projecting each
distinct (slot, key) once and summing 11 cached rows per path replaces the
8448 x 1536 matmul -- about 85% of the network's FLOPs -- with a gather and add.

The result equals the full first layer up to float32 rounding (the sum is just
reassociated). Kept free of torch so it can be tested without the worker image.
"""

from typing import Callable

import numpy as np

EMBEDDING_DIM = 768
NUM_SLOTS = 11


class SlotProjector:
    """Caches per-slot first-layer projections for one message.

    Usage per chunk: ``index_path`` each path (which records any keys not yet
    seen), then ``hidden`` on the chunk's index rows, then ``evict_if_full``.

    ``project(slot, embeddings)`` maps a ``(n, 768)`` float32 array of slot
    ``slot``'s embeddings to their ``(n, hidden)`` projections -- the worker
    does this with torch so the matmul honours its thread limit.

    Memory is bounded by ``max_cached_rows`` across all slots: once a chunk
    leaves the cache past that, it is dropped and later chunks re-project what
    they need. Eviction only happens between chunks, so indices handed out by
    ``index_path`` stay valid until the chunk using them has been scored.
    """

    def __init__(
        self,
        project: Callable[[int, np.ndarray], np.ndarray],
        bias: np.ndarray,
        max_cached_rows: int,
    ):
        self._project = project
        self._bias = np.asarray(bias, dtype=np.float32)
        self._hidden_dim = self._bias.shape[0]
        self._max_cached_rows = max_cached_rows
        self.reset()

    def reset(self) -> None:
        self._index = [{} for _ in range(NUM_SLOTS)]
        # Embeddings seen but not yet projected, per slot, in index order.
        self._pending = [[] for _ in range(NUM_SLOTS)]
        # Projected rows per slot, grown by doubling; _size[k] rows are valid.
        self._proj = [
            np.empty((0, self._hidden_dim), dtype=np.float32) for _ in range(NUM_SLOTS)
        ]
        self._size = [0] * NUM_SLOTS

    @property
    def cached_rows(self) -> int:
        return sum(len(table) for table in self._index)

    def index_path(
        self, keys: list[str], lookup: Callable[[str], np.ndarray]
    ) -> list[int]:
        """Return the cache row for each of a path's 11 keys, in slot order.

        ``lookup`` fetches a key's embedding and raises ``KeyError`` if it has
        none; that propagates so the caller can skip the path. Keys of earlier
        slots registered before the failure are real and stay cached.
        """
        rows = []
        for slot, key in enumerate(keys):
            table = self._index[slot]
            row = table.get(key)
            if row is None:
                embedding = lookup(key)
                row = len(table)
                table[key] = row
                self._pending[slot].append(embedding)
            rows.append(row)
        return rows

    def _project_pending(self) -> None:
        for slot in range(NUM_SLOTS):
            pending = self._pending[slot]
            if not pending:
                continue
            projected = self._project(slot, np.asarray(pending, dtype=np.float32))
            size = self._size[slot]
            needed = size + len(pending)
            store = self._proj[slot]
            if needed > store.shape[0]:
                grown = np.empty(
                    (max(needed, 2 * store.shape[0], 64), self._hidden_dim),
                    dtype=np.float32,
                )
                grown[:size] = store[:size]
                self._proj[slot] = store = grown
            store[size:needed] = projected
            self._size[slot] = needed
            pending.clear()

    def hidden(self, rows: np.ndarray) -> np.ndarray:
        """First-layer pre-activations for a ``(batch, 11)`` array of rows."""
        self._project_pending()
        out = self._proj[0][rows[:, 0]]
        out += self._bias
        for slot in range(1, NUM_SLOTS):
            out += self._proj[slot][rows[:, slot]]
        return out

    def evict_if_full(self) -> bool:
        """Drop the cache if it has outgrown its budget. Call between chunks."""
        if self.cached_rows > self._max_cached_rows:
            self.reset()
            return True
        return False
