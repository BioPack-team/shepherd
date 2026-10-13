# Vendored from Translator-CATRAX/xCRG @ e67f2c0, src/xcrg/memory.py (DEC-21).
# Changes from upstream:
#   - unchanged
# See README.md.
from array import array
from mmap import mmap
from typing import Literal, Union

Byte_Order = Literal["big", "little"]
# TODO: Import "typing.Buffer" in Python 3.12
Buffer = Union[bytes, bytearray, memoryview, array, mmap]


class U32_View:
    """Provides a view over a byte buffer of unsigned 32-bit integers."""
    _mv         : memoryview
    _byte_order : Byte_Order

    def __init__(self, buffer: Buffer, byte_order: Byte_Order):
        if (len(buffer) % 4) != 0:
            raise ValueError("The buffer must be 4-byte aligned")
        self._mv = memoryview(buffer).cast("B")
        self._byte_order = byte_order

    def __getitem__(self, index: int, /) -> int:
        i = index * 4
        return int.from_bytes(self._mv[i:i + 4], byteorder = self._byte_order)

    def __len__(self) -> int:
        return int(len(self._mv) / 4)

    def __iter__(self):
        for i in range(len(self)):
            yield self.__getitem__(i)
