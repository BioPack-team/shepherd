from sys import byteorder as native_byte_order

from shepherd_utils.arax.xcrg.memory import U32_View

BYTE_ARRAY = bytearray([
    0x12, 0x34, 0x56, 0x78,
    0x90, 0xAB, 0xCD, 0xEF
])

def test_exception_for_u32_buffers():
    try:
        U32_View(bytearray([0x12, 0x34]), byte_order = "little")
    except Exception as _:
        return
    assert False, "An exception should be raised when buffer is not aligned"

def test_u32be():
    view = U32_View(BYTE_ARRAY, byte_order = "big")
    assert len(view) == 2
    if native_byte_order == "little":
        assert view[0] == 0x12_34_56_78
        assert view[1] == 0x90_AB_CD_EF
    else:
        assert view[0] == 0x78_56_34_12
        assert view[1] == 0xEF_CD_AB_90

def test_u32le():
    view = U32_View(BYTE_ARRAY, byte_order = "little")
    assert len(view) == 2
    if native_byte_order == "big":
        assert view[0] == 0x12_34_56_78
        assert view[1] == 0x90_AB_CD_EF
    else:
        assert view[0] == 0x78_56_34_12
        assert view[1] == 0xEF_CD_AB_90

def test_iterator():
    view = U32_View(BYTE_ARRAY, byte_order = "little")
    iterator = iter(view)
    if native_byte_order == "big":
        assert next(iterator) == 0x12_34_56_78
        assert next(iterator) == 0x90_AB_CD_EF
    else:
        assert next(iterator) == 0x78_56_34_12
        assert next(iterator) == 0xEF_CD_AB_90
