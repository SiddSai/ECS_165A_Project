import struct

PAGE_SIZE_BYTES = 4096
INT64_BYTES = 8
MAX_RECORDS = PAGE_SIZE_BYTES // INT64_BYTES

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE_BYTES)

    def has_capacity(self):
        return self.num_records < MAX_RECORDS

    def write(self, value):
        if not self.has_capacity():
            raise OverflowError("Page is full")

        if value is None:
            value = 0

        if not isinstance(value, int):
            raise TypeError(f"Page.write expects int, got {type(value)}")

        if value < -(1 << 63) or value > (1 << 63) - 1:
            raise OverflowError("Value out of signed 64-bit range")

        slot = self.num_records
        offset = slot * INT64_BYTES
        struct.pack_into("<q", self.data, offset, value)
        self.num_records += 1
        return slot
    
    def read(self, slot: int) -> int:
        if slot < 0 or slot >= self.num_records:
            raise IndexError("Slot out of bounds")
        offset = slot * INT64_BYTES
        return struct.unpack_from("<q", self.data, offset)[0]

    def update(self, slot: int, value: int) -> None:
        if slot < 0 or slot >= self.num_records:
            raise IndexError("Slot out of bounds")

        if value is None:
            value = 0

        if not isinstance(value, int):
            raise TypeError(f"Page.update expects int, got {type(value)}")

        if value < -(1 << 63) or value > (1 << 63) - 1:
            raise OverflowError("Value out of signed 64-bit range")

        offset = slot * INT64_BYTES
        struct.pack_into("<q", self.data, offset, value)
