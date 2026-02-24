import struct

PAGE_SIZE_BYTES = 4096
INT64_BYTES = 8
MAX_RECORDS = PAGE_SIZE_BYTES // INT64_BYTES

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE_BYTES)

        """
        # Return True if there is space for at least one more 64-bit integer
        """
    def has_capacity(self) -> bool:
        return self.num_records < MAX_RECORDS

        """
        # Append a 64-bit signed integer to the next free slot in this page
        # Returns slot : int
        # Raises OverflowError if the page is full or the value is out of 64-bit range
        # Raises TypeError if value is not an integer
        """
    def write(self, value: int) -> int:
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
    
        """
        # Read the 64-bit signed integer stored at the given slot
        #Parameters slot : int
        # Raises IndexError if slot is out of bounds
        """
    def read(self, slot: int) -> int:
        
        if slot < 0 or slot >= self.num_records:
            raise IndexError("Slot out of bounds")

        offset = slot * INT64_BYTES
        return struct.unpack_from("<q", self.data, offset)[0]
     
        """
        # Overwrite the existing value at the given slot
        # Parameters slot : int
        # Raises IndexError if slot is out of bounds
        # RaisesTypeError if value is not an integer
        # Raises OverflowError if value is out of signed 64-bit range
        """
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

        """
        # Return the raw bytes of this page
        """
    def to_bytes(self) -> bytes:
        
        return bytes(self.data)
    
        """
        # Rebuild a Page from raw bytes read from disk.
        # Parameters raw : bytes （PAGE_SIZE_BYTES of raw page data）
        #num_records : int
        """
    @classmethod
    def from_bytes(cls, raw: bytes, num_records: int) -> "Page":
        
        if len(raw) != PAGE_SIZE_BYTES:
            raise ValueError("Invalid page size when rebuilding Page from bytes")

        page = cls()
        page.data[:] = raw
        page.num_records = num_records
        return page