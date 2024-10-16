
import struct

class ByteBuffer:

    def __init__(self, size=30):
        self.__bytes = [None]*size
        self.__pos = 0

    def set_position(self, pos):
        self.__pos = pos

    def from_bytes(self, bytes):
        self.__pos = 0
        self.__bytes = [b for b in bytes]

    def to_bytes(self):
        return bytes([b for b in self.__bytes if b != None])

    def read_int(self):
        b = self.__bytes[self.__pos: self.__pos + 4]
        val = int.from_bytes(b, byteorder='big', signed=True)
        self.__pos = self.__pos + 4
        return val

    def put_int(self, i):
        for i, c in enumerate(i.to_bytes(4, byteorder = 'big', signed=True)):
            self.__bytes[self.__pos + i] = c
        self.__pos = self.__pos + 4

    def read_float(self):
        b = bytes(self.__bytes[self.__pos: self.__pos + 4])
        f = struct.unpack_from('<f', b)[0]
        f = round(f, ndigits=2)
        self.__pos = self.__pos + 4
        return f

    def put_float(self, f):
        for i, c in enumerate(struct.pack('<f', f)):
            self.__bytes[self.__pos + i] = c
        self.__pos = self.__pos + 4

    def read_char(self):
        r = self.__bytes[self.__pos]
        r = bytes([r]).decode('utf-8')
        self.__pos += 1
        return r

    def put_char(self, c):
        b = c.encode('utf-8')
        self.__bytes[self.__pos] = b[0]
        self.__pos += 1