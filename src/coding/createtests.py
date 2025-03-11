import struct

def create_test_file(filename, data):
    """Create a binary file with the provided data"""
    with open(filename, 'wb') as f:
        f.write(bytes(data))
    print(f"Created file '{filename}' with {len(data)} bytes")

# 1: all zero file
zeros = [0] * 65536
with open('zeros.bin', 'wb') as f:
    f.write(bytes(zeros))

# 2: f1
# 0x01 0x00 0x00 0x00
byte_sequence = struct.pack('<I', 1)
with open('f1.bin', 'wb') as f:
    f.write(byte_sequence * 16384)

# 2: f2
# 0x00 0x01 0x00 0x00
byte_sequence = struct.pack('<I', 1<<8)
with open('f2.bin', 'wb') as f:
    f.write(byte_sequence * 16384)

# 3: f3
# 0x00 0x00 0x01 0x00
byte_sequence = struct.pack('<I', 1<<16)
with open('f3.bin', 'wb') as f:
    f.write(byte_sequence * 16384)