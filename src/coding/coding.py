import argparse
import struct
from itertools import groupby


class Encoder:
    """Encoding and decoding for data compression"""
    
    @staticmethod
    def constant_encoding(data):
        """
        Constant encoding: Store a single value if all values are identical.

        Returns the encoded data and a flag indicating if encoding was successful.
        ret: (value, is_constant)
        """
        if not data:
            return None, False
            
        first_value = data[0]
        is_constant = all(x == first_value for x in data)
        
        if is_constant:
            return first_value, True
        else:
            return None, False
    
    @staticmethod
    def run_length_encoding(data):
        """
        Run length encoding: Process data as 4-byte integers, storing runs of identical
        4-byte values as (count, value) pairs.

        Returns the encoded data as a list of (count, value) tuples where value is a 4-byte integer.
        ret: [(count, value), ...]
        """
        if not data:
            return []
        
        # Convert byte list into integers (4 bytes per integer)
        integers = []
        for i in range(0, len(data), 4):
            if i + 4 <= len(data):
                # Convert 4 bytes to an integer (little-endian)
                value = data[i] | (data[i+1] << 8) | (data[i+2] << 16) | (data[i+3] << 24)
                integers.append(value)
            else:
                # Handle the last chunk if it's less than 4 bytes
                remaining = len(data) - i
                value = 0
                for j in range(remaining):
                    value |= (data[i+j] << (j*8))
                integers.append(value)
        
        # Apply run-length encoding on the integers
        return [(len(list(g)), k) for k, g in groupby(integers)]
    
    @staticmethod
    def bit_packing_encoding(data):
        """
        Bit packing encoding: Process data as 4-byte integers, handling up to 1024 integers
        in each chunk (4096 bytes), finding the max value in each chunk and packing 
        all values in that chunk with the same bit width.
        
        Returns a list of (max_bits, chunk_values) pairs.
        ret: [(bits_per_value, packed_values), ...]
        """
        if not data:
            return []
        
        # Convert byte list into integers (assuming data is a list of bytes)
        # Each 4 consecutive bytes form one integer
        integers = []
        for i in range(0, len(data), 4):
            if i + 4 <= len(data):
                # Convert 4 bytes to an integer (little-endian)
                value = data[i] | (data[i+1] << 8) | (data[i+2] << 16) | (data[i+3] << 24)
                integers.append(value)
        
        INT_CHUNK_SIZE = 1024  # Process 1024 integers (4096 bytes) at a time
        BITS_PER_INT = 32  # Each original integer is 32 bits
        result = []
        
        # Process integers in chunks
        for i in range(0, len(integers), INT_CHUNK_SIZE):
            chunk = integers[i:i+INT_CHUNK_SIZE]
            
            # Find max value in this chunk
            max_value = max(chunk) if chunk else 0
            
            # Calculate bits needed for this chunk
            if max_value == 0:
                bits_per_value = 1
            else:
                bits_per_value = max_value.bit_length()
            
            # Pack values - in this case we pack multiple integers into as few 32-bit integers as possible
            packed_values = []
            
            if bits_per_value == 32:
                # If we need all 32 bits, no packing is done, just store the original values
                packed_values = chunk
            else:
                # We can pack multiple values into one 32-bit integer
                values_per_pack = BITS_PER_INT // bits_per_value
                
                for j in range(0, len(chunk), values_per_pack):
                    sub_chunk = chunk[j:j+values_per_pack]
                    packed_int = 0
                    
                    for k, value in enumerate(sub_chunk):
                        packed_int |= (value << (k * bits_per_value))
                    
                    packed_values.append(packed_int)
            
            # Store bits_per_value and the packed integers for this chunk
            result.append((bits_per_value, packed_values))
        
        return result

    @staticmethod
    def decode_constant(value, length):
        """Decode constant-encoded data."""
        return [value] * length
    
    @staticmethod
    def decode_rle(encoded_data):
        """Decode run-length encoded data where each value is a 4-byte integer."""
        result_integers = []
        for count, value in encoded_data:
            result_integers.extend([value] * count)
        
        # Convert integers back to bytes
        result_bytes = []
        for integer in result_integers:
            result_bytes.append(integer & 0xFF)
            result_bytes.append((integer >> 8) & 0xFF)
            result_bytes.append((integer >> 16) & 0xFF)
            result_bytes.append((integer >> 24) & 0xFF)
        
        return result_bytes
    
    @staticmethod
    def decode_bit_packing(encoded_chunks, original_length):
        """
        Decode bit-packed data that was processed as 4-byte integers.
        
        Args:
            encoded_chunks: List of (bits_per_value, packed_values) pairs
            original_length: The original number of integers
                
        Returns:
            List of bytes representing the unpacked 4-byte integers
        """
        result_integers = []
        BITS_PER_INT = 32  # Each original integer is 32 bits
        
        for bits_per_value, packed_values in encoded_chunks:
            if bits_per_value == 32:
                # If we used all 32 bits, no unpacking needed
                result_integers.extend(packed_values)
            else:
                # Calculate how many values can fit in each packed integer
                values_per_pack = BITS_PER_INT // bits_per_value
                
                # Create a bit mask for extracting values
                mask = (1 << bits_per_value) - 1
                
                # Extract values from each packed integer
                for packed_int in packed_values:
                    for j in range(values_per_pack):
                        if len(result_integers) < original_length:
                            value = (packed_int >> (j * bits_per_value)) & mask
                            result_integers.append(value)
        
        # Convert integers back to bytes
        result_bytes = []
        for integer in result_integers:
            result_bytes.append(integer & 0xFF)
            result_bytes.append((integer >> 8) & 0xFF)
            result_bytes.append((integer >> 16) & 0xFF)
            result_bytes.append((integer >> 24) & 0xFF)
        
        return result_bytes[:original_length]  # Ensure we return exactly the original number of bytes

def encode_file(input_file, output_file, encoding_type):
    """
    Encode a file using the specified encoding technique.
    
    Args:
        input_file: Path to the input file
        output_file: Path to the output file
        encoding_type: Type of encoding to use ('constant', 'rle', or 'bit')
    """
    try:
        # Read input file as binary
        with open(input_file, 'rb') as f:
            data = list(f.read())
        
        if not data:
            print("Error: Input file is empty")
            return False
        
        # Apply the specified encoding
        if encoding_type == 'constant':
            value, is_constant = Encoder.constant_encoding(data)
            if not is_constant:
                print("Error: Data is not constant, cannot use constant encoding")
                return False
                
            with open(output_file, 'wb') as f:
                # Write encoding type (1 byte)
                f.write(b'C')
                # Write original data length (4 bytes)
                f.write(struct.pack('<I', len(data)))
                # Write the constant value (1 byte)
                f.write(bytes([value]))

            encoded_size = 6  # 1 byte for type + 4 bytes for length + 1 byte for value
            
        elif encoding_type == 'rle':
            encoded_data = Encoder.run_length_encoding(data)
            
            with open(output_file, 'wb') as f:
                # Write encoding type (1 byte)
                f.write(b'R')
                # Write original data length (4 bytes)
                f.write(struct.pack('<I', len(data)))
                # Write number of RLE pairs (4 bytes)
                f.write(struct.pack('<I', len(encoded_data)))
                # Write each count-value pair (8 bytes each: 4 for count, 4 for value)
                for count, value in encoded_data:
                    f.write(struct.pack('<II', count, value))
            
            encoded_size = 9 + (8 * len(encoded_data))  # 1+4+4 bytes for header + 8 bytes per pair
            
        elif encoding_type == 'bit':
            encoded_chunks = Encoder.bit_packing_encoding(data)
            
            with open(output_file, 'wb') as f:
                # Write encoding type (1 byte)
                f.write(b'B')
                # Write original data length (4 bytes)
                f.write(struct.pack('<I', len(data)))
                # Write number of chunks (4 bytes)
                f.write(struct.pack('<I', len(encoded_chunks)))
                
                # Write each chunk
                for bits_per_value, packed_values in encoded_chunks:
                    # Write bits per value for this chunk (1 byte)
                    f.write(bytes([bits_per_value]))
                    # Write number of packed integers in this chunk (4 bytes)
                    f.write(struct.pack('<I', len(packed_values)))
                    # Write each packed integer (4 bytes each)
                    for packed_int in packed_values:
                        f.write(struct.pack('<I', packed_int))
            
            # Calculate encoded size: header + sum(5 + 4*len(packed_values) for each chunk)
            encoded_size = 9  # 1+4+4 bytes for header
            for _, packed_values in encoded_chunks:
                encoded_size += 5 + (4 * len(packed_values))  # 1+4 bytes for chunk header + 4 bytes per packed int
            
        else:
            print(f"Error: Unknown encoding type '{encoding_type}'")
            return False
        
        original_size = len(data)
        compression_ratio = (original_size / encoded_size) if encoded_size > 0 else float('inf')
        print(f"File encoded successfully using {encoding_type} encoding.")
        print(f"Original size: {original_size} bytes")
        print(f"Encoded size: {encoded_size} bytes")
        print(f"Compression ratio: {compression_ratio:.2f}x")
        
    except Exception as e:
        print(f"Error encoding file: {e}")
        return False
    
    return True


def decode_file(input_file, output_file):
    """
    Decode a file that was encoded using one of our encoding techniques.
    The encoding type is determined from the file header.
    """
    try:
        with open(input_file, 'rb') as f:
            # Read encoding type (1 byte)
            encoding_type = f.read(1)
            
            # Read original data length (4 bytes)
            original_length = struct.unpack('<I', f.read(4))[0]
            
            if encoding_type == b'C':
                # Constant encoding
                value = f.read(1)[0]
                decoded_data = Encoder.decode_constant(value, original_length)
                
            elif encoding_type == b'R':
                # Run length encoding
                num_pairs = struct.unpack('<I', f.read(4))[0]
                encoded_data = []
                for _ in range(num_pairs):
                    count = struct.unpack('<I', f.read(4))[0]
                    value = struct.unpack('<I', f.read(4))[0]  # Now reading 4 bytes for value instead of 1
                    encoded_data.append((count, value))
                decoded_data = Encoder.decode_rle(encoded_data)
                
            elif encoding_type == b'B':
                # Bit packing encoding
                num_chunks = struct.unpack('<I', f.read(4))[0]
                
                # Read each chunk
                encoded_chunks = []
                for _ in range(num_chunks):
                    bits_per_value = f.read(1)[0]
                    num_packed_values = struct.unpack('<I', f.read(4))[0]
                    
                    packed_values = []
                    for _ in range(num_packed_values):
                        packed_int = struct.unpack('<I', f.read(4))[0]
                        packed_values.append(packed_int)
                    
                    encoded_chunks.append((bits_per_value, packed_values))
                
                decoded_data = Encoder.decode_bit_packing(encoded_chunks, original_length)
                
            else:
                print(f"Error: Unknown encoding type in file")
                return False
        
        # Write decoded data
        with open(output_file, 'wb') as f:
            f.write(bytes(decoded_data))
            
        print(f"File decoded successfully. Output size: {len(decoded_data)} bytes")
        
    except Exception as e:
        print(f"Error decoding file: {e}")
        return False
    
    return True


def main():
    parser = argparse.ArgumentParser(description='Compression with Multiple Encoding Techniques')
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Encode command
    encode_parser = subparsers.add_parser('encode', help='Encode a file')
    encode_parser.add_argument('input', help='Input file path')
    encode_parser.add_argument('output', help='Output file path')
    encode_parser.add_argument('--method', choices=['constant', 'rle', 'bit'], 
                              required=True, help='Encoding method to use')
    
    # Decode command
    decode_parser = subparsers.add_parser('decode', help='Decode a file')
    decode_parser.add_argument('input', help='Input file path')
    decode_parser.add_argument('output', help='Output file path')
    
    args = parser.parse_args()
    
    if args.command == 'encode':
        encode_file(args.input, args.output, args.method)
    elif args.command == 'decode':
        decode_file(args.input, args.output)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()