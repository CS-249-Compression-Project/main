import argparse
import struct
import os
from itertools import groupby


class Encoder:
    """Class implementing multiple encoding techniques for data compression."""
    
    @staticmethod
    def constant_encoding(data):
        """
        Constant encoding: Store a single value if all values are identical.
        Returns the encoded data and a flag indicating if encoding was successful.
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
        Run length encoding: Store runs of values as (count, value) pairs.
        Returns the encoded data as a list of (count, value) tuples.
        """
        if not data:
            return []
            
        return [(len(list(g)), k) for k, g in groupby(data)]
    
    @staticmethod
    def decode_constant(value, length):
        """Decode constant-encoded data."""
        return [value] * length
    
    @staticmethod
    def decode_rle(encoded_data):
        """Decode run-length encoded data."""
        result = []
        for count, value in encoded_data:
            result.extend([value] * count)
        return result


def encode_file(input_file, output_file, encoding_type):
    """
    Encode a file using the specified encoding technique.
    
    Args:
        input_file: Path to the input file
        output_file: Path to the output file
        encoding_type: Type of encoding to use ('constant', 'rle', or 'for')
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
                
            original_size = len(data)
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
                # Write each count-value pair (5 bytes each: 4 for count, 1 for value)
                for count, value in encoded_data:
                    f.write(struct.pack('<IB', count, value))
            
            original_size = len(data)
            encoded_size = 9 + (5 * len(encoded_data))  # 1+4+4 bytes for header + 5 bytes per pair
            
        else:
            print(f"Error: Unknown encoding type '{encoding_type}'")
            return False
        
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
                    value = f.read(1)[0]
                    encoded_data.append((count, value))
                decoded_data = Encoder.decode_rle(encoded_data)
                
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
    encode_parser.add_argument('--method', choices=['constant', 'rle'], 
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