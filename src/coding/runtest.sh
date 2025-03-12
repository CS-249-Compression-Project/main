#!/bin/bash

# List of files to process
files=("a.txt" "b.txt")

# Encoding loop
for method in constant rle; do
    for file in "${files[@]}"; do
        echo "Encoding method: $method for file: $file"  # Debugging output
        python coding.py encode --method "$method" "$file" "encoded_${method}_${file}"
    done
done

# Decoding loop
for method in constant rle; do
    for file in "${files[@]}"; do
        echo "Decoding method: $method for file: $file"  # Debugging output
        python coding.py decode "encoded_${method}_${file}" "decoded_${method}_${file}"
    done
done
