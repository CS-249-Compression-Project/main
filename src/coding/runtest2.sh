#!/bin/bash

# List of files to process
FILES=("zeros.bin" "f1.bin" "f2.bin")

python createtests.py

# Encoding loop
for FILE in "${FILES[@]}"; do
    echo "encoding ${FILE} using bit method"
    python coding.py encode --method bit ${FILE} encoded.bin
    python coding.py decode encoded.bin decoded_file

    echo "comparing ${FILE} and decoded_file"
    diff ${FILE} decoded_file

    echo "removing encoded file and decoded_file file"
    rm encoded.bin decoded_file
done

rm *.bin