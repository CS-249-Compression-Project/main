#!/bin/bash

# make tests
make

list=$(ls test_*)  # List all files that start with 'tests_'
declare -A base_names_set  # Using associative array as a set

# List all files starting with 'tests_' and strip the extensions
for file in $list; do
    if [[ -f "$file" ]]; then  # Ensure it's a file
        # Get the base name without the extension
        base_name=$(basename "$file" .${file##*.})
        echo "$base_name"

        if [[ -z "${base_names_set[$base_name]}" ]]; then
            # If not in the set, add it
            base_names_set["$base_name"]=1

            # Print the base name
            echo "$base_name"

            # Run the test (e.g., execute the test)
            "./$base_name"
        fi

        "./$base_name"
    fi
done

# clean up tests
make superclean