#!/usr/bin/env python3
from typing import List, Union, Callable, Optional
from types import SimpleNamespace
import itertools
import os
import random
import struct

REPRESENTATION_KINDS = SimpleNamespace(DIRECT=1, RUN_LENGTH_ENCODED=2, DICTIONARY_ONE_BYTE=3, ONE_SBYTE_DELTA_ENCODED=4)

def try_compress(column_data: Union[List[int], List[float]], which_representation: int) -> Optional[bytes]:
    def datum_to_bytes(datum):
        data_is_ints = isinstance(datum, int)
        return struct.pack("<I" if data_is_ints else "<f", datum)

    match which_representation:
        case REPRESENTATION_KINDS.DIRECT:
            data_is_ints = len(column_data) > 0 and isinstance(column_data[0], int)
            return b"".join(datum_to_bytes(x) for x in column_data)
        case REPRESENTATION_KINDS.RUN_LENGTH_ENCODED:
            # count-value pairs where count is one byte.
            result = bytes()
            for value, group in itertools.groupby(column_data):
                count = sum(1 for _ in group)
                while count > 255:
                    result += struct.pack("<B", 255) + datum_to_bytes(value)
                    count -= 255
                result += struct.pack("<B", count) + datum_to_bytes(value)
            return result
        case REPRESENTATION_KINDS.DICTIONARY_ONE_BYTE:
            result = bytes()
            dictionary_values = list(set(column_data))
            if len(dictionary_values) > 256:
                return None
            result += struct.pack("<I", len(dictionary_values))
            for v in dictionary_values:
                result += datum_to_bytes(v)
            for datum in column_data:
                result += struct.pack("<B", dictionary_values.index(datum))
            return result
        case REPRESENTATION_KINDS.ONE_SBYTE_DELTA_ENCODED:
            # Encode the first value in four bytes, and encode every subsequent value as its difference
            # from the previous value, as one signed byte.
            if not isinstance(column_data[0], int):
                return None
            result = bytes()
            previous_value = None
            for value in column_data:
                if previous_value is None:
                    result += datum_to_bytes(value)
                else:
                    difference = value - previous_value
                    if -128 <= difference and difference <= 127:
                        result += struct.pack("<b", difference)
                    else:
                        return None
                previous_value = value
            return result
        case _:
            return None

def create_and_populate_table(filename: str, num_rows: int, row_group_size: int, column_generators: List[Callable[[], Union[int, float]]], preferred_column_representations: Optional[List[int]] = None):
    assert(preferred_column_representations is None or len(preferred_column_representations) == len(column_generators))
    num_columns = len(column_generators)
    if preferred_column_representations is None:
        preferred_column_representations = [REPRESENTATION_KINDS.DIRECT] * num_columns
    with open(filename, 'wb') as f:
        f.write(struct.pack('<I', num_rows))
        f.write(struct.pack('<I', num_columns))
        # Write column data
        for group_start in range(0, num_rows, row_group_size):
            group_size = min(num_rows - group_start, row_group_size)
            column_data = [[generator() for _ in range(group_size)] for generator in column_generators]
            # The column representations actually used for this row group. If the preferred ones fail on this row group,
            # REPRESENTATION_KINDS.DIRECT will be used.
            column_representations = list(preferred_column_representations)
            compressed_column_data = [try_compress(column_data[c], column_representations[c]) for c in range(num_columns)]
            for c in range(num_columns):
                if compressed_column_data[c] is None:
                    column_representations[c] = REPRESENTATION_KINDS.DIRECT
                    compressed_column_data[c] = try_compress(column_data[c], REPRESENTATION_KINDS.DIRECT)
            for c in range(num_columns):
                assert(compressed_column_data[c] is not None)
                # Write representation and number of bytes used
                f.write(struct.pack('<B', column_representations[c]))
                f.write(struct.pack('<I', len(compressed_column_data[c])))
            for c in range(num_columns):
                f.write(compressed_column_data[c])

def get_users_table_column_generators() -> List[Callable[[], Union[int, float]]]:
    def id_generator():
        id_generator.next_id += 1
        return id_generator.next_id - 1
    id_generator.next_id = 0
    def is_active_generator():
        return random.random() < 0.99
    def gender_generator():
        r = random.random()
        return 1 if r < 0.49 else 2 if r < 0.98 else 3
    return [id_generator, is_active_generator, gender_generator]

def get_purchases_table_column_generators(num_user_ids) -> List[Callable[[], Union[int, float]]]:
    def id_generator():
        return random.randint(0, num_user_ids - 1)
    def item_id_generator():
        return random.randint(0, 1 << 31 - 1)
    def price_generator():
        return 10.0 * random.random()
    return [id_generator, item_id_generator, price_generator]

def determine_bytes_per_row(num_rows: int, row_group_size: int, column_generators: List[Callable[[], Union[int, float]]], representations: Optional[List[int]]):
    create_and_populate_table("populate_tables_table.tbl", num_rows, row_group_size, column_generators, representations)
    size = os.path.getsize("populate_tables_table.tbl")
    os.remove("populate_tables_table.tbl")
    return size / num_rows

if __name__ == "__main__":
    num_purchases_rows = 1_000_000
    user_table_preferred_representations = [REPRESENTATION_KINDS.ONE_SBYTE_DELTA_ENCODED, REPRESENTATION_KINDS.RUN_LENGTH_ENCODED, REPRESENTATION_KINDS.DICTIONARY_ONE_BYTE]
    purchases_table_preferred_representations = [REPRESENTATION_KINDS.DICTIONARY_ONE_BYTE, REPRESENTATION_KINDS.DIRECT, REPRESENTATION_KINDS.DIRECT]
    for num_users_rows in [10, 100, 1_000]:
        for row_group_size in [50, 1000]:
            for reps in [(None, None), (user_table_preferred_representations, purchases_table_preferred_representations)]:
                user_rep, purchases_rep = reps
                users_bpr = determine_bytes_per_row(num_users_rows, 50, get_users_table_column_generators(), user_rep)
                purchases_bpr = determine_bytes_per_row(num_purchases_rows, 50, get_purchases_table_column_generators(num_users_rows), purchases_rep)
                print(f"users {num_users_rows}, row group size {row_group_size}, user rep {user_rep}, purchases rep {purchases_rep}; users: {users_bpr}, purchases: {purchases_bpr}")
    # create_and_populate_table("users.tbl", num_users_rows, get_users_table_column_generators(), user_table_preferred_representations)
    # create_and_populate_table("purchases.tbl", num_purchases_rows, get_purchases_table_column_generators(num_users_rows), purchases_table_preferred_representations)
