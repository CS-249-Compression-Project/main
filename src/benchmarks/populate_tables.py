#!/usr/bin/env python3
from typing import List, Union, Callable, Optional
from types import SimpleNamespace
import random
import struct

MAX_ROW_GROUP_SIZE = 50

REPRESENTATION_KINDS = SimpleNamespace(DIRECT=1)

def try_compress(column_data: Union[List[int], List[float]], which_representation: int) -> Optional[bytes]:
    match which_representation:
        case REPRESENTATION_KINDS.DIRECT:
            data_is_ints = len(column_data) > 0 and isinstance(column_data[0], int)
            return b"".join(struct.pack("<I" if data_is_ints else "<f", x) for x in column_data)
        case _:
            return None

def create_and_populate_table(filename: str, num_rows: int, column_generators: List[Callable[[], Union[int, float]]]):
    num_columns = len(column_generators)
    with open(filename, 'wb') as f:
        f.write(struct.pack('<I', num_rows))
        f.write(struct.pack('<I', num_columns))
        # Write column data
        for group_start in range(0, num_rows, MAX_ROW_GROUP_SIZE):
            group_size = min(num_rows - group_start, MAX_ROW_GROUP_SIZE)
            column_data = [[generator() for _ in range(group_size)] for generator in column_generators]
            column_representations = [REPRESENTATION_KINDS.DIRECT] * num_columns
            compressed_column_data = [try_compress(column_data[c], column_representations[c]) for c in range(num_columns)]
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

if __name__ == "__main__":
    num_users_rows = 1_000
    num_purchases_rows = 10_000
    create_and_populate_table("users.tbl", num_users_rows, get_users_table_column_generators())
    create_and_populate_table("purchases.tbl", num_purchases_rows, get_purchases_table_column_generators(num_users_rows))
