#!/usr/bin/env python
"""Reducer that sorts input before grouping."""

import sys
from itertools import groupby
from operator import itemgetter

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def main(separator='\t'):
    # Read the input data
    data = read_mapper_output(sys.stdin, separator=separator)
    # Convert to list and sort the data by key (word)
    data = sorted(data, key=itemgetter(0))
    # Group the data and sum the counts
    for current_word, group in groupby(data, itemgetter(0)):
        try:
            total_count = sum(int(count) for current_word, count in group)
            print(f"{current_word}{separator}{total_count}")
        except ValueError:
            # Ignore lines where the count is not a number
            pass

if __name__ == "__main__":
    main()
