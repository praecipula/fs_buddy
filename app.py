#!/usr/bin/env python

import cProfile, pstats, io
from src.models.file import FileLikeObject, session
import datetime
import argparse


import logging
import python_logging_base
from python_logging_base import ASSERT, TODO

python_logging_base.example_logs()
LOG = logging.getLogger("fs_buddy")


if __name__ == "__main__":
    print("Starting with self data")


    parser = argparse.ArgumentParser(prog = "FS Buddy", description=f'Batch tools for managing a mess of files')
    parser.add_argument("directories", metavar='DIRECTORY', type=str, nargs='+')

    args = parser.parse_args()

    pr = cProfile.Profile()
    pr.enable()
    for d in args.directories:
        new_file = FileLikeObject(
                path = d
                )
        FileLikeObject.scan_recursively(new_file)
    pr.disable()

    s = io.StringIO()
    sortby = pstats.SortKey.CUMULATIVE
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    with open("perf.txt", 'w') as f:
        print(s.getvalue(), file=f)

    LOG.info("Printed stats to perf.txt")
