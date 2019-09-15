#!/usr/bin/env python

from lib.models.file import FileLikeObject, session
import datetime


if __name__ == "__main__":
    print("Starting")

    new_file = FileLikeObject(
            last_stat = datetime.datetime.now(),
            path = "DummyPath"
            )
    session.add(new_file)
    session.commit()

