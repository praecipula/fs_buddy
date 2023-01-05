import os
import sys
import datetime

from sqlalchemy import Column, ForeignKey, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy import create_engine

Base = declarative_base()

class FileLikeObject(Base):
    __tablename__ = 'filelikes'
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('filelikes.id'))
    last_stat = Column(DateTime, nullable=False)                # When we last looked at the file
    path = Column(String(4096), index=True, nullable=False)     # The filename; max size is from extfs
    tree_size_bytes = Column(Integer, index=True)               # The size of the whole subtree (for files, size of self)
    is_container = Column(Boolean, default=False)               # True for directories but also TODO e.g. zip files, which can be recursed into.
    mime = Column(String(1024), index=True)                     # MIME type from Python Magic library
    fingerprint = Column(String(1024), index=True)              # The fingerprint of the file. Generally some hash, can be filetype-dependent.
    fingerprint_type = Column(String(256), index=False)         # The type of the fingerprint, e.g. "md5" or "feature_detection", e.g., for images.
    children = relationship('FileLikeObject',
            backref=backref('parent', remote_side=[id]))

    @classmethod
    def create(cls, pathname, parent_id = None):
        return FileLikeObject(
                parent_id=parent_id,
                last_stat = datetime.datetime.utcnow().isoformat(),
                path=os.path.abspath(pathname))



sqlite_db_filename = "file_metadata.sqlite"
engine = create_engine('sqlite:///{}'.format(sqlite_db_filename))
if not os.path.exists(sqlite_db_filename):
    print("Creating DB")
    Base.metadata.create_all(engine)

Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

session = DBSession()
