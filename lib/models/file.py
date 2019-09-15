import os
import sys

from sqlalchemy import Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy import create_engine

Base = declarative_base()

class FileLikeObject(Base):
    __tablename__ = 'filelikes'
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('file_like_object.id'))
    last_stat = Column(DateTime, nullable=False)                # When we last looked at the file
    path = Column(String(4096), index=True, nullable=False)     # The filename; max size is from extfs
    tree_size_bytes = Column(Integer, index=True)               # The size of the whole subtree (for files, size of self)
    children = relationship('FileLikeObject',
            backref=backref('parent', remote_side=['id']))



sqlite_db_filename = "file_metadata.sqlite"
engine = create_engine('sqlite:///{}'.format(sqlite_db_filename))
if not os.path.exists(sqlite_db_filename):
    print("Creating DB")
    Base.metadata.create_all(engine)

Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

session = DBSession()
