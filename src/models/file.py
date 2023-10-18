import os
import stat
import sys
import re
import datetime
import magic
import imohash          # imohash is a fast nearly-file-size-independent sampling hash algorithm.
import subprocess
import pathlib
from PIL import Image, ExifTags

from sqlalchemy import Column, ForeignKey, Integer, BigInteger, String, DateTime, Boolean, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy import create_engine

Base = declarative_base()

import logging
import python_logging_base
from python_logging_base import ASSERT, TODO

LOG = logging.getLogger("flo")
LOG.level = logging.DEBUG

PILLog = logging.getLogger("PIL.TiffImagePlugin")
PILLog.level = logging.INFO

flo_scan_cache = {}

class FileLikeObject(Base):
    __tablename__ = 'filelikes'
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('filelikes.id'))
    last_stat = Column(DateTime, nullable=False)                # When we last looked at the file
    path = Column(String(4096), index=True, nullable=False)     # The filename; max size is from extfs
    permissions = Column(String(12), nullable=False)            # Permissions in human-readable string 
    tree_size_bytes = Column(BigInteger, index=True)            # The size of the whole subtree (for files, size of self)
    directory = Column(Boolean, default=False)                  # True for directories
    inode = Column(BigInteger)                                  # The inode of the file
    dev_number = Column(Integer, index=True)                    # The device number of the file
    mime = Column(String(1024), index=True)                     # MIME type from Python Magic library
    fingerprint = Column(String(1024), index=True)              # The fingerprint of the file. Generally some hash, can be filetype-dependent.
    fingerprint_type = Column(String(256), index=True)          # The type of the fingerprint, e.g. "md5" or "feature_detection", e.g., for images.
    last_access = Column(DateTime, index=False)
    last_modified = Column(DateTime, index=True)
    creation_or_meta = Column(DateTime, index=True)
    children = relationship('FileLikeObject',
            backref=backref('parent', remote_side=[id]))
    image_meta = relationship('ImageMetadata', uselist=False,
            backref=backref('file', remote_side=[id]))

    def fs_refresh(self):
        """
        Refresh the file like information on the filesystem.
        """
        # Get the absolute path just to be sure
        full_path = os.path.abspath(self.path)
        # TODO: how to handle moves? hmm. Anyway, for now just re-set the path to absolute.
        self.path = full_path
        # Stat that sucker
        f_stat = os.lstat(self.path)
        self.last_stat = datetime.datetime.now()
        LOG.trace(f"Stats for file {self.path} collected")
        LOG.trace(f"* Is a directory? {stat.S_ISDIR(f_stat.st_mode)}")
        if stat.S_ISDIR(f_stat.st_mode): self.directory = True
        LOG.trace(f"* Is a regular file? {stat.S_ISREG(f_stat.st_mode)}")
        LOG.trace(f"* Is a symlink? {stat.S_ISLNK(f_stat.st_mode)}")
        self.permissions = stat.filemode(f_stat.st_mode)
        LOG.trace(f"* Permissions bits? {oct(stat.S_IMODE(f_stat.st_mode))}, or {self.permissions}")
        self.inode = f_stat[stat.ST_INO]
        LOG.trace(f"* Inode number? {self.inode}")
        self.dev_number = f_stat[stat.ST_DEV]
        LOG.trace(f"* Device number? {self.dev_number}")
        LOG.trace(f"* User ID number? {f_stat[stat.ST_UID]}")
        LOG.trace(f"* Group ID number? {f_stat[stat.ST_GID]}")
        if not self.directory: self.tree_size_bytes = f_stat[stat.ST_SIZE]
        # Directories will have tree sizes set on "unwind" of descending into all their children.
        LOG.trace(f"* Size in bytes? {self.tree_size_bytes}")
        self.last_access = datetime.datetime.fromtimestamp(f_stat[stat.ST_ATIME])
        LOG.trace(f"* Last access time? {self.last_access}")
        self.last_modified = datetime.datetime.fromtimestamp(f_stat[stat.ST_MTIME])
        LOG.trace(f"* Last modification time? {self.last_modified}")
        self.creation_or_meta = datetime.datetime.fromtimestamp(f_stat[stat.ST_CTIME])
        LOG.trace(f"* Creation/ last metadata change time? {self.creation_or_meta}")
        # Finally, do mime types and file hashes if we're a regular file (not a directory or symlink or pipe)
        if stat.S_ISREG(f_stat.st_mode):
            self.mime = magic.from_file(self.path, mime=True)
            LOG.trace(f"* Mime type: {self.mime}")
            self.fingerprint_type = "imohash_default_hex"
            self.fingerprint = imohash.hashfile(self.path, hexdigest=True)
            LOG.trace(f"* Fingerprint ({self.fingerprint_type}): {self.fingerprint}")
            if self.mime.startswith("image"):
                meta = ImageMetadata(file = self)
                meta.populate_from_file()
                self.image_meta = meta


    @staticmethod
    def scan_recursively(file):
        """
        Call this function from the outside.
        Sets up recursion and ensures commits when we're done.
        """
        # This is the fastest way I know of to do this. Python iterating over directories
        # is slower than even this redirect.

        LOG.info(f"Counting files...")
        find_sub = subprocess.Popen(("find", file.path), stdout=subprocess.PIPE)
        output = subprocess.check_output(("wc", "-l"), stdin = find_sub.stdout)
        find_sub.wait()
        count = int(output.strip())
        LOG.info(f"Processing approximately {count} files")
        FileLikeObject.depth_first_recurse(file, count, 0, datetime.datetime.now())
        # Since we might not have committed the last set on a non-round batch size, do so now.
        session.commit()

    @staticmethod
    def depth_first_recurse(file, expected_total, files_processed, start_time):
        file.fs_refresh()
        # Nothing else to do if a file; recurse if dir.
        if file.directory:
            # Aggregate the bytes as we go for this directory's "size"
            aggregated_bytes = 0
            try:
                with os.scandir(file.path) as iterator:
                    for entry in iterator:
                        # This is not a perfect check but collisions should be vanishingly rare.
                        # We really want unique per path and *device id*, but device id is not returned
                        # form os.scandir as a cached entry. However, on non-Windows, inode is documented
                        # as being returned. It would be a strong coincidence that a file has both the
                        # same path and inode on two different volumes; good enough for me.
                        db_existing_entries = session.query(FileLikeObject).filter(FileLikeObject.path == entry.path).filter(FileLikeObject.inode == entry.inode()).limit(2).all()
                        ASSERT(len(db_existing_entries) <= 1, "Got more than one entry with path {entry.path}")
                        flo = None
                        if len(db_existing_entries) == 1:
                            flo = db_existing_entries[0]
                        else:
                            flo = FileLikeObject(path=entry.path)
                        # Recurse, getting flo's stats and, if directory, its stat and childrens' stats.
                        files_processed = FileLikeObject.depth_first_recurse(flo, expected_total, files_processed, start_time)
                        # Now that we're on the upside of the recursion, every sub file or directory has its size.
                        if flo.tree_size_bytes != None: # This can happen when e.g. a folder doesn't have permissions - it's effectively 0 bytes to us.
                            aggregated_bytes += flo.tree_size_bytes
                # OK, should have recursed amongst all children. Now "this" size should be accurate.
                file.tree_size_bytes = aggregated_bytes
            except PermissionError as p:
                LOG.error(f"Did not have permission to descend into {file.path}")
                return files_processed + 1
        session.add(file)
        if files_processed > 0 and files_processed % 100 == 0:
            time_delta = datetime.datetime.now() - start_time
            seconds_per_file = float(time_delta.seconds) / files_processed
            remaining = expected_total - files_processed
            remaining_time = datetime.timedelta(seconds=remaining * seconds_per_file)
            end_time = datetime.datetime.now() + remaining_time
            LOG.info(f"Files processed: {files_processed:7d} in {time_delta.seconds:5d} seconds, {seconds_per_file:.6f} seconds/file; projected end {end_time}")
            session.commit()
        return files_processed + 1

class ImageMetadata(Base):

    name_conversion_regex = re.compile(r'(?<!^)(?=[A-Z])')

    __tablename__ = 'image_meta'
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey('filelikes.id'))
    make = Column(String)
    model = Column(String)
    software = Column(String)
    orientation = Column(Integer)
    date_time = Column(DateTime) # This is in local time, but interestingly OffsetTime gives TZ offset... might be useful to know.
    x_resolution = Column(Numeric)
    y_resolution = Column(Numeric)
    date_time_original = Column(DateTime)
    shutter_speed_value = Column(Numeric)
    aperture_value = Column(Numeric)
    brightness_value = Column(Numeric)
    exposure_bias_value = Column(Numeric)
    gps_latitude_dms = Column(String)
    gps_longitude_dms = Column(String)
    gps_altitude_m = Column(Numeric)
    gps_datetime = Column(DateTime)
    gps_direction = Column(Numeric) #"M" is the most common suffix; this is "ref to magnetic north"

    def populate_from_file(self):
        img = Image.open(self.file.path)
        exif = img._getexif()
        if exif == None:
            LOG.info(f"{self.file.path} has no EXIF data")
            return
        for (k, v) in exif.items():
            tagname = ExifTags.TAGS[k]
            # Direct / easy set with little type manipulation
            if tagname in ["Make", "Model", "Software", "Orientation", "XResolution", "YResolution", \
                    "ShutterSpeedValue", "ApertureValue", "BrightnessValue", "ExposureBiasValue"]:
                propname = ImageMetadata.name_conversion_regex.sub("_", tagname).lower()
                setattr(self, propname, v)
            elif tagname in ["DateTime", "DateTimeOriginal"]:
                # Turns out this also works with our naming here. That might not be as generally true
                # with values that require massaging (the GPS values do, for example).
                # Also these are just datetimes so we can generalize that for now too.
                propname = ImageMetadata.name_conversion_regex.sub("_", tagname).lower()
                setattr(self, propname, datetime.datetime.strptime(v, "%Y:%m:%d %H:%M:%S"))
            LOG.trace(f"{tagname} => {v}")
            if tagname == "GPSInfo": # Unpack the GPS subtags
                for (l, w) in v.items():
                    subtagname = ExifTags.GPSTAGS[l]
                    LOG.trace(f"  {subtagname} => {w}")
                # Assume the standard set of tags are always here if any gps tags are.
                lat_t = v[ExifTags.GPS.GPSLatitude]
                lat_h = v[ExifTags.GPS.GPSLatitudeRef]
                self.gps_latitude_dms = f"{int(lat_t[0])}°{int(lat_t[1])}'{lat_t[2]}\"{lat_h}"
                lon_t = v[ExifTags.GPS.GPSLongitude]
                lon_h = v[ExifTags.GPS.GPSLongitudeRef]
                self.gps_longitude_dms = f"{int(lon_t[0])}°{int(lon_t[1])}'{lon_t[2]}\"{lon_h}"
                self.gps_altitude = v[ExifTags.GPS.GPSAltitude] # All the references I see are from \00 - sea level
                if ExifTags.GPS.GPSImgDirection in v:
                    self.gps_direction = v[ExifTags.GPS.GPSImgDirection]
                date_string = v[ExifTags.GPS.GPSDateStamp]
                time_t = v[ExifTags.GPS.GPSTimeStamp]
                stringified = f"{date_string} {int(time_t[0])}:{int(time_t[1])}:{int(time_t[2])}"
                gps_datetime = datetime.datetime.strptime(stringified, "%Y:%m:%d %H:%M:%S")
        return self



class DuplicateView(Base):
    """
    Defined in sqlite as:
    CREATE VIEW view_duplicates AS SELECT a.id, a.path, a.fingerprint, a.mime, a.tree_size_bytes, a.dev_number, a.creation_or_meta FROM filelikes a JOIN (SELECT path, fingerprint, COUNT(*) FROM filelikes GROUP BY fingerprint HAVING COUNT(*) > 1 ) b ON a.fingerprint = b.fingerprint ORDER BY a.fingerprint DESC

    TODO: find a way to define this in SQLAlchemy?
    """
    __tablename__ = "view_duplicates"
    __table_args__ = {'info': dict(is_view=True)}

    # A subset of the stuff in the main table
    id = Column(Integer, primary_key=True)
    path = Column(String(4096), index=True, nullable=False)     # The filename; max size is from extfs
    fingerprint = Column(String(1024), index=True)              # The fingerprint of the file. Generally some hash, can be filetype-dependent.
    mime = Column(String(1024), index=True)                     # MIME type from Python Magic library
    tree_size_bytes = Column(BigInteger, index=True)            # The size of the whole subtree (for files, size of self)
    dev_number = Column(Integer, index=True)                    # The device number of the file
    creation_or_meta = Column(DateTime, index=True)

    @staticmethod
    def scan_for_duplicate_folders():
        # Map of directory to
        #   Map of directory to array of ids of matches
        #
        # This is a bidirectional map; i.e. there will be an A => B and a B => A pairing (because we don't yet know the set we draw bidirectional edges to cover all keys)
        folders = {}
        def bidirectional_duplicates(value, last_value):
            last_dir = str(pathlib.Path(last_value.path).parent)
            current_dir = str(pathlib.Path(value.path).parent)
            def map_a_b(a, b):
                if folders.get(a) == None:
                    folders[a] = {}
                if folders.get(a).get(b) == None:
                    folders[a][b] = 0
                folders[a][b] += 1
            map_a_b(current_dir, last_dir)
            map_a_b(last_dir, current_dir)

        dupes = session.query(DuplicateView).all()
        last_value = None
        for value in dupes:
            # First loop through we don't care; we need two (in order to get the first directory name)
            if last_value != None and value.fingerprint == last_value.fingerprint:
                bidirectional_duplicates(value, last_value)
            last_value = value
        # Now create tuples from the hashes
        tuples = set()
        for key, innerdict in folders.items():
            for innerkey, count in innerdict.items():
                # Dedupe with alphabetical sort
                smaller_key = min(key, innerkey)
                larger_key = max(key, innerkey)
                tup = (smaller_key, larger_key, count)
                tuples.add(tup)
        # Return sorted list of (dir, dir, count_overlaps) tuples by number of overlapping files
        return sorted(list(tuples), key=lambda t: -t[2])





session = None

in_memory_session = None
def bind_in_memory_db():
    in_memory_engine = create_engine('sqlite://')
    print("Creating in-memory DB")
    Base.metadata.create_all(in_memory_engine)

    Base.metadata.bind = in_memory_engine
    DBSession = sessionmaker(bind=in_memory_engine)

    in_memory_session = DBSession()
    return in_memory_session

file_session = None
def bind_file_db():

    sqlite_db_filename = "file_metadata.sqlite"
    file_engine = create_engine(f'sqlite:///{sqlite_db_filename}')
    print(f"Creating filesystem DB at {sqlite_db_filename}")
    Base.metadata.create_all(file_engine)

    Base.metadata.bind = file_engine
    DBSession = sessionmaker(bind=file_engine)

    file_session = DBSession()
    return file_session

session = bind_file_db() # For now it doesn't seem to help perf by worrying about in-memory or in file.
