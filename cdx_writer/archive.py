"""
Augments Warctools with features needed for genrating CDX from real-world
web archive files. When we're convinced these modification are good for wider
audiences, submit patch to Warctools.

Historically, these modifications were once implemented as local Warctool
modifications. Unfortunately major changes to Warctool after the fork made those
changes very difficult to merge. Here we reimplement them as runtime patches.

As this module monkey-patches hanzo.warctools modules upon import, all access to
hanzo.warctools shall be made after importing this module.
"""
from __future__ import unicode_literals

import re
import hanzo
from hanzo.warctools import ArchiveRecord
from hanzo.warctools.stream import open_record_stream as _open_record_stream
from .handler import ParseError

from hanzo.warctools.arc import SPLIT, ArcParser, ArcRecord
from hanzo.warctools.warc import WarcRecord
from hanzo.warctools.stream import RecordStream, GeeZipFile, GzipRecordStream

try:
    from .zstdstream import ZstdRecordStream, get_zstd_dictionary
except ImportError:
    ZstdRecordStream = None

ARC_HEADER_V1 = [ArcRecord.URL, ArcRecord.IP, ArcRecord.DATE, ArcRecord.CONTENT_TYPE,
                 ArcRecord.CONTENT_LENGTH]

ARC_HEADER_FIELDS = {
    ArcRecord.URL: br"([a-z]+:.*)",
    # some IP-Address field has hostname
    ArcRecord.IP: br"((?:\d{1,3}\.){3}\d{1,3}|-_a-zA-Z0-9.]+)",
    # some timestamps have more or less digits than 14
    ArcRecord.DATE: br"(\d{12,16})",
    ArcRecord.CONTENT_TYPE: br"(\S+)(?:;\s*\S+)?",
    ArcRecord.CONTENT_LENGTH: br"(\d+)",
    ArcRecord.RESULT_CODE: br"(\d{3})",
    ArcRecord.CHECKSUM: br"(\S+)",
    # many redirect URLs contain white spaces
    ArcRecord.LOCATION: br"(-|[a-z]+:\S.*)",
    ArcRecord.OFFSET: br"(\d+)",
    # filename often contains spaces
    ArcRecord.FILENAME: br"(\S[\S ]*\S)"
}

RE_DATE = re.compile(ARC_HEADER_FIELDS[ArcRecord.DATE] + b'$')
RE_IP = re.compile(ARC_HEADER_FIELDS[ArcRecord.IP] + b'$')

def F(spec):
    fields = [getattr(ArcRecord, a) for a in spec.split()]
    regex = re.compile(
        b" ".join(ARC_HEADER_FIELDS[f] for f in fields) + b"$"
    )
    return fields, regex

ARC_HEADER_FORMATS = [
    # standard v1 header
    F("URL IP DATE CONTENT_TYPE CONTENT_LENGTH"),
    # standard v2 header
    F("URL IP DATE CONTENT_TYPE RESULT_CODE CHECKSUM LOCATION OFFSET FILENAME CONTENT_LENGTH"),
    # some Alexa ARC files have only 4 fields for v1, missing Content-Type
    F("URL IP DATE CONTENT_LENGTH"),
]

class PatchedArcParser(ArcParser):
    def __init__(self):
        self.version = 1
        self.headers = ARC_HEADER_V1

    def parse_header_list(self, line):
        """replaces ArcParser.parse_header_list to support
        unusual ARC header.
        """
        line = line.rstrip(b'\r\n')
        values = SPLIT(line)
        headers = self.headers
        if len(values) == len(headers):
            header_dict = dict(zip(headers, values))
            # some old Alexa ARC files have IP-Address and Date field transposed in ARC header.register_record_type
            # see small_warcs/transposed_header.arc.gz
            date = header_dict.get(ArcRecord.DATE, '')
            ip = header_dict.get(ArcRecord.IP, '')
            if (RE_IP.match(date) and RE_DATE.match(ip)):
                header_dict[ArcRecord.DATE] = ip
                header_dict[ArcRecord.IP] = date

            return header_dict.items()

        for headers, regex in ARC_HEADER_FORMATS:
            m = regex.match(line)
            if m:
                values = m.groups()
                return list(zip(headers, values))

        raise Exception('ARC header %s does not match declared %s',
                        line, ",".join(self.headers))
        # if len(values) > len(headers):
        #     # line has more fields than declared - following is copy of warctools 4.10 code.
        #     if self.headers[0] in (ArcRecord.URL, ArcRecord.CONTENT_TYPE):
        #         # guess URL or Content-type field has stray space
        #         values = [s[::-1] for s in reversed(SPLIT(line[::-1], len(headers) - 1))]
        #     else:
        #         # leave extra fields in the last item
        #         values = SPLIT(line, len(headers) - 1)
        # elif len(values) < len(headers):
        #     if len(values) == 5:
        #         # 1. some ARC writes out v1 header while declaring v2 header in filedesc.
        #         headers = ARC_HEADER_V1
        #     elif len(values) == 4:
        #         # 2. some Alexa ARC files have just 4 fields, missing Content-Type.
        #         headers = [ArcRecord.URL, ArcRecord.IP, ArcRecord.DATE, ArcRecord.CONTENT_LENGTH]

        # if len(headers) != len(values):
        #     raise Exception('ARC header %s does not match declared %s',
        #                     ",".join(values), ",".join(self.headers))

        # # 3. some old Alexa ARC files have IP-Address and Date field transposed in ARC header
        # # see small_warcs/transposed_header.arc.gz
        # if len(values) == 5:
        #     if RE_DATE.match(values[1]) and RE_IP.match(values[2]):
        #         values[1:3] = values[2:0:-1]

        # return list(zip(headers, values))

hanzo.warctools.arc.ArcParser = PatchedArcParser

# this only works for Python 2.7 and <=3.4
class PatchedGeeZipFile(GeeZipFile):
    def __init__(self, *args, **kwargs):
        GeeZipFile.__init__(self, *args, **kwargs)
        self.at_eom = None

    def finish_member(self):
        """Read off member to the end. self.raw_fh will be at the end
        of member (just after the checksum bytes) after calling this
        method.
        Calling this method when at_eom==True is no-op.
        """
        if not self.at_eom or self.extrasize > 0:
            while True:
                d = self.read(1024)
                if not d: break

    def next_block(self):
        # read off until the end of current member
        # (at_eom==None is considered at_eom here, to allow calling
        # next_block() before reading the first member)
        if self.at_eom is not None:
            self.finish_member()
        self.at_eom = False
        # self.extrasize is supposed to be 0
        try:
            # start reading next member
            self._read()
            # self._new_member and self.at_eom are both supposed to be
            # False here, *unless* member is smaller than one read.
            return True
        except EOFError:
            return False

    def _read(self, size=1024):
        # in EOM state, return EOF until it's reset.
        if self.at_eom:
            raise EOFError('Reached End-of-Member')
        GeeZipFile._read(self, size)
        if self._new_member:
            self.at_eom = True

    def _add_read_data(self, data):
        GeeZipFile._add_read_data(self, data)
        assert self.offset - self.extrastart + self.extrasize == len(self.extrabuf)

    def close(self):
        # debugging
        import traceback


#__import__('hanzo').warctools.stream.GeeZipFile = PatchedGeeZipFile

class PatchedGzipRecordStream(GzipRecordStream):
    def __init__(self, file_handle, record_parser):
        RecordStream.__init__(self, PatchedGeeZipFile(fileobj=file_handle),
                              record_parser)
        self.raw_fh = file_handle

    def _finish_record(self):
        self.fh.finish_member()

    def _read_record(self, offsets):
        # overridden to call next_block()
        # if self.bytes_to_eoc is not None:
        #     self._skip_to_eoc()
        # self.bytes_to_eoc = None
        # clear EOM state
        self.fh.next_block()
        self.bytes_to_eoc = None # not necessary, probably
        record, errors, _offset = \
            self.record_parser.parse(self, offset=None, line=None)
        offset = self.fh.member_offset
        return offset, record, errors

    def read_records(self, limit=1, offsets=True):
        # overridden to support empty gzip member
        nrecords = 0
        prev_offset = None
        while limit is None or nrecords < limit:
            offset, record, errors = self._read_record(offsets)
            nrecords += 1
            yield offset, record, errors
            if not record and prev_offset is not None and prev_offset == offset:
                break
            prev_offset = offset


hanzo.warctools.stream.GzipRecordStream = PatchedGzipRecordStream

def open_record_stream(record_class=None, filename=None, file_handle=None,
                       mode='rb', gzip='auto', offset=None, length=None):
    # assumes our specific way of calling. does not support general usage.
    assert record_class is None and filename is not None and file_handle is None
    assert offset is None
    if filename.endswith('.zst'):
        if ZstdRecordStream is None:
            raise RuntimeError('.zst archive support is not available (requires zstandard.cffi)')
        file_handle = open(filename, mode=mode)
        record_parser = WarcRecord.make_parser()
        # find dictionary
        zdict = get_zstd_dictionary(file_handle)
        return ZstdRecordStream(file_handle, record_parser, zdict=zdict)
    return _open_record_stream(record_class, filename, file_handle, mode, gzip, offset, length)

from hanzo.warctools.archive_detect import register_record_type

# some ARC files are missing the filedesc record at the beginning
register_record_type(
    # pattern for ARC v1 header
    re.compile('^https?://\S+ (?:\d{1,3}\.){3}\d{1,3} \d{14} \S* \d+$'),
    ArcRecord
)

class ArchiveRecordEx(object):
    def __init__(self, reader, offset, record):
        self._reader = reader
        self.offset = offset
        self.wrapped_record = record

    RE_RESPONSE_CONTENT_TYPE = re.compile('application/http;\s*msgtype=response$', re.I)

    @property
    def compressed_record_size(self):
        # read off up to the end-of-record
        stream = self.wrapped_record.content_file
        if stream is not None:
            # TODO: define finish_record() in all ArchiveStream
            while True:
                d = stream.read(8192)
                if not d: break
        # above is enough for plain RecordStream. For GzipRecordStream
        # we need to further read up to the end of current member to get
        # correct end-of-member offset. we cannot use content_file here.
        self._reader._finish_record()
        end_offset = self._reader._stream_offset()
        return end_offset - self.offset
        #return self._reader._next_offset() - self.offset

    def is_response(self):
        """Return ``True`` if this record is WARC ``response`` record
        (i.e. currently returns ``False`` for ARC response records).
        It is determined by ``Content-Type`` in WARC header, not ``WARC-Type``.
        """
        content_type = self.content_type
        return content_type and self.RE_RESPONSE_CONTENT_TYPE.match(content_type)

    # following methods makes ArchiveRecordEx compatible with ArchiveRecord
    @property
    def type(self):
        return self.wrapped_record.type

    @property
    def url(self):
        return self.wrapped_record.url

    @property
    def date(self):
        return self.wrapped_record.date

    @property
    def content(self):
        # ArchiveRecord.content shall not be used because it breaks RecordHandler's
        # reading content_file, and loads entire record content into memory.
        raise Exception('content shall not be used')
        #return self.wrapped_record.content

    @property
    def content_file(self):
        return self.wrapped_record.content_file

    @property
    def content_type(self):
        # we cannot use ArchiveRecord.content_type because it accesses its content[0]
        # (i.e. it invalidates content_file)
        #return self.wrapped_record.content_type
        # this returns record-level content-type.
        return self.get_header(self.wrapped_record.CONTENT_TYPE)

    @property
    def content_length(self):
        # XXX ArchiveRecord.content_length resorts to content[1] if Content-Length
        # header does not exist.
        return self.wrapped_record.content_length

    def get_header(self, name):
        return self.wrapped_record.get_header(name)

    # XXX CONTENT_LENGTH has different value for WARC and ARC
    # ("Length" for WARC, "Archive-length" for ARC). It'd be beter to
    # define a common method for retrieving Record's content-length.
    @property
    def CONTENT_LENGTH(self):
        return self.wrapped_record.CONTENT_LENGTH

class ArchiveRecordReader(object):
    def __init__(self, filepath):
        self._stream = open_record_stream(None, filename=filepath, gzip="auto", mode="rb")
        self._records = iter(self._stream.read_records(limit=None, offsets=True))
        self._next_record = None

    def __iter__(self):
        return self

    def _stream_offset(self):
        if hasattr(self._stream, 'raw_fh'):
            return self._stream.raw_fh.tell()
        else:
            return self._stream.fh.tell()

    def _finish_record(self):
        if hasattr(self._stream, '_finish_record'):
            self._stream._finish_record()

    def _next_offset(self):
        if self._next_record:
            return self._next_record[0]
        while True:
            rectuple = next(self._records, None)
            if rectuple is None or (rectuple[1] or rectuple[2]):
                break
        if rectuple is None:
            self._next_record = () # end marker
            return self._stream_offset()
        self._next_record = rectuple
        return rectuple[0]

    def __next__(self):
        while True:
            if self._next_record is None:
                # raises StopIterator at the end
                self._next_record = next(self._records)
            if self._next_record:
                offset, record, errors = self._next_record
                self._next_record = None
                if record is None:
                    if errors:
                        raise ParseError(str(errors))
                    # RecordStream can return None for both record and error
                    # at the end of WARC file. safely ignored.
                    continue
                return ArchiveRecordEx(self, offset, record)
            # end marker
            raise StopIteration()

    next = __next__

    def close(self):
        self._stream.close()
