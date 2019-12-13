import io
from hanzo.warctools.stream import RecordStream
# requires cffi version of zstandard for calculating source offset
#import zstandard as zstd
import zstandard.cffi as zstd

class ZstdRecordStream(RecordStream):
    def __init__(self, file_handle, record_parser):
        """A stream to read/write archive record in individual zstandard-compressed frames.
        """
        dict_data = b''
        cdict = zstd.ZstdCompressionDict(dict_data)
        self.dctx = zstd.ZstdDecompressor(dict_data=cdict)
        self.raw_fh = file_handle
        RecordStream.__init__(self, self._decompress_reader(), record_parser)

    def _decompress_reader(self):
        fileobj = self.raw_fh
        self.reader = self.dctx.stream_reader(fileobj, read_across_frames=False)
        self.member_offset = self.raw_fh.tell()
        # reader does not implement readline() (raises io.UnsupportedOperation if called) needed
        # for RecordStream. so we wrap it with io.BufferedReader.
        breader = io.BufferedReader(self.reader)
        return breader

    def _finish_record(self):
        while True:
            d = self.fh.read(1024)
            if not d: break
        self.bytes_to_eoc = None
        # ArchiveReaderEx relies on raw_fh being at the end of current frame
        # after _finish_record(). Seek back to the beginninig of the next frame,
        # which zstandard very likely has read past.
        overread = self.reader._in_buffer.size - self.reader._in_buffer.pos
        self.raw_fh.seek(-overread, 1)

    def _read_record(self, offsets):
        if self.bytes_to_eoc is not None:
            self._finish_record()
        # this would prevent BufferedReader from cascading close to underlining
        # zstd stream reader, but it is unnecessary since it is single use.
        #if self.fh and hasattr(self.fh, 'detach'):
        #    self.fh.detach()
        self.fh = self._decompress_reader()
        line = None
        record, errors, _offset = self.record_parser.parse(self, offset=None, line=line)
        offset = self.member_offset
        return offset, record, errors

    def seek(self, offset, pos=0):
        # untested
        self.raw_fh.seek(offset, pos)
        self.fh = self._decompress_reader()


