import io
from hanzo.warctools.stream import RecordStream
# requires cffi version of zstandard for calculating source offset
#import zstandard as zstd
import zstandard.cffi as zstd

class ZstdRecordStream(RecordStream):
    def __init__(self, file_handle, record_parser, zdict=b''):
        """A stream to read/write archive record in individual zstandard-compressed frames.
        """
        if zdict is None:
            zdict = b''
        if isinstance(zdict, bytes):
            zdict = zstd.ZstdCompressionDict(zdict)
        self.dctx = zstd.ZstdDecompressor(dict_data=zdict)
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

# zstd.get_frame_parameters(data) returns FrameParameters object, which does not copy
# frameType from raw ZSTD_frameHeader struct.
# TODO: propose addtion of frame_type attribute.
class FrameParametersEx(zstd.FrameParameters):
    def __init__(self, fparams):
        zstd.FrameParameters.__init__(self, fparams)
        self.frame_type = fparams.frameType
        # headerSize is always 0 for skippable frames. no use to copy.
        #self.header_size = fparams.headerSize

def _get_frame_parameters(data):
    params = zstd.ffi.new('ZSTD_frameHeader *')

    data_buffer = zstd.ffi.from_buffer(data)
    zresult = zstd.lib.ZSTD_getFrameHeader(params, data_buffer, len(data_buffer))
    if zstd.lib.ZSTD_isError(zresult):
        raise zstd.ZstdError('cannot get frame parameters: %s' %
                        _zstd_error(zresult))

    if zresult:
        raise zstd.ZstdError('not enough data for frame parameters; need %d bytes' %
                        zresult)

    return FrameParametersEx(params[0])

def get_zstd_dictionary(fobj):
    # method 1: the first skippable frame
    # frame header is 2 to 14 bytes.
    if  hasattr(fobj, 'peek'):
        data = fobj.peek(4 + 14)
    else:
        data = fobj.read(4 + 14)
        fobj.seek(-len(data), 1)
    try:
        frame_params = _get_frame_parameters(data)
        # dictionary frame must meet following conditions:
        # * it is a skippable frame (frame_type == 1)
        # * it has frame_content_size > 0
        # * it does not have dict
        # dictionary frame must not have dictionary
        if frame_params.frame_type == 1 and frame_params.dict_id == 0:
            content_size = frame_params.content_size
            if content_size != zstd.lib.ZSTD_CONTENTSIZE_UNKNOWN:
                # getFrameHeader() does not set headerSize. Assume fixed length 8
                fobj.seek(8, 1)
                zdict = fobj.read(content_size)
                assert len(zdict) == content_size
                if frame_params.has_checksum:
                    fobj.seek(4, 1)
                # TODO: zdict could be zstd-compressed.
                magic = zdict[:4]
                if magic == b'\x37\xa4\x30\xec':
                    return zdict
        return b''
    except zstd.ZstdError:
        return b''



