import io
import random
import struct
import pytest

from hanzo.warctools.warc import WarcRecord
try:
    from cdx_writer.zstdstream import ZstdRecordStream, get_zstd_dictionary
    import zstandard.cffi as zstd
except ImportError:
    pytestmark = pytest.mark.skip(
        "skipping zstandard tests as zstandard.cffi is unavailable")


def random_content():
    n = random.randint(200, 1000)
    return bytearray(random.randint(0,255) for i in range(n))

def write_warc_record(z, headers, content):
    assert isinstance(content, (bytes, bytearray))
    z.write(b'WARC/1.0\n')
    for n, v in headers:
        z.write(n + b': ' + v + b'\r\n')
    z.write('Content-Length: {}\r\n'.format(len(content)).encode('ascii'))
    z.write(b'\r\n')
    z.write(content)
    z.write(b'\r\n\r\n')

def build_warc_record(headers, content):
    b = io.BytesIO()
    write_warc_record(b, headers, content)
    return b.getvalue()


def test_offsets(tmpdir):
    """Test ZstdRecordStream returns correct offsets.
    """
    NRECORDS = 10
    warc = tmpdir / 'a.warc.zst'
    cctx = zstd.ZstdCompressor(level=10)
    expected_offsets = []
    with warc.open('wb') as w:
        for ir in range(NRECORDS):
            expected_offsets.append(w.tell())
            content = random_content()
            rec = build_warc_record([(b'WARC-Type', b'metadata')], content)
            w.write(cctx.compress(rec))
    
    with warc.open('rb') as f:
       s = ZstdRecordStream(f, WarcRecord.make_parser())
       offsets = []
       for offset, record, errors in s.read_records(limit=None, offsets=True):
           if record:
               offsets.append(offset)

    assert offsets == expected_offsets


def test_get_zstd_dictionary(tmpdir):
    """test of discovering dictionary for the archive.
    """
    NRECORDS = 10
    records = [build_warc_record([(b'WARC-Type', b'metadata')], random_content()) for i in range(NRECORDS)]
    zdict = zstd.train_dictionary(16*1024, records)
    zdict_bytes = zdict.as_bytes()

    warc = tmpdir / 'b.warc.gz'
    cctx = zstd.ZstdCompressor(level=10, dict_data=zdict)

    with warc.open('wb') as w:
        # write out dictionary in a skippable frame at the beginning
        # skippable frame can have any ID in [0x184D2A50, 0x184D2A5F] range
        frame_header = struct.pack('<LL', 0x184D2A5D, len(zdict_bytes))
        w.write(frame_header)
        w.write(zdict_bytes)
        
        for rec in records:
            w.write(cctx.compress(rec))

    with warc.open('rb') as f:
        zdict = get_zstd_dictionary(f)

        s = ZstdRecordStream(f, WarcRecord.make_parser(), zdict=zdict)
        offsets = []
        for offset, record, errors in s.read_records(limit=None, offsets=True):
            if record:
                offsets.append(offset)
        
        assert len(offsets) == NRECORDS
