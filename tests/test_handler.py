import pytest
import mock
from cdx_writer.handler import RecordHandler
import surt

def test_record_handler_modify_original_url():
    """If `original_url` returns URL different from ``Target-URI`` in the
    record, it shall be used to generate `massaged_url` as well.
    """
    class CustomHandler(RecordHandler):
        @property
        def original_url(self):
            return 'http://web.archive.org/custom/' + self.record.url

    record = mock.Mock(url='http://example.com/')
    cdx_writer = mock.Mock(urlkey=surt.surt)

    cut = CustomHandler(record, 0, cdx_writer)

    urlkey = cut.massaged_url

    # surt canonicalizes // to / and removes trailing slash
    assert urlkey == 'org,archive,web)/custom/http:/example.com'
