from .handler import (RecordHandler, ResponseHandler, RevisitHandler,
                      ResourceHandler, FtpHandler, WarcinfoHandler)

__all__ = [
    'RecordDispatcher', 'DefaultDispatcher', 'AllDispatcher'
]

class RecordDispatcher(object):
    _cache = None
    def dispatch(self, record, offset, cdx_writer):
        record_type = record.type
        if self._cache is None:
            self._cache = {}
        if record_type in self._cache:
            disp = self._cache[record_type]
        else:
            attr = "dispatch_{}".format(record_type)
            disp = getattr(self, attr, None)
            if disp is None:
                disp = getattr(self, "dispatch_any", None)
            self._cache[record_type] = disp
        if disp:
            handler_factory = disp(record)
            if handler_factory:
                return handler_factory(record, offset=offset, cdx_writer=cdx_writer)
        return None

class DefaultDispatcher(RecordDispatcher):
    def dispatch_response(self, record):
        # probbaly it's better to test for "dns:" scheme?
        if record.content_type in ('text/dns',):
            return None
        # exclude 304 Not Modified responses (unless --all-records)
        m = ResponseHandler.RE_RESPONSE_LINE.match(record.content[1])
        if m and m.group('statuscode') == '304':
            return None
        # discard ARC records for failed liveweb proxy
        ipaddr = record.get_header('IP-address')
        if ipaddr == '0.0.0.0':
            # some ARcs have valid captures with IP-Address=0.0.0.0
            # we need to check further; no HTTP version and 50{2,4}
            # status.
            if (m and m.group('version') is None and
                m.group('statuscode') in ('502', '504')):
                return None
        return ResponseHandler

    def dispatch_revisit(self, record):
        # exclude 304 Not Modified revisits (unless --all-records)
        if record.get_header('WARC-Profile') and record.get_header(
                'WARC-Profile').endswith('/revisit/server-not-modified'):
            return None
        return RevisitHandler

    def dispatch_resource(self, record):
        # wget saves resource records with wget agument and logging
        # output at the end of the WARC. those need to be skipped.
        if record.url.startswith('ftp://'):
            return FtpHandler
        elif record.url.startswith(('http://', 'https://')):
            return ResourceHandler
        return None

class AllDispatcher(DefaultDispatcher):

    def dispatch_response(self, record):
        return ResponseHandler

    def dispatch_revisit(self, record):
        return RevisitHandler

    def dispatch_resource(self, record):
        disp = super(AllDispatcher, self).dispatch_resource(record)
        return disp or RecordHandler

    def dispatch_warcinfo(self, record):
        return WarcinfoHandler

    def dispatch_any(self, record):
        return RecordHandler
