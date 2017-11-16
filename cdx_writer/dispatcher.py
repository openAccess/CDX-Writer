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
    # handlers are resolved through these attributes so that sub-class can
    # easily substitute handler classes.
    response_handler = ResponseHandler
    revisit_handler = RevisitHandler
    ftp_handler = FtpHandler
    resource_handler = ResourceHandler

    def response_code(self, record):
        """Return response status code from HTTP response line.
        Only valid for ``response`` and (new style) ``revisit`` records.
        Note return value is binary string, not int.
        """
        m = ResponseHandler.RE_RESPONSE_LINE.match(record.content[1])
        return m and m.group(1)

    def dispatch_response(self, record):
        # probbaly it's better to test for "dns:" scheme?
        if record.content_type in ('text/dns',):
            return None
        # exclude 304 Not Modified responses (unless --all-records)
        if self.response_code(record) == '304':
            return None
        return self.response_handler

    def is_server_not_modified(self, record):
        warc_profile = record.get_header('WARC-Profile')
        return warc_profile and warc_profile.endswith('/revisit/server-not-modified')

    def dispatch_revisit(self, record):
        # exclude 304 Not Modified revisits (unless --all-records)
        if self.is_server_not_modified(record):
            return None
        return self.revisit_handler

    def dispatch_resource(self, record):
        # wget saves resource records with wget agument and logging
        # output at the end of the WARC. those need to be skipped.
        if record.url.startswith('ftp://'):
            return self.ftp_handler
        elif record.url.startswith(('http://', 'https://')):
            return self.resource_handler
        return None

class AllDispatcher(DefaultDispatcher):
    warcinfo_handler = WarcinfoHandler

    def dispatch_response(self, record):
        return self.response_handler

    def dispatch_revisit(self, record):
        return self.revisit_handler

    def dispatch_resource(self, record):
        disp = super(AllDispatcher, self).dispatch_resource(record)
        return disp or RecordHandler

    def dispatch_warcinfo(self, record):
        return self.warcinfo_handler

    def dispatch_any(self, record):
        return RecordHandler
