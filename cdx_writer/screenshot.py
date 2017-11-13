from .handler import RecordHandler
from .dispatcher import RecordDispatcher

class ScreenshotHandler(RecordHandler):
    @property
    def original_url(self):
        return 'http://web.archive.org/screenshot/' + self.safe_url()

    @property
    def mime_type(self):
        return self.record.content[0]

class ScreenshotDispatcher(RecordDispatcher):
    def dispatch_metadata(self, record):
        content_type = record.content_type
        if content_type and content_type.startswith('image/'):
            return ScreenshotHandler
        return None
