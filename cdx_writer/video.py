from .handler import RecordHandler
from .dispatcher import DefaultDispatcher

class VideoMetaHandler(RecordHandler):
    @property
    def original_url(self):
        return 'http://wayback-metadata.archive.org/video-meta/' + self.safe_url()

    @property
    def massaged_url(self):
        return 'org,archive,wayback-metadata)/video-meta/' + self.urlkey(self.safe_url())

    @property
    def mime_type(self):
        return self._normalize_content_type('application/json;generator-youtube-dl')

class ArchivedVideoHandler(RecordHandler):
    @property
    def original_url(self):
        return 'http://wayback-metadata.archive.org/video-meta/' + self.safe_url()

    @property
    def massaged_url(self):
        return 'org,archive,wayback-metadata)/archived-video/' + self.urlkey(self.safe_url())

    @property
    def mime_type(self):
        return self._normalize_content_type('application/json;generator=yt-archiver')

class VideoDispatcher(DefaultDispatcher):
    def dispatch_metadata(self, record, env):
        content_type = record.content_type

        if content_type and content_type.startswith('application/json;generator=yt-archiver'):
            return ArchivedVideoHandler
        return None
