import os
import re
import sys
import base64
import chardet
import hashlib
import urlparse
from datetime import datetime

def to_unicode(s, charset):
    if isinstance(s, str):
        if charset is None:
            #try utf-8 and hope for the best
            s = s.decode('utf-8', 'replace')
        else:
            try:
                s = s.decode(charset, 'replace')
            except LookupError:
                s = s.decode('utf-8', 'replace')
    return s

# these function used to be used for normalizing URL for ``redirect`` field.
def urljoin_and_normalize(base, url, charset):
    """urlparse.urljoin removes blank fragments (trailing #),
    even if allow_fragments is set to True, so do this manually.

    Also, normalize /../ and /./ in url paths.

    Finally, encode spaces in the url with %20 so that we can
    later split on whitespace.

    Usage (run doctests with  `python -m doctest -v cdx_writer.py`):
    >>> base = 'http://archive.org/a/b/'
    >>> url  = '/c/d/../e/foo'
    >>> print CDX_Writer.urljoin_and_normalize(base, url, 'utf-8')
    http://archive.org/c/e/foo

    urljoin() doesn't normalize if the url starts with a slash, and
    os.path.normalize() has many issues, so normalize using regexes

    >>> url = '/foo/./bar/#'
    >>> print CDX_Writer.urljoin_and_normalize(base, url, 'utf-8')
    http://archive.org/foo/bar/#

    >>> base = 'http://archive.org'
    >>> url = '../site'
    >>> print CDX_Writer.urljoin_and_normalize(base, url, 'utf-8')
    http://archive.org/site

    >>> base = 'http://www.seomoz.org/page-strength/http://www.example.com/'
    >>> url  = 'http://www.seomoz.org/trifecta/fetch/page/http://www.example.com/'
    >>> print CDX_Writer.urljoin_and_normalize(base, url, 'utf-8')
    http://www.seomoz.org/trifecta/fetch/page/http://www.example.com/
    """

    url  = to_unicode(url, charset)

    #the base url is from the arc/warc header, which doesn't specify a charset
    base = to_unicode(base, 'utf-8')

    try:
        joined_url = urlparse.urljoin(base, url)
    except ValueError:
        #some urls we find in arc files no longer parse with python 2.7,
        #e.g. 'http://\x93\xe0\x90E\x83f\x81[\x83^\x93\xfc\x97\xcd.com/'
        return '-'

    # We were using os.path.normpath, but had to add too many patches
    # when it was doing the wrong thing, such as turning http:// into http:/
    m = re.match('(https?://.+?/)', joined_url)
    if m:
        domain = joined_url[:m.end(1)]
        path   = joined_url[m.end(1):]
        if path.startswith('../'):
            path = path[3:]
        norm_url = domain + re.sub('/[^/]+/\.\./', '/', path)
        norm_url = re.sub('/\./', '/', norm_url)
    else:
        norm_url = joined_url

    # deal with empty query strings and empty fragments, which
    # urljoin sometimes removes
    if url.endswith('?') and not norm_url.endswith('?'):
        norm_url += '?'
    elif url.endswith('#') and not norm_url.endswith('#'):
        norm_url += '#'

    #encode spaces
    return norm_url.replace(' ', '%20')


class ParseError(Exception):
    pass

class RecordHandler(object):
    def __init__(self, record, offset, cdx_writer):
        """Defines default behavior for all fields.
        Field values are defined as properties with name
        matching descriptive name in ``field_map``.
        """
        self.record = record
        self.offset = offset
        self.cdx_writer = cdx_writer
        self.urlkey = cdx_writer.urlkey

    def get_record_header(self, name):
        return self.record.get_header(name)

    @property
    def massaged_url(self):
        """massaged url / field "N".
        """
        url = self.safe_url().encode('latin1')
        try:
            return self.urlkey(url)
        except:
            return self.original_url

    @property
    def date(self):
        """date / field "b".
        """
        # warcs and arcs use a different date format
        # consider using dateutil.parser instead
        record = self.record
        if record.date is None:
            # TODO: in strict mode, this shall be a fatal error.
            return None
        elif record.date.isdigit():
            date_len = len(record.date)
            if 14 == date_len:
                #arc record already has date in the format we need
                return record.date
            elif 14 < date_len and date_len <= 18:
                #some arc records have 15-digit dates: 201512000000000
                #some arc records have 16-digit dates: 2000082305410049
                #some arc records have 18-digit dates: 200009180023002953
                return record.date[:14]
            elif 12 == date_len:
                #some arc records have 12-digit dates: 200011201434
                return record.date + '00'
            elif 10 == date_len:
                #some arc records have 10-digit dates: 2016020900
                return record.date + '0000'
        elif re.match('[a-f0-9]+$', record.date):
            #some arc records have a hex string in the date field
            return None
        elif re.match('[0-9]{14,18}[a-zA-Z]+$', record.date):
            #some arc records are like this: 20160211000000jpg
            return record.date[:14]

        #warc record
        date = datetime.strptime(record.date, "%Y-%m-%dT%H:%M:%SZ")
        return date.strftime("%Y%m%d%H%M%S")

    def safe_url(self):
        url = self.record.url
        # There are few arc files from 2002 that have non-ascii characters in
        # the url field. These are not utf-8 characters, and the charset of the
        # page might not be specified, so use chardet to try and make these usable.
        if isinstance(url, bytes):
            url = url.decode('latin1')

        # due to a descrepancy in WARC 1.0 specification, certain versions of
        # wget put < > around WARC-Target-URI value.
        if url[:1] == '<' and url[-1:] == '>':
            url = url[1:-1]

        # Some arc headers contain urls with the '\r' character, which will cause
        # problems downstream when trying to process this url, Browsers typially
        # remove '\r'. So do we.
        url = url.replace('\r', '')
        # %-encode other white spaces that can cuase CDX parsing problems
        def percent_hex(m):
            return "%{:02X}".format(ord(m.group(0)))
        url = re.sub(r'[ \r\n\x0c\x08]', percent_hex, url)

        return url

    @property
    def original_url(self):
        """original url / field "a".
        """
        url = self.safe_url()
        return url.encode('latin1')

    @property
    def mime_type(self):
        """mime type / field "m".
        """
        return 'warc/' + self.record.type

    @property
    def response_code(self):
        """response code / field "s".
        """
        return None

    @property
    def new_style_checksum(self):
        """new style checksum / field "k".
        """
        h = hashlib.sha1(self.record.content[1])
        return base64.b32encode(h.digest())

    @property
    def redirect(self):
        """redirect / field "r".
        """
        # only meaningful for HTTP response records.
        return None

    @property
    def compressed_record_size(self):
        """compressed record size / field "S".
        """
        size = self.record.compressed_record_size
        if size is None:
            return None
        return str(size)

    @property
    def compressed_arc_file_offset(self):
        """compressed arc file offset / field "V".
        """
        # TODO: offset attribute
        return str(self.offset)

    @property
    def aif_meta_tags(self):
        """AIF meta tags / field "M".
        robot metatags, if present, should be in this order:
        A, F, I. Called "robotsflags" in Wayback.
        """
        return None

    @property
    def file_name(self):
        """file name / field "g".
        """
        return self.cdx_writer.warc_path

class WarcinfoHandler(RecordHandler):
    """``wercinfo`` record handler."""
    #similar to what what the wayback uses:
    fake_build_version = "archive-commons.0.0.1-SNAPSHOT-20120112102659-python"

    @property
    def massaged_url(self):
        return self.original_url

    @property
    def original_url(self):
        return 'warcinfo:/%s/%s' % (
            self.cdx_writer.file, self.fake_build_version
        )

    @property
    def mime_type(self):
        return 'warc-info'

class HttpHandler(RecordHandler):
    """Logic common to all HTTP response records
    (``response`` and ``revisit`` record types).
    """
    meta_tags = None

    @property
    def redirect(self):
        # Aaron, Ilya, and Kenji have proposed using '-' in the redirect column
        # unconditionally, after a discussion on Sept 5, 2012. It turns out the
        # redirect column of the cdx has no effect on the Wayback Machine, and
        # there were issues with parsing unescaped characters found in redirects.
        return None

        # followig code is copied from old version before refactoring. it will
        # not work with new structure.

        # response_code = self.response_code
        #
        # ## It turns out that the refresh tag is being used in both 2xx and 3xx
        # ## responses, so always check both the http location header and the meta
        # ## tags. Also, the java version passes spaces through to the cdx file,
        # ## which might break tools that split cdx lines on whitespace.
        #
        # #only deal with 2xx and 3xx responses:
        # #if 3 != len(response_code):
        # #    return '-'
        #
        # charset = self.parse_charset()
        #
        # #if response_code.startswith('3'):
        # location = self.parse_http_header('location')
        # if location:
        #     return self.urljoin_and_normalize(record.url, location, charset)
        # #elif response_code.startswith('2'):
        # if self.meta_tags and 'refresh' in self.meta_tags:
        #     redir_loc = self.meta_tags['refresh']
        #     m = re.search('\d+\s*;\s*url=(.+)', redir_loc, re.I) #url might be capitalized
        #     if m:
        #         return self.urljoin_and_normalize(record.url, m.group(1), charset)
        #
        # return '-'

    def parse_charset(self):
        charset = None

        content_type = self.parse_http_header('content-type')
        if content_type:
            m = self.charset_pattern.search(content_type)
            if m:
                charset = m.group(1)

        if charset is None and self.meta_tags is not None:
            content_type = self.meta_tags.get('content-type')
            if content_type:
                m = self.charset_pattern.search(content_type)
                if m:
                    charset = m.group(1)

        if charset:
            charset = charset.replace('win-', 'windows-')

        return charset

class ResponseHandler(HttpHandler):
    """Handler for HTTP response with archived content (``response`` record type).
    """
    def __init__(self, record, offset, cdx_writer):
        super(ResponseHandler, self).__init__(record, offset, cdx_writer)
        self.lxml_parse_limit = cdx_writer.lxml_parse_limit
        self.headers, self.content = self.parse_headers_and_content()
        self.meta_tags = self.parse_meta_tags()

    response_pattern = re.compile('application/http;\s*msgtype=response$', re.I)

    def parse_http_header(self, header_name):
        if self.headers is None:
            return None

        pattern = re.compile(header_name+':\s*(.+)', re.I)
        for line in iter(self.headers):
            m = pattern.match(line)
            if m:
                return m.group(1)
        return None

    def parse_http_content_type_header(self):
        content_type = self.parse_http_header('content-type')
        if content_type is None:
            return 'unk'

        # some http responses end abruptly: ...Content-Length: 0\r\nConnection: close\r\nContent-Type: \r\n\r\n\r\n\r\n'
        content_type = content_type.strip()
        if '' == content_type:
            return 'unk'

        m = re.match('(.+?);', content_type)
        if m:
            content_type = m.group(1)

        if re.match('[a-z0-9\-\.\+/]+$', content_type):
            return content_type
        else:
            return 'unk'

    charset_pattern = re.compile('charset\s*=\s*([a-z0-9_\-]+)', re.I)

    crlf_pattern = re.compile('\r?\n\r?\n')

    def parse_headers_and_content(self):
        """Returns a list of header lines, split with splitlines(), and the content.
        We call splitlines() here so we only split once, and so \r\n and \n are
        split in the same way.
        """
        if self.record.content[1].startswith('HTTP'):
            # some records with empty HTTP payload end with just one CRLF or
            # LF. If split fails, we assume this situation, and let content be
            # an empty bytes, rather than None, so that payload digest is
            # emitted correctly (see get_new_style_checksum method).
            try:
                headers, content = self.crlf_pattern.split(self.record.content[1], 1)
            except ValueError:
                headers = self.record.content[1]
                content = ''
            return headers.splitlines(), content
        else:
            return None, None

    def is_response(self):
        content_type = self.record.content_type
        return content_type and self.response_pattern.match(content_type)

    @property
    def mime_type(self):
        if self.is_response():
            # WARC
            return self.parse_http_content_type_header()

        # For ARC record content_type returns response content type from
        # ARC header line.
        content_type = self.record.content_type
        if content_type is None:
            return 'unk'

        # Alexa arc files use 'no-type' instead of 'unk'
        if content_type == 'no-type':
            return 'unk'
        # if content_type contains non-ascii chars, return 'unk'
        try:
            content_type.decode('ascii')
        except (LookupError, UnicodeDecodeError):
            content_type = 'unk'
        return content_type

    RE_RESPONSE_LINE = re.compile(
        r'HTTP(?P<version>/\d\.\d)? (?P<statuscode>\d+)')

    @property
    def response_code(self):
        m = self.RE_RESPONSE_LINE.match(self.record.content[1])
        return m and m.group('statuscode')

    @property
    def new_style_checksum(self):
        if self.is_response():
            digest = self.get_record_header('WARC-Payload-Digest')
            return digest.replace('sha1:', '')
        elif self.content is not None:
            # This is an arc record. Our patched warctools fabricates the WARC-Payload-Digest
            # header even for arc files so that we don't need to load large payloads in memory
            digest = self.get_record_header('WARC-Payload-Digest')
            if digest is not None:
                return digest.replace('sha1:', '')
            else:
                h = hashlib.sha1(self.content)
                return base64.b32encode(h.digest())
        else:
            h = hashlib.sha1(self.record.content[1])
            return base64.b32encode(h.digest())

    def parse_meta_tags(self):
        """We want to parse meta tags in <head>, even if not direct children.
        e.g. <head><noscript><meta .../></noscript></head>

        What should we do about multiple meta tags with the same name?
        currently, we append the content attribs together with a comma seperator.

        We use either the 'name' or 'http-equiv' attrib as the meta_tag dict key.
        """

        if self.mime_type != 'text/html':
            return None

        if self.content is None:
            return None

        meta_tags = {}

        #lxml.html can't parse blank documents
        html_str = self.content.strip()
        if '' == html_str:
            return meta_tags

        #lxml can't handle large documents
        if self.record.content_length > self.lxml_parse_limit:
            return meta_tags

        # lxml was working great with ubuntu 10.04 / python 2.6
        # On ubuntu 11.10 / python 2.7, lxml exhausts memory hits the ulimit
        # on the same warc files. Unfortunately, we don't ship a virtualenv,
        # so we're going to give up on lxml and use regexes to parse html :(

        for x in re.finditer("(<meta[^>]+?>|</head>)", html_str, re.I):
            #we only want to look for meta tags that occur before the </head> tag
            if x.group(1).lower() == '</head>':
                break
            name = None
            content = None

            m = re.search(r'''\b(?:name|http-equiv)\s*=\s*(['"]?)(.*?)(\1)[\s/>]''', x.group(1), re.I)
            if m:
                name = m.group(2).lower()
            else:
                continue

            m = re.search(r'''\bcontent\s*=\s*(['"]?)(.*?)(\1)[\s/>]''', x.group(1), re.I)
            if m:
                content = m.group(2)
            else:
                continue

            if name not in meta_tags:
                meta_tags[name] = content
            else:
                if 'refresh' != name:
                    #for redirect urls, we only want the first refresh tag
                    meta_tags[name] += ',' + content

        return meta_tags

    @property
    def aif_meta_tags(self):
        x_robots_tag = self.parse_http_header('x-robots-tag')

        robot_tags = []
        if self.meta_tags and 'robots' in self.meta_tags:
            robot_tags += self.meta_tags['robots'].split(',')
        if x_robots_tag:
            robot_tags += x_robots_tag.split(',')
        robot_tags = [x.strip().lower() for x in robot_tags]

        s = ''
        if 'noarchive' in robot_tags:
            s += 'A'
        if 'nofollow' in robot_tags:
            s += 'F'
        if 'noindex' in robot_tags:
            s += 'I'

        # IA-proprietary extension 'P' flag for password protected pages.
        # crawler adds special header to WARC record, whose value consists
        # of three values separated by comma. The first value is a number
        # of attempted logins (so >0 value means captured with login).
        # Example: ``1,1,http://(com,example,)/``
        sfps = self.get_record_header('WARC-Simple-Form-Province-Status')
        if sfps:
            sfps = sfps.split(',', 2)
            try:
                if int(sfps[0]) > 0:
                    s += 'P'
            except ValueError as ex:
                pass

        return ''.join(s) if s else None

class ResourceHandler(RecordHandler):
    """HTTP resource record (``resource`` record type).
    """
    @property
    def mime_type(self):
        return self.record.content[0]

class RevisitHandler(HttpHandler):
    """HTTP revisit record (``revisit`` record type).

    Note that this handler does not override ``mime_type``.
    Hence ``mime_type`` field will always be ``warc/revisit``.
    """
    @property
    def new_style_checksum(self):
        digest = self.get_record_header('WARC-Payload-Digest')
        if digest is None:
            return None
        return digest.replace('sha1:', '')

class FtpHandler(RecordHandler):
    @property
    def mime_type(self):
        return self.record.content[0]

    @property
    def response_code(self):
        """Always return 226 assuming all ftp captures are successful ones.
        Code 226 represents successful completion of file action, and it is
        what org.apache.commons.net.ftp.FTPClient#getReplyCode() (used by
        Heritrix) returns upon successful download.

        Ref. https://en.wikipedia.org/wiki/List_of_FTP_server_return_codes
        """
        return '226'

    @property
    def new_style_checksum(self):
        # For "resource" record, block is also a payload. So
        # Both WARC-Payload-Digest and WARC-Block-Digest is valid.
        # wget uses Block. Heritirx uses Payload.
        digest = self.get_record_header('WARC-Payload-Digest')
        if digest:
            return digest.replace('sha1:', '')
        digest = self.get_record_header('WARC-Block-Digest')
        if digest:
            return digest.replace('sha1:', '')

        h = hashlib.sha1(self.record.content[1])
        return base64.b32encode(h.digest())
