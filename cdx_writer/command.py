import sys
import os
import json
from operator import attrgetter
from optparse import OptionParser
from pkg_resources import iter_entry_points

from hanzo.warctools import ArchiveRecord
from surt import surt

from .dispatcher import DefaultDispatcher, AllDispatcher
from .exclusion import PrefixExclusion

class Mode(object):
    OPTIONS = {
        'format': "N b a m s k r M S V g",
        'use_full_path': False,
        'file_prefix': None,
        'dispatch_mode': 'default',
        'exclude_list': None,
        'canonicalizer_options': []
    }

    def __init__(self, dispatcher=None, option_name=None, option_help=None, **params):
        self.dispatcher = dispatcher or DefaultDispatcher()
        self.option_name = option_name
        self.option_help = option_help
        self.options = dict(self.OPTIONS, **params)

class CDXWriterConfig(object):
    _modes = {
        'all': Mode(dispatcher=AllDispatcher())
    }
    _fields = {
        'M': 'AIF meta tags',
        'N': 'massaged url',
        'S': 'compressed record size',
        'V': 'compressed arc file offset',
        'a': 'original url',
        'b': 'date',
        'g': 'file name',
        'k': 'new style checksum',
        'm': 'mime type',
        'r': 'redirect',
        's': 'response code',
    }

    @classmethod
    def define_mode(cls, name, dispatcher=None, option_name=None, option_help=None,
                    format=None):
        cls._modes[name] = Mode(dispatcher=dispatcher,
                                option_name=option_name,
                                option_help=option_help,
                                format=format)

    @classmethod
    def set_option_default(cls, name, value):
        cls._option_defaults[name] = value

    @classmethod
    def add_field(cls, label, name):
        cls._fields[label] = name

class CDX_Writer(object):
    def __init__(self, config, mode, in_file,
                 format="N b a m s k r M S V g",
                 warc_path=None,
                 exclude_list=None, canonicalizer=None):
        """This class is instantiated for each web archive file and generates
        CDX from it.

        :param out_file: file object to write CDX to
        :param format: CDX field specification string.
        :param warc_path: filename field value (literal or callable taking `file` as an argument)
        :param dispatch_mode: string representing dispatchers to use
        :param exclude_list: a file containing a list of excluded URLs
        :param canonicalizer_options: URL canonicalizer options
        """
        self.config = config
        self.in_file = in_file

        self.field_map = config._fields

        self.format = format

        self.fieldgetter = self._build_fieldgetter(self.format.split())

        self.dispatcher = mode.dispatcher

        self.urlkey = canonicalizer or surt

        #Large html files cause lxml to segfault
        #problematic file was 154MB, we'll stop at 5MB
        self.lxml_parse_limit = 5 * 1024 * 1024

        self.warc_path = warc_path(in_file) if warc_path else in_file

        self.exclusion = exclude_list or False

    def _build_fieldgetter(self, fieldcodes):
        """Return a callable that collects CDX field values from a
        :class:`RecordHandler` object, according to CDX field specification
        `fieldcodes`.

        :param fieldcodes: a list of single-letter CDX field codes.
        """
        attrs = []
        for field in fieldcodes:
            if field not in self.field_map:
                raise ParseError('unknown field; {}'.format(field))
            attrs.append(self.field_map[field].replace(' ', '_').lower())
        return attrgetter(*attrs)

    def urlkey(self):
        # replaced in __init__
        pass

    def should_exclude(self, urlkey):
        return self.exclusion and self.exclusion.excluded(urlkey)

    def make_cdx(self, out_file):
        """
        :param in_file: input web archive file name
        """
        self.stats = {
            'num_records_processed': 0,
            'num_records_included': 0,
            'num_records_filtered': 0,
        }
        if hasattr(out_file, "write"):
            self._make_cdx(out_file, self.stats)
        else:
            with open(out_file, "wb") as w:
                self._make_cdx(w, self.stats)

    def _make_cdx(self, out_file, stats):

        out_file.write(b' CDX ' + self.format + b'\n') #print header

        fh = ArchiveRecord.open_archive(self.in_file, gzip="auto", mode="r")
        for (offset, record, errors) in fh.read_records(limit=None, offsets=True):
            if not record:
                if errors:
                    raise ParseError(str(errors))
                continue # tail

            stats['num_records_processed'] += 1
            handler = self.dispatcher.dispatch(record, offset, self)
            if not handler:
                continue

            ### arc files from the live web proxy can have a negative content length and a missing payload
            ### check the content_length from the arc header, not the computed payload size returned by record.content_length
            content_length_str = record.get_header(record.CONTENT_LENGTH)
            if content_length_str is not None and int(content_length_str) < 0:
                continue

            surt = handler.massaged_url
            if self.should_exclude(surt):
                stats['num_records_filtered'] += 1
                continue

            ### precalculated data that is used multiple times
            # self.headers, self.content = self.parse_headers_and_content(record)
            # self.mime_type             = self.get_mime_type(record, use_precalculated_value=False)

            values = [b'-' if v is None else v for v in self.fieldgetter(handler)]
            out_file.write(b' '.join(values) + b'\n')
            #record.dump()
            stats['num_records_included'] += 1

        fh.close()

def main(args=None):

    parser = OptionParser(usage="%prog [options] warc.gz [output_file.cdx]")

    parser.add_option("--format",  dest="format", help="A space-separated list of fields [default: '%default']")
    parser.add_option("--use-full-path", dest="use_full_path", action="store_true", help="Use the full path of the warc file in the 'g' field")
    parser.add_option("--file-prefix",   dest="file_prefix", help="Path prefix for warc file name in the 'g' field."
                      " Useful if you are going to relocate the warc.gz file after processing it."
                     )
    parser.add_option("--all-records", dest="mode", action="store_const", const="all",
                      help="By default cdx_writer only indexes http responses. "
                      "Use this flag to index all WARC records in the file")
    parser.add_option("--exclude-list", dest="exclude_list", help="File containing url prefixes to exclude")
    parser.add_option("--stats-file", dest="stats_file", help="Output json file containing statistics")
    parser.add_option("--no-host-massage", dest="canonicalizer_options",
                      action='append_const', const=('host_massage', False),
                      help='Turn off host_massage (ex. stripping "www.")')

    config = CDXWriterConfig

    entry_points = iter_entry_points("CDX_Writer", "config")
    for ep in entry_points:
        ep.load()(config)

    for mode_name, mode in config._modes.items():
        if mode_name != 'default' and mode.option_name:
            parser.add_option('--' + mode.option_name, action='store_const',
                              const=mode_name, dest='mode',
                              help=mode.option_help)
    default_mode = config._modes.get('default') or Mode()
    parser.set_defaults(**default_mode.options)

    if args is None:
        args = sys.argv[1:]
    options, input_files = parser.parse_args(args=args)

    if len(input_files) != 2:
        if len(input_files) == 1:
            input_files.append(sys.stdout)
        else:
            parser.print_help()
            return -1

    if options.use_full_path:
        warc_path = lambda fn: os.path.abspath(fn)
    elif options.file_prefix:
        warc_path = lambda fn: os.path.join(file_prefix, fn)
    else:
        warc_path = None

    def canonicalizer(url, options=dict(options.canonicalizer_options)):
        return surt(url, **options)

    exclude_list = options.exclude_list and PrefixExclusion(
            options.exclude_list, canonicalizer)

    cdx_writer = CDX_Writer(config=config,
                            mode=config._modes.get(options.mode, default_mode),
                            in_file=input_files[0],
                            format=options.format,
                            warc_path=warc_path,
                            exclude_list=exclude_list,
                            canonicalizer=canonicalizer
                           )
    try:
        cdx_writer.make_cdx(input_files[1])
    finally:
        if options.stats_file is not None:
            with open(options.stats_file, 'w') as f:
                json.dump(cdx_writer.stats, f, indent=4)
    return 0
