
from imgtr.staging import Staging
from imgtr.tardis import TardisServer
from imgtr.utils import safe_name
from imgtr.utils import create_tmpdir
import tempfile
import pathlib
import configparser
import multiprocessing
import logging

logger = logging.getLogger(__name__)


class Job:
    # Default path for config file
    DEFAULT_CONFIG = pathlib.Path.home()/'imagetrove/imagetrove.ini'
    # Default root path where tmpdir will be created
    DEFAULT_TMPROOT = pathlib.Path(tempfile.gettempdir())
    # Default number of cores
    DEFAULT_CORES = 1

    def __init__(self, indir, config=None):
        # Essential parameters
        self.indir = pathlib.Path(indir).resolve(strict=True)
        self.name = safe_name(self.indir.name)

        # Config file
        config = config if config else self.DEFAULT_CONFIG
        self.config = pathlib.Path(config).resolve(strict=True)

        self.tmproot = self.DEFAULT_TMPROOT
        self.cores = self.DEFAULT_CORES
        self.cfg = self.parse_config()
        self.tmpdir = None
        self.tmphandle = None
        self.server = None
        self.staging = None

        # Tardis objects
        self.instrument = None
        self.experiment = None
        self.dataset = None

        self.storagebox = {}

    def __str__(self):
        return self.name

    @property
    def cores(self):
        return self._cores

    @cores.setter
    def cores(self, cores):
        cores = int(cores)
        maxcores = multiprocessing.cpu_count()
        if cores > maxcores:
            cores = maxcores
        elif cores < 1:
            cores = 1
        self._cores = cores

    def server_from_cfg(self):
        url = self.cfg.get('Server', 'Url')
        user = self.cfg.get('Server', 'User')
        apikey = self.cfg.get('Server', 'ApiKey')
        institution = self.cfg.get('Server', 'Institution')
        curl = False
        if self.cfg.has_section('Staging'):
            if self.cfg.has_option('Staging', 'curl'):
                curl = eval(self.cfg.get('Staging', 'curl'))
                pass
        self.server = TardisServer(url=url, user=user, apikey=apikey, institution=institution, curl=curl)
        logging.info('Tardis server at %s' % self.server.url)

    def staging_from_cfg(self):
        if self.cfg.has_section('Staging'):
            user = self.cfg.get('Staging', 'user')
            host = self.cfg.get('Staging', 'host')
            port = self.cfg.get('Staging', 'port')
            key = self.cfg.get('Staging', 'key')
            self.staging = Staging(user=user, host=host, port=port, key=key)
            logging.info('Staging at %s' % self.staging.host)
        else:
            self.staging = Staging()

    def make_tmpdir(self):
        self.tmpdir, self.tmphandle = create_tmpdir(self.tmproot)

    def config_optionals(self):
        if self.cfg.has_option('Client', 'tmproot'):
            self.tmproot = self.cfg.get('Client', 'tmproot')
            self.tmproot = pathlib.Path(self.tmproot).resolve(strict=True)
        if self.cfg.has_option('Client', 'cores'):
            self.cores = self.cfg.get('Client', 'cores')

    def args_optionals(self, args):
        if args.tmproot:
            self.tmproot = pathlib.Path(args.tmproot).resolve(strict=True)
        if args.cores:
            self.cores = args.cores
        if args.experiment:
            self.experiment = args.experiment
        if args.dataset:
            self.dataset = args.dataset

    def parse_config(self):
        """parse config file to dictionary using ConfigParser module"""
        cfg = configparser.ConfigParser()
        cfg.read(self.config)
        return cfg
