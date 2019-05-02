
import paramiko
import logging
logger = logging.getLogger(__name__)


class Staging:
    def __init__(self, user=None, host=None, port=None, key=None):
        self.user = user
        self.host = host
        self.port = port
        self.key = key
        self.ssh = None

    @property
    def active(self):
        if self.user and self.host and self.port and self.key:
            return True
        return False

    def open(self):
        if self.active:
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(
                hostname=self.host,
                port=self.port,
                username=self.user,
                key_filename=self.key
            )

    def close(self):
        if self.active:
            logging.info('Closing SSH session')
            self.ssh.close()


def upload_file(datafile, ssh):
    """Static upload sequence for map/multiprocessing"""
    datafile.fetch(create=False, ssh=ssh)
    if not datafile.verified:
        if ssh:
            datafile.fetch(create=True, ssh=ssh)
            datafile.scp(ssh=ssh)
        else:
            datafile.fetch(create=True, ssh=ssh, files=datafile.file)
        datafile.verify(ssh=ssh)
    else:
        size = str(datafile.file.stat().st_size)
        if size != str(datafile.size):
            if datafile.count > 10:
                # Sanity check
                logging.error('Too many non-identical files with same name')
                raise Exception
            else:
                time_tag = datafile.studytime.replace(':', '').replace('-', '')
                newname = f'{datafile.file.stem}_{time_tag}{datafile.file.suffix}'
                if newname == datafile.name:
                    datafile.name = f'{datafile.name}0'
                else:
                    datafile.name = newname
                datafile.verified = False
                datafile.count = datafile.count + 1
                upload_file(datafile, ssh)
