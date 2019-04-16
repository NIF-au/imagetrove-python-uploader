
from imgtr.utils import checksum
import mimetypes
import pathlib
import requests
import json
import logging
import urllib
import urllib.parse
from urllib.parse import urlencode
logger = logging.getLogger(__name__)


class TardisServer:
    def __init__(self, url, user, apikey, institution, curl=False):
        self.url = url
        self.user = user
        self.apikey = apikey
        self.institution = institution
        self.curl = curl
        self.headers = {"Authorization": f"ApiKey {user}:{apikey}"}

    def get(self, apipath, ssh=None):
        url = urllib.parse.urljoin(self.url, apipath)
        # logging.info(f'GET {url}')
        if ssh and self.curl is True:
            cmd = f'curl --header \"Authorization: ApiKey {self.user}:{self.apikey}\" --request GET \"{url}\"'
            logging.info(f'curl GET {url}')
            _, stdout, stderr = ssh.exec_command(cmd)
            response = stdout.read()
        else:
            logging.info(f'GET {url}')
            response = requests.get(headers=self.headers, url=url).text

        if response:
            results = json.loads(response)
            if 'objects' in results:
                return results['objects']
            else:
                return None
        else:
            return None

    def post(self, apipath, data, ssh=None, files=None):
        url = urllib.parse.urljoin(self.url, apipath)
        logging.info(f'POST {url} {data}')
        if ssh and self.curl is True:
            cmd = f'curl --header \"Authorization: ApiKey {self.user}:{self.apikey}\" --header \"Content-Type: application/json\" --data \'{data}\' --request POST \"{url}\"'
            _, stdout, stderr = ssh.exec_command(cmd)
            response = stdout.read()
            logging.info(response)
        else:
            if files is not None:
                file_obj = open(files, 'rb')
                headers = {"Authorization": f"ApiKey {self.user}:{self.apikey}"}
                response = requests.post(headers=headers, url=url, data={"json_data": data}, files={'attached_file': file_obj}).text
                logging.info(response)
                file_obj.close()
            else:
                headers = {"Authorization": f"ApiKey {self.user}:{self.apikey}", "Content-Type": "application/json"}
                response = requests.post(headers=headers, url=url, data=data).text
                logging.info(response)


class TardisObject:
    def __init__(self, server=None, name=None):
        self.server = server
        self.name = str(name)
        self.id = None
        self.model_name = 'tardisobject'
        self.new_json = {}
        self.query = {'name':self.name}

    def fetch(self, create=False, ssh=None):
        query_string = urlencode(self.query)
        results = self.server.get(f'/api/v1/{self.model_name}/?format=json&{query_string}', ssh)
        if results:
            self.id = results[-1]['id']
        elif create:
            self.server.post(f'/api/v1/{self.model_name}/?format=json', json.dumps(self.new_json), ssh)
            self.fetch(False, ssh)

    def __str__(self):
        return self.name


class Group(TardisObject):
    def __init__(self, server, name):
        TardisObject.__init__(self, server, name)
        self.model_name = 'group'
        self.new_json = {
            "name": self.name
        }


class Facility(TardisObject):
    def __init__(self, server, name, manager):
        TardisObject.__init__(self, server, name)
        self.manager = manager
        self.model_name = 'facility'
        self.new_json = {
            "name": self.name,
            "manager_group": f"/api/v1/group/{self.manager.id}/"
        }


class Instrument(TardisObject):
    def __init__(self, server, name, facility):
        TardisObject.__init__(self, server, name)
        self.facility = facility
        self.model_name = 'instrument'
        self.new_json = {
            "name": self.name,
            "facility": f"/api/v1/facility/{self.facility.id}/"
        }


class Experiment(TardisObject):
    def __init__(self, server, name, handle=""):
        TardisObject.__init__(self, server, name)
        self.model_name = 'experiment'
        self.handle = handle
        self.query = {'title': self.name}
        self.new_json = {
            "title": self.name,
            "immutable": False,
            'institution': self.server.institution,
            'handle': self.handle
        }

    def fetch(self, create=False, ssh=None):
        query_string = urlencode(self.query)
        results = self.server.get(f'/api/v1/{self.model_name}/?format=json&{query_string}', ssh)
        if results:
            self.id = results[-1]['id']
            if 'handle' in results[-1]:
                self.handle = results[-1]['handle']
        elif create:
            self.server.post(f'/api/v1/{self.model_name}/?format=json', json.dumps(self.new_json), ssh)
            self.fetch(False, ssh)


class Dataset(TardisObject):
    def __init__(self, server, name, experiment, instrument, acqtime):
        TardisObject.__init__(self, server, name)
        self.model_name = 'dataset'
        self.experiment = experiment
        self.instrument = instrument
        self.fullname = f'{self.name}-{self.experiment.name}'
        self.acqtime = acqtime
        self.query = {
            'description': self.name,
            'experiments__id': self.experiment.id
        }
        self.new_json = {
            "description": self.name,
            "experiments": [f"/api/v1/experiment/{self.experiment.id}/"],
            "immutable": False,
            "instrument": f"/api/v1/instrument/{self.instrument.id}/",
            "created_time": self.acqtime
        }

    @property
    def uri(self):
        return f'{self.name}-{self.id}'


class StorageBox(TardisObject):
    def __init__(self, server, name):
        TardisObject.__init__(self, server, name)
        self.path = None
        self.model_name = 'storagebox'

    def fetch(self, create=False, ssh=None):
        if create:
            logging.warning('Storagebox creation not authorized')
        results = self.server.get(f'/api/v1/{self.model_name}/?format=json', ssh)
        if results:
            for result in results:
                if result['name'] == self.name:
                    self.id = result['id']
                    self.path = pathlib.PurePosixPath(result['options'][0]['value'])
                    if not self.path.is_absolute():
                        logging.error('Storage box path not absolute')
                        raise Exception


class ObjectACL(TardisObject):
    def __init__(self, server, group, experiment):
        TardisObject.__init__(self, server)
        self.model_name = 'objectacl'
        self.group = group
        self.experiment = experiment
        self.query = {
            'pluginId': 'django_group',
            'entityId': self.group.id
        }
        self.new_json = {
            "pluginId": "django_group",
            "entityId": str(self.group.id),
            "content_object": f"/api/v1/experiment/{self.experiment.id}/",
            "content_type": "experiment",
            "object_id": self.experiment.id,
            "aclOwnershipType": 1,
            "isOwner": False,
            "canRead": True,
            "canWrite": False,
            "canDelete": False,
            "effectiveDate": None,
            "expiryDate": None}

    def fetch(self, create=False, ssh=None):
        query_string = urlencode(self.query)
        results = self.server.get(f'/api/v1/{self.model_name}/?format=json&{query_string}', ssh)
        for result in results:
            if result['content_object'] == f"/api/v1/experiment/{self.experiment.id}/":
                break
            else:
                result = None

        if result:
            self.id = result['id']
        elif create:
            self.server.post(f'/api/v1/{self.model_name}/?format=json', json.dumps(self.new_json), ssh)
            self.fetch(False, ssh)

    def __str__(self):
        return None


class Datafile(TardisObject):
    def __init__(self, server, file, storagebox, dataset, study, acqtime):
        self.file = file
        self.name = file.name
        TardisObject.__init__(self, server, file.name)
        self.model_name = 'dataset_file'
        self.dataset = dataset
        self.study = study
        self.storagebox = storagebox
        self.directory = None
        self.md5sum = None
        self.sha512sum = None
        self.verified = None
        self.size = None
        self.count = 1
        self.acqtime = acqtime
        self.query = {
            'dataset__id': self.dataset.id,
            'filename': urllib.parse.quote(self.name)
        }
        self.new_json = {
            "dataset": f"/api/v1/dataset/{self.dataset.id}/",
            "filename": self.name,
            "directory": self.study,
            "md5sum": checksum('md5', self.file),
            "sha512sum": checksum('sha512', self.file),
            "size": str(self.file.stat().st_size),
            "mimetype": mimetypes.guess_type(str(self.file))[0],
            "created_time": self.acqtime,
            "replicas": [{
                "url": str(self.uri),
                "location": self.storagebox.name,
                "protocol": "file",
                "verified": False
            }]
        }

    @property
    def uri(self):
        return f'{self.dataset.uri}/{self.name}'

    def fetch(self, create=False, ssh=None, files=None):
        self.query = {
            'dataset__id': self.dataset.id,
            'filename': urllib.parse.quote(self.name)
        }
        query_string = urlencode(self.query)
        results = self.server.get(f'/api/v1/{self.model_name}/?format=json&{query_string}', ssh)
        if results:
            self.id = results[-1]['id']
            self.md5sum = results[-1]['md5sum']
            self.sha512sum = results[-1]['sha512sum']
            self.verified = results[-1]['replicas'][0]['verified']
            self.size = results[-1]['size']
            self.directory = results[-1]['directory']
        elif create:
            self.new_json = {
                "dataset": f"/api/v1/dataset/{self.dataset.id}/",
                "filename": self.name,
                "directory": self.study,
                "md5sum": checksum('md5', self.file),
                "sha512sum": checksum('sha512', self.file),
                "size": str(self.file.stat().st_size),
                "mimetype": mimetypes.guess_type(str(self.file))[0],
                "created_time": self.acqtime,
                "replicas": [{
                    "url": str(self.uri),
                    "location": self.storagebox.name,
                    "protocol": "file",
                    "verified": False
                }]
            }
            if files:
                # self.new_json["replicas"][0]["uri"] = ""
                self.server.post(f'/api/v1/{self.model_name}/?format=json', json.dumps(self.new_json), ssh, files)
            else:
                self.server.post(f'/api/v1/{self.model_name}/?format=json', json.dumps(self.new_json), ssh)
            self.fetch(False, ssh)

    def verify(self, ssh):
        self.server.get(f'/api/v1/{self.model_name}/{self.id}/verify/?format=json', ssh)

    def scp(self, ssh):
        sftp = ssh.open_sftp()
        targetdir = f'{self.storagebox.path}/{self.dataset.uri}'
        targetfile = f'{targetdir}/{self.name}'
        try:
            sftp.chdir(str(self.storagebox.path))  # Test if remote_path exists
        except IOError:
            sftp.mkdir(str(self.storagebox.path))  # Create remote_path
            sftp.chdir(str(self.storagebox.path))
        try:
            sftp.chdir(targetdir)  # Test if remote_path exists
        except IOError:
            sftp.mkdir(targetdir)  # Create remote_path
            sftp.chdir(targetdir)

        # _, stdout, _ = ssh.exec_command(f'mkdir -p {targetdir}')
        sftp.put(str(self.file), targetfile)


