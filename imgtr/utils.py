
import hashlib
import logging
import os
import pathlib
import tempfile
import re
import zipfile
import shutil

logger = logging.getLogger(__name__)


def zipdir(path_dir, zipfilepath=''):
    """Zips contents of input directory
    :param path_dir: input directory
    :param zipfilepath: output zip file
    :return: None
    """
    shutil.make_archive(root_dir=str(path_dir),format='zip', base_name=str(path_dir))
    # if not zipfilepath:
    #     zipfilepath = os.path.join(os.path.dirname(path_dir), os.path.basename(path_dir)+'.zip')
    # logging.info(f'Doing {zipfilepath}')
    # with zipfile.ZipFile(zipfilepath, 'w', zipfile.ZIP_DEFLATED) as zip_file:
    #     for root, dirs, files in os.walk(os.path.join(path_dir, 'DICOM')):
    #         for inode in files + dirs:
    #             zip_file.write(os.path.join(root, inode), inode)


def safe_name(name):
    """Replaces unsafe filename characters with underscore
    Windows: <>:"/\|?*
    Linux: whitespace
    :param name:
    :return:
    """
    name = re.sub(r'[ =?()<>:^/"|*\\ ]', '_', str(name))
    return name


def create_tmpdir(tmproot=tempfile.gettempdir(), prefix=None):
    """Creates tmp dir
    :param tmproot: Root dir to create tmp_dir in, defaults to tempfile.gettempdir()
    :type tmproot: pathlib.Path, optional
    :param suffix: suffix name to attach
    :type suffix: string
    :return: Temporary directory created in tmp_parent
    :rtype: pathlib.Path
    """
    tmproot = pathlib.Path(tmproot).resolve()
    tmproot.mkdir(parents=True, exist_ok=True)
    tmphandle = tempfile.TemporaryDirectory(dir=tmproot)
    tmpdir = pathlib.Path(tmphandle.name).resolve()
    logging.debug('Temporary directory created at %s' % tmpdir)
    return tmpdir, tmphandle


def checksum(hasher, infile, blocksize=65536):
    """Calculates checksum using supported hashing function
    :param hasher: md5 & sha512 supported
    :param infile: input file to be checksummed
    :param blocksize: defaults to 65536
    :return: hash as string
    """
    hashers = {'md5': hashlib.md5(), 'sha512': hashlib.sha512()}
    with open(str(infile), 'rb') as datafile:
        buf = datafile.read(blocksize)
        while len(buf) > 0:
            hashers[hasher].update(buf)
            buf = datafile.read(blocksize)
        return hashers[hasher].hexdigest()
