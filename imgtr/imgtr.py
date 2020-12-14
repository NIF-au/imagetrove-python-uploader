
import imgtr.tardis
import imgtr.dicom
from imgtr.job import Job

import argparse
import logging
import sys

logger = logging.getLogger(__name__)


def get_args(args=sys.argv[1:]):
    """Read sys.args and return python args object"""
    parser = argparse.ArgumentParser()
    # Required arguments
    parser.add_argument('datatype', help='input datatype: ')
    parser.add_argument('indir', help='Input directory')
    # Optional arguments
    parser.add_argument('--config', help='Config file')
    parser.add_argument('--tmproot', help='Root dir to create tmpdir')
    parser.add_argument('--cores', help='number of cpu cores')
    parser.add_argument('--experiment', help='experiment name override')
    parser.add_argument('--dataset', help='dataset name override')
    parser.add_argument('--instrument', help='instrument name override')
    args = parser.parse_args(args)
    return args


def main(args=sys.argv[1:]):
    # Datatypes
    runner = {'dicom': imgtr.dicom.run}

    args = get_args(args)
    job = Job(args.indir, args.config)
    # job = UPLOAD_JOB[args.datatype](args.indir, args.config)
    logging.info('Datatype %s' % args.datatype)
    logging.info('Job %s' % job)

    # Getting config for optional arguments
    job.config_optionals()
    # Getting arguments for optional arguments
    job.args_optionals(args)

    logging.info('Input data dir at %s' % job.indir)
    logging.info('Config file at %s' % job.config)
    logging.info('%s cores for multiprocessing' % job.cores)

    job.server_from_cfg()
    job.staging_from_cfg()

    job.make_tmpdir()

    with job.tmphandle:
        logging.info('Created tmpdir at %s' % job.tmpdir)
        runner[args.datatype](job)

        # Zipping source files for archiving
        # job.archive_indir()


if __name__ == '__main__':
    main()
