
from imgtr.tardis import ObjectACL
from imgtr.tardis import Datafile
from imgtr.tardis import Dataset
from imgtr.tardis import Facility
from imgtr.tardis import Instrument
from imgtr.tardis import Experiment
from imgtr.tardis import Group
from imgtr.tardis import StorageBox
from imgtr.staging import upload_file
from imgtr.utils import safe_name
import pathlib
import logging
import pydicom
from imgtr.utils import zipdir
import datetime
import json
import os

logger = logging.getLogger(__name__)


def run(job):
    # Scanning all dicoms for all series
    logging.info('Scanning all dicoms for all series')
    # pools = mp.Pool(processes=job.cores)
    # Compiling list of dicoms and json metadata
    dicom_json_tuples = dir_scan(indir=job.indir, cfg=job.cfg, experiment=job.experiment, dataset=job.dataset)

    # Making series output dir
    series_json_tuples = make_series_dirs(dicom_json_tuples, outdir=job.tmpdir)

    # Sorting dicom
    logging.info('Sorting dicoms into series dirs ...')
    [sorter(*x, job.tmpdir) for x in dicom_json_tuples]
    # for dicom_json_tuple in dicom_json_tuples:
    #     sorter(*dicom_json_tuple, job.tmpdir)
    # pools.starmap(partial(sorter, outdir=job.tmpdir), dicom_json_tuples)

    logging.info('Zipping dicoms into series zips ...')
    # for x in [x[0] for x in series_json_tuples]:
    #     zipdir(x)
    [zipdir(x) for x in [x[0] for x in series_json_tuples]]
    # pools.map(zipdir, [x[0] for x in series_json_tuples])

    job.staging.open()
    push_series(
        series_json_tuples=series_json_tuples,
        server=job.server,
        cfg=job.cfg,
        ssh=job.staging.ssh
    )
    job.staging.close()


def dir_scan(indir, cfg, experiment, dataset):
    infiles = indir.glob('**/*')
    scan_generator = ((x, cfg, experiment, dataset) for x in infiles)
    scan_results = [scanner(*x) for x in scan_generator]
    # scan_results = pools.starmap(scanner, scan_generator)
    return scan_results


def make_series_dirs(scan_results, outdir):
    series_json_strings = sorted(set((x[1] for x in scan_results)))
    logger.info("Series ...\n{}".format('\n'.join(series_json_strings)))
    series_json_tuples = []
    for series_json_string in series_json_strings:
        series_json = json.loads(series_json_string)
        seriesdir = outdir/series_json['facility']/series_json['experiment']/series_json['dataset']/series_json['study']/series_json['series']
        dicomdir = seriesdir/'DICOM'
        dicomdir.mkdir(parents=True, exist_ok=True)
        series_json_tuples.append((str(seriesdir), series_json_string))
    return series_json_tuples


def scanner(infile, cfg, experiment, dataset):
    infile = str(infile)
    try:
        dcm = pydicom.dcmread(infile, stop_before_pixels=True)

        try:
            station = safe_name('-'.join([dcm.Manufacturer, dcm.StationName]))
        except AttributeError:
            logging.error(f'Manufacturer or StationName not found in {infile}')
            raise
        if not station:
            logging.error(f'Manufacturer and StationName is blank in {infile}')
            raise ValueError

        try:
            instrument = cfg['Instrument Mapping'][station]
        except KeyError:
            logging.error(f'{station} not found in config [Instrument Mapping]')
            raise
        if not instrument:
            logging.error(f'Instrument name not found in config [Instrument Mapping]')
            raise ValueError

        if cfg.has_option(instrument, 'facility-name'):
            facility = cfg[instrument]['facility-name']
            if not facility:
                logging.error(f'Facility name not found in config [{instrument}]')
                raise ValueError
        else:
            facility = instrument

        if not experiment:
            try:
                experiment = safe_name(getattr(dcm, cfg[instrument]['experiment-tag']))
            except KeyError:
                logging.error('experiment-tag entry missing in config')
                raise
            except AttributeError:
                logging.error('{} not found in {}'.format(cfg[instrument]['experiment-tag'], infile))
                raise
            if not experiment:
                logging.error(f'Experiment value is blank in  {infile}')
                raise ValueError

        if not dataset:
            try:
                dataset = safe_name(getattr(dcm, cfg[instrument]['dataset-tag']))
            except KeyError:
                logging.error('dataset-tag entry missing in config')
                raise
            except AttributeError:
                logging.error('{} not found in {}'.format(cfg[instrument]['dataset-tag'], infile))
                raise
            if not dataset:
                logging.error(f'Dataset value is blank in {infile}')
                raise ValueError

        try:
            study = safe_name(dcm.StudyDescription)
        except AttributeError:
            study = ''

        try:
            if not dcm.SeriesNumber:
                logging.error(f'SeriesNumber is blank in {infile}')
                raise ValueError
        except AttributeError:
            logging.error(f'SeriesNumber missing in {infile}')
            raise Exception

        try:
            if not dcm.SeriesDescription:
                logging.error(f'SeriesDescription is blank in {infile}')
                raise ValueError
        except AttributeError:
            logging.error(f'SeriesDescription missing in {infile}')
            raise Exception

        series = safe_name(f'{dcm.SeriesNumber:04}_{dcm.SeriesDescription}').lower()

        series_json = {
            'instrument': instrument,
            'facility': facility,
            'experiment': experiment,
            'dataset': dataset,
            'study': study,
            'series': series
        }
        return infile, json.dumps(series_json)
    except pydicom.errors.InvalidDicomError:
        logging.error(f'Invalid dicom file. Skipping ... {infile}')


def sorter(infile, series_json_string, outdir):
    series_json = json.loads(series_json_string)
    outdir = outdir/series_json['facility']/series_json['experiment']/series_json['dataset']/series_json['study']/series_json['series']/'DICOM'
    ERASE_TAG_LIST = [
            0x00080050,
            0x00204000,
            0x00081070,
            0x00101000,
            0x00100030,
            0x001021B0,
            0x00081080,
            0x00101090,
            0x00081060,
            0x00102180,
            0x00101001,
            0x00100032,
            0x00104000,
            0x00081050,
            0x00081048,
            0x00080092,
            0x00080090,
            0x00080094,
            # 0x00200010,
            0x00100010,
            0x00100020
            # 0x00081030
        ]
    dcm = pydicom.dcmread(str(infile), stop_before_pixels=False)
    # ISO 9660 compliance dicom filename
    outfilename = safe_name(f'{dcm.InstanceNumber:08}.dcm').lower()
    outfile = (outdir/outfilename).resolve()
    # De-identifying
    for tag in ERASE_TAG_LIST:
        if tag in dcm:
            dcm[tag].value = ''
    # Override StudyName with experiment
    dcm[0x00080090].value = series_json['experiment']  # Referring Physician Name
    # dcm[0x00200010].value = series_json['experiment']  # Study ID
    # dcm[0x00081030].value = series_json['experiment']  # Study Description
    # Override PatientName with dataset
    dcm[0x00100010].value = series_json['dataset']  # Patient Name
    dcm[0x00100020].value = series_json['dataset']  # Patient ID
    # Saving output dicom file
    dcm.save_as(str(outfile))


def push_series(series_json_tuples, server, cfg, ssh):
    for i in range(len(series_json_tuples)):
        seriesdir = series_json_tuples[i][0]
        series_json_string = series_json_tuples[i][1]

        serieszip = pathlib.Path(os.path.join(os.path.dirname(seriesdir), '{}.zip'.format(os.path.basename(seriesdir)))).resolve(strict=True)
        series_json = json.loads(series_json_string)
        dicomdir = pathlib.Path(f'{seriesdir}/DICOM')
        dicom = next((dicomdir).glob('*.dcm'))
        try:
            dcm = pydicom.dcmread(str(dicom), stop_before_pixels=True)
            timestring = f'{dcm.SeriesDate} {dcm.SeriesTime}'
            seriestime = datetime.datetime.strptime(timestring, "%Y%m%d %H%M%S.%f").replace(microsecond=0).isoformat()
            timestring = f'{dcm.StudyDate} {dcm.StudyTime}'
            studytime = datetime.datetime.strptime(timestring, "%Y%m%d %H%M%S.%f").replace(microsecond=0).isoformat()
        except Exception:
            seriestime = datetime.datetime.fromtimestamp(int(dicom.stat().st_ctime)).replace(microsecond=0).isoformat()
            studytime = datetime.datetime.fromtimestamp(int(dicom.stat().st_ctime)).replace(microsecond=0).isoformat()

        if i > 0 and old_series_json['instrument'] == series_json['instrument'] and \
            old_series_json['facility'] == series_json['facility'] and \
                old_series_json['experiment'] == series_json['experiment'] and \
                old_series_json['dataset'] == series_json['dataset']:
            ''' Current series shares dataset with previous series 
            Skipping MyTardis HTTP requests for dataset and above
            '''
            pass
        else:
            storagebox = StorageBox(server, cfg[series_json['instrument']]['storagebox'])
            storagebox.fetch(ssh=ssh)
            manager = Group(server, series_json['facility'])
            manager.fetch(create=True, ssh=ssh)
            facility = Facility(server, series_json['facility'], manager)
            facility.fetch(create=True, ssh=ssh)
            instrument = Instrument(server, series_json['instrument'], facility)
            instrument.fetch(create=True, ssh=ssh)
            experiment = Experiment(server, series_json['experiment'])
            experiment.fetch(create=True, ssh=ssh)
            group = Group(server, experiment)
            group.fetch(create=True, ssh=ssh)
            dataset = Dataset(server, series_json['dataset'], experiment, instrument, seriestime)
            dataset.fetch(create=True, ssh=ssh)
            groupacl = ObjectACL(server, group, experiment)
            groupacl.fetch(create=True, ssh=ssh)
            manageracl = ObjectACL(server, manager, experiment)
            manageracl.fetch(create=True, ssh=ssh)

        datafile = Datafile(server, serieszip, storagebox, dataset, series_json['study'], seriestime, studytime)
        upload_file(datafile, ssh)
        old_series_json = series_json
