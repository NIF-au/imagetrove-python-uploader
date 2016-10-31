#! /usr/bin/env python3
#
# upload DICOM directory to ImageTrove/mytardis
#
#
# Andrew Janke - a.janke@gmail.com
#
# Copyright Andrew Janke, The University of Queensland.
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose and without fee is hereby granted,
# provided that the above copyright notice appear in all copies.  The
# author and the University make no representations about the
# suitability of this software for any purpose.  It is provided "as is"
# without express or implied warranty.
#
# sudo pip3 install pydicom

import sys
import argparse
import os.path
import os
import errno
import tempfile
import shutil
import subprocess
import datetime
import dicom as pydicom
import tarfile
import json
import requests
import urllib
import hashlib
import mimetypes
import datetime
import configparser


def do_cmd(cmd, show_output):
    if args.verbose:
        print(' '.join(str(x) for x in cmd))
    if not args.fake:

        if show_output:
            subprocess.call((str(x) for x in cmd))
        else:
            FNULL = open(os.devnull, 'w')
            subprocess.call((str(x) for x in cmd), stdout=FNULL, stderr=subprocess.STDOUT)


def listdir_fullpath(d):
    return [os.path.join(d, f) for f in os.listdir(d)]


# Calculate MD5 checksum
def md5_sum(file_path, blocksize=65536):
    hasher = hashlib.md5()
    with open(file_path, 'rb') as datafile:
        buf = datafile.read(blocksize)
        while len(buf) > 0:
            hasher.update(buf)
            buf = datafile.read(blocksize)
        return hasher.hexdigest()


# get an experimentID from an experiment Title
# Create an experiment if it doesn't exist yet
def get_experimentID(cfg, title, institution):
    imagetrove_headers = {
        "Authorization": "ApiKey %s:%s" % (cfg['ServerConfig']['Username'], cfg['ServerConfig']['ApiKey'])}
    url = cfg['ServerConfig']['Url'] + 'api/v1/experiment/?format=json'
    url += "&title=" + urllib.parse.quote(title)
    response = requests.get(headers=imagetrove_headers, url=url)
    if response.status_code == 200:
        print("   - E Success Code: " + str(response.status_code))
    else:
        print(url)
        print(json.dumps(response.json()))
        print("response.status_code = " + str(response.status_code))
        sys.exit()

    # check if it exists
    if response.json()["meta"]["total_count"] > 0:
        print("   * experiment " + title + " exists")
        experimentID = str(response.json()["objects"][0]["id"])

    else:
        # create the experiment
        print("   + experiment " + title + " not found, Creating")
        new_exp_json = {
            "title": title,
            "description": "Experiment Description for Project " + title,
            "immutable": False}
        new_exp_json['institution'] = institution
        response = requests.post(
            headers=imagetrove_headers,
            url=url,
            data=json.dumps(new_exp_json))
        if response.status_code == 201:
            print("   + E Success Code: " + str(response.status_code))
        else:
            print(url)
            print(json.dumps(response.json()))
            print("response.status_code = " + str(response.status_code))
            sys.exit()

        experimentID = str(response.json()["id"])

    return experimentID


# get a datasetID from an experiment Title
# Create an experiment if it doesn't exist yet
def get_datasetID(cfg, description, experimentID, instrument):
    imagetrove_headers = {
        "Authorization": "ApiKey %s:%s" % (cfg['ServerConfig']['Username'], cfg['ServerConfig']['ApiKey'])}
    url = cfg['ServerConfig']['Url'] + 'api/v1/dataset/?format=json'
    url += "&description=" + urllib.parse.quote(description)
    url += "&experiments__id=" + experimentID
    response = requests.get(headers=imagetrove_headers, url=url)
    if response.status_code == 200:
        print("   + DS Success Code: " + str(response.status_code))
    else:
        print(url)
        print(json.dumps(response.json()))
        print("response.status_code = " + str(response.status_code))
        sys.exit()

    # check if it exists
    if response.json()["meta"]["total_count"] > 0:
        print("   * dataset " + description + " exists")
        # get the ID
        datasetID = str(response.json()["objects"][0]["id"])

    else:
        # create the dataset
        print("   + dataset " + description + " not found for " + experimentID + ", Creating")

        new_dataset_json = {
            "description": description,
            "experiments": ["/api/v1/experiment/%s/" % experimentID],
            "immutable": False,
            "instrument": "/api/v1/instrument/%s/" % instruments[dodgymapping[Manufacturer + "-" + StationName]]}

        response = requests.post(
            headers=imagetrove_headers,
            url=url,
            data=json.dumps(new_dataset_json))
        if response.status_code == 201:
            print("   + DS Success Code: " + str(response.status_code))
        else:
            print(url)
            print(json.dumps(response.json()))
            print("response.status_code = " + str(response.status_code))
            sys.exit()

        datasetID = str(response.json()["id"])

    return datasetID


# check for a datafile
def datafile_exists(cfg, filename, datasetID):
    imagetrove_headers = {
        "Authorization": "ApiKey %s:%s" % (cfg['ServerConfig']['Username'], cfg['ServerConfig']['ApiKey'])}
    url = cfg['ServerConfig']['Url'] + 'api/v1/dataset_file/?format=json'
    url += "&dataset__id=" + datasetID
    url += "&filename=" + urllib.parse.quote(os.path.basename(filename))

    response = requests.get(url=url, headers=imagetrove_headers)
    if response.status_code < 200 or response.status_code >= 300:
        raise Exception("Failed to check for existing file '%s' in dataset ID %s." % (filename, datasetID))
    return response.json()['meta']['total_count'] > 0


# upload a datafile to a dataset
def upload_datafile(cfg, filename, datasetID, storagebox_name):

    print("   + Adding " + filename + " to " + datasetID + " in " + storagebox_name)
    if datafile_exists(cfg, filename, datasetID):
        print("     - ALREADY EXISTS")

    else:
        new_datafile_json = {
            "dataset": "/api/v1/dataset/%s/" % datasetID,
            "filename": os.path.basename(filename),
            "directory": "",    #os.path.dirname(filename),
            "md5sum": md5_sum(filename),
            "size": str(os.stat(filename).st_size),
            "mimetype": mimetypes.guess_type(filename)[0],
            "created_time": datetime.datetime.fromtimestamp(os.stat(filename).st_ctime).isoformat(),
            "replicas": [{ "url": "", "protocol": "file", "location": storagebox_name}]}

        # setup and send the request
        imagetrove_headers = {
            "Authorization": "ApiKey %s:%s" % (cfg['ServerConfig']['Username'], cfg['ServerConfig']['ApiKey'])}
        url = cfg['ServerConfig']['Url'] + 'api/v1/dataset_file/?format=json'

        file_obj = open(filename, 'rb')
        response = requests.post(
            headers=imagetrove_headers,
            url=url,
            data={"json_data": json.dumps(new_datafile_json)},
            files={'attached_file': file_obj})
        file_obj.close()

        if response.status_code == 201:
            print("   + DF UPLOADED: " + filename + " " + str(response.status_code))
        else:
            print("response.status_code = " + str(response.status_code))
            print("****URL: " + url)
            print("****Headers: " + json.dumps(imagetrove_headers))
            print("****JSON: " + json.dumps(new_datafile_json))
            print(json.dumps(response.json()))
            print(response.text)
            sys.exit()


# main upload routine
if __name__ == "__main__":
    me = os.path.basename(sys.argv[0])

    # get history string
    # history = datetime.datetime.now().isoformat()
    # history += '>>>> ' + " ".join(str(x) for x in sys.argv)

    # set up and parse command line arguments
    parser = argparse.ArgumentParser(description = "Convert a DICOM directory " +
        "to MINC/NIFTI/PNG and upload to imagetrove." +
        "\n\n" +
        "Configuration of the server to upload to and mappings are stored in " +
        "the config file specified by --config")
    parser.add_argument('-v', '--verbose', help="be verbose",
        action="store_true", default=False)
    parser.add_argument('--debug', help="be very verbose",
        action="store_true", default=False)
    parser.add_argument('-c', '--config', help="file to read config from",
        default="./imgtr-uploader-dicom.cfg")
    parser.add_argument('-f', '--fake', help="do a dry run (echo cmds only)",
        action="store_true", default=False)
    parser.add_argument('-z', '--compress', help="compress .tar.bz2 input dir when done",
        action="store_true", default=False)
    parser.add_argument("indir", help="the input DICOM directory")
    args = parser.parse_args()

    # remove possible trailing slash that will break
    # everything later with dirname/basename
    args.indir = os.path.normpath(args.indir)
    print(" + indir: " + args.indir)

    # TODO eventually handled by external config
    args.pngargs = ['-triplanar',
        '--tilesize', 350,
        '--horizontal',
        '--sagittal_offset_perc', 10,
        '--auto_range']

    # make tmpdir
    tmpdir = tempfile.TemporaryDirectory()
    dcmdir = tmpdir.name + "/dcm"
    if args.verbose:
        print(" + dcmdir: " + dcmdir)
    try:
        os.makedirs(dcmdir)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise exc
        pass

    # import config
    cfg = configparser.ConfigParser()
    cfg.read(args.config)

    # sort input directory via dcmsort
    do_cmd(['dcmsort', '--by_id', '--outdir', dcmdir, '--copy', args.indir], False)

    # for each experiment/dataset (in case there is more than one)
    for edir in os.listdir(dcmdir):
        print(" + experiment: " + edir)

        # create output dir
        outdir = tmpdir.name + "/upload/" + edir
        try:
            os.makedirs(outdir)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise exc
            pass

        # read the first DICOM to create the experiment_title and dataset
        for root, dirs, files in os.walk(os.path.join(dcmdir, edir), topdown=True):
            for f in files:
                first_file = os.path.join(root, f)
                break
        print("   - first_file: " + first_file)
        firstDCM = pydicom.read_file(first_file)

        # Get Manufacturer and StationName
        # use this to determine where to get other information
        if "Manufacturer" in firstDCM:
            Manufacturer = firstDCM.Manufacturer
        else:
            Manufacturer = "noManufacturer"

        if "StationName" in firstDCM:
            StationName = firstDCM.StationName
        else:
            StationName = "noStationName"

        instrument = cfg['InstrumentMapping'][Manufacturer + "-" + StationName]
        instrumentID = cfg['InstrumentIds'][instrument]
        if args.verbose:
            print("InstrumentID: " + instrumentID + " -- " + instrument)

        # Determine the ProjectID
        # typically this is DICOM.ReferringPhysicianName (0008,009)
        if cfg[instrument]['ProjectID'] in firstDCM:
            experiment_title = str(getattr(firstDCM, cfg[instrument]['ProjectID']))
        else:
            print("ProjectID missing, this isn't going to work")
            sys.exit()

        # check the experiment_title here and make sure it is numeric
        # TODO

        # Determine the DatasetID
        # typically this is the DICOM.PatientID (0010,0020)
        if cfg[instrument]['DatasetID'] in firstDCM:
            dataset_title = str(getattr(firstDCM, cfg[instrument]['DatasetID']))

        # Determine InstitutionName
        # typically DICOM.InstitutionName (0008,0080)
        if "InstitutionName" in firstDCM:
            InstitutionName = firstDCM.InstitutionName
        else:
            InstitutionName = cfg['Defaults']['Institution']

        # check (create and) get the MyTardis ExperimentID
        # Note that this could be one level up but we are being cautious
        experimentID = str(get_experimentID(cfg, experiment_title, InstitutionName))
        if args.verbose:
            print("experimentID: " + experimentID)

        # check (create and) get the datasetID
        datasetID = str(get_datasetID(cfg, dataset_title, experimentID, instrumentID))
        if args.verbose:
            print("datasetID: " + str(datasetID))

        # for each datafile
        for ddir in sorted(os.listdir(dcmdir + "/" + edir)):

            # list of DCM files for dataset
            flist = listdir_fullpath(dcmdir + "/" + edir + "/" + ddir)

            # get the information we need from the first DICOM of
            # of each dataset/series
            firstDCM = pydicom.read_file(flist[0])

            # get and clean up StudyDescription (0008,1030)
            if "StudyDescription" in firstDCM:
                StudyDescription = firstDCM.StudyDescription
            else:
                StudyDescription = "noStudy"
            StudyDescription = StudyDescription.replace('^', '-').replace(' ', '-').replace('_', '-')

            # get and clean up SeriesDescription (0008,103e)
            if "SeriesDescription" in firstDCM:
                SeriesDescription = firstDCM.SeriesDescription
            else:
                SeriesDescription = "noSeries"
            SeriesDescription = SeriesDescription.replace('^', '-')
            SeriesDescription = SeriesDescription.replace(' ', '-')
            SeriesDescription = SeriesDescription.replace('_', '-')
            SeriesDescription = SeriesDescription.replace('(', '-')
            SeriesDescription = SeriesDescription.replace(')', '-')

            # get and clean up SeriesNumber (0020,0011)
            if "SeriesNumber" in firstDCM:
                SeriesNumber = firstDCM.SeriesNumber
            else:
                SeriesNumber = 0

            print(" + [" + experiment_title + ' : ' +
                dataset_title + ' : ' +
                instrument + ' : ' +
                str(SeriesNumber) + ' : ' +
                StudyDescription + ' : ' +
                SeriesDescription + "]")

            # anonymise
            cmd = ['dcmodify',
                '-ie',   # ignore errors
                '-nb',   # no backup files
                '-imt',  # ignore missing tags
                '-gin',  # generate new SOP Instance UID
                '-ma', "(0010,0010)=" + dataset_title,
                "-ea", "(0010,0030)",    # Patient birth date
                "-ea", "(0008,0050)",    # Accession number
                "-ea", "(0020,000D)",    # Study Instance UID
                "-ea", "(0020,000E)",    # Series Instance UID
                "-ea", "(0008,0018)",    # SOP Instance UID
                #"-ea", "(0008,0080)",    # Institution Name
                #"-ea", "(0008,0081)",    # Institution Address
                "-ea", "(0008,1070)",    # Operator Name
                "-ea", "(0008,1155)",    # Referenced SOP Instance UID
                "-ea", "(0010,1000)",    # Other Patient Ids
                "-ea", "(0020,0010)",    # Study ID
                "-ea", "(0020,4000)"]    # Image Comments
            cmd.extend(flist)
            do_cmd(cmd, False)

            if args.verbose:
               print(" + Anonymised " + str(len(flist)) + " files in " + ddir)

            # create a metadatfile file
            metafile = outdir + "/" + edir + "_" + str(SeriesNumber).zfill(2) + ".txt"
            with open(metafile, "a+") as f:
                f.write(str(pydicom.read_file(flist[0])))
            upload_datafile(cfg, metafile, datasetID, 'imagetrove_disk')

            # convert to MINC
            cmd = ['dcm2mnc', '-usecoordinates', '-anon',
                '-dname', '',
                '-fname', '%N_%D-%T_' + str(SeriesNumber).zfill(2) + '_' + StudyDescription + '_' + SeriesDescription + '%s%e%t%p%c']
            cmd.extend(flist)
            cmd.append(outdir)
            do_cmd(cmd, False)

        # create the DICOM.tar.gz
        tfile = outdir + "/" + edir + ".tar.gz"
        print("  - creating " + tfile)
        tar = tarfile.open(tfile, "w:gz")
        tar.add(dcmdir + "/" + edir, recursive=True, arcname=edir)
        tar.close()
        upload_datafile(cfg, tfile, datasetID, 'imagetrove_hfs')

        # collect all the MINC files and convert
        for f in os.listdir(outdir):
            if f.endswith(".mnc"):
                mncfile = os.path.join(outdir, f)
                niifile = mncfile.rsplit(".", 1)[0] + ".nii"
                pngfile = mncfile.rsplit(".", 1)[0] + ".png"

                # to NII
                do_cmd(['mnc2nii', mncfile, niifile], False)

                # to PNG
                cmd = ['mincpik']
                cmd.extend(args.pngargs)
                cmd.extend(['--title',
                    '--title_text', os.path.basename(mncfile),
                    mncfile, pngfile])
                do_cmd(cmd, False)

                # add the files to the upload list
                upload_datafile(cfg, mncfile, datasetID, 'imagetrove_hfs')
                upload_datafile(cfg, niifile, datasetID, 'imagetrove_hfs')
                upload_datafile(cfg, pngfile, datasetID, 'imagetrove_disk')

        print("  + Finished with experiment " + experiment_title + " (" + experimentID + ")" +
            " uploaded files to dataset " + dataset_title + "(" + datasetID + ")")


    # compress indir
    zipfile = os.path.basename(args.indir) + '-' + experiment_title + "-" + dataset_title + ".zip"
    print("  + zipfile: " + zipfile)
    os.chdir(os.path.dirname(args.indir))
    do_cmd(['zip', '--move', '--test', '--recurse-paths', zipfile,
        os.path.basename(args.indir)], True)
