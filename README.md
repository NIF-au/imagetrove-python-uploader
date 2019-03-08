# ImageTrove python DICOM uploader

Andrew Janke - __andrew.janke@uq.edu.au__ - National Imaging Facility.


Auto-upload from instrument to an AAF authenticated ImageTrove/MyTardis respository (in NecTAR/AWS cloud) from an instrument. Works by watching a local dcmtk DICOM listener client that runs in the local network. Thus it can upload data past VPN's and all sorts of hard things has it only uses the HTTPS protocol. All data is identified by project (and thus a group of people) from there access to the data is granted to the group of users associated with the project. 

## Usage
This python listener client will connect to a single instance of Imagetrove/MyTardis but can back-end on multiple DICOM instruments (typically MRI/CT/PET).

## Pre-requisites
Linux, Windows, Mac  
Miniconda Recommended  
`pip install -r requirements.txt`

## Config file
Default config file location is `HOME_DIR/imagetrove/imagetrove.ini`

#### [Instrument Mapping]

Use the following syntax:  
`ManufacturerName-StationName = MyTardis Instrument Name`  

*Remove spaces from `ManacturerName-StationName`. Examples found in imagetrove.ini.example

#### [MyTardis Instrument Name]
`experiment-tag` =  DICOM tag used as Experiment name  
`dataset-tag` = DICOM tag used as Dataset name  
`facility-name` = MyTardis Facility  
`storagebox` = MyTardis Storagebox  



## Running
`python run.py dicom {input-directory}`

### Optionals
| Option        | Description                      | Default value |
| ------------- |:--------------------------------:|--------------:|
| `--config `   | Config file path                 | `~/imagetrove/imagetrove.ini` |
| `--tmproot`   | Temp dir                         | Default OS tmp directory |
| `--cores`     | No. of cores for multiprocessing | All system cores |
| `--experiment`| Manual Experiment name override  | From metadata, see experiment-tag in config |
| `--dataset`   | Manual Dataset name override     | From metadata, see dataset-tag in config |

