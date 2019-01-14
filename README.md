# Unavailability-in-Transmission-Grid

### First
You need to have installed python 3.6+ and pipenv

##### Installing pipenv 
You can install pipenv via pip or an OS package 
manager

Ubuntu: `sudo apt install pipenv`

MacOS: `brew install pipenv`

Python : `pip install pipenv`

Windows: try google ?

### Setup environment and depedencies
Setting up a virtual environment and installing all code dependencies is easy 
open a terminal

`cd Unavailability-in-Transmission-Grid`

`pipenv install`

## Running the scapper

#### Run with 
`pipenv run python main.py`

#### Verbose logging 
Produce more detailed log messages and save them to a file by setting -v flag.
Every time the file main.py is executed, log file is overwritten.

`pipenv run python main.py -v`

#### Config file
The script needs a config file formatted as JSON. A missing or corrupted config
file will produce a runtime error.

A simple config.json
``` {
  "session": {
    "from_date": "14.01.2019",
    "to_date": "16.01.2019",
    "country": "FR",
    "asset_type": [
      "AC Link",
      "DC Link",
      "Substation",
      "Transformer",
      "Not specified"
    ],
    "outage_status": [
      "Active"
    ],
    "outage_type": [
      "Forced",
      "Planned"
    ],
    "area_type": "BORDER_BZN"
  },
  "advanced": {
    "log_file": "logs.log",
    "time_delay": 1,
    "data_dir": "data"
  }
}
``` 
 
 