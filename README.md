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
You can enable more detailed logging by forwarding logs to a log server 
. You need to have two terminals open at the same time

 terminal 1: `pipenv run python main.py -v`
 
 terminal 2:`pipenv run python console.py`
 