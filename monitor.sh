#!/bin/bash

# Debug option
#python3.9 mysqlstatus.py -h 10.0.10.78 -u admin -P'Welcome#1' --debug -m global

# Not Debug
python3.9 mysqlstatus.py -h 10.0.20.204 -u admin -P'Welcome#1' -m global
