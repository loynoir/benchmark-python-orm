#!/bin/bash
pip install peewee sqlalchemy && python ./test.py | tee README.txt
