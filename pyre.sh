#!/usr/bin/env bash

pyre --search-path=$(pipenv --venv)/lib/python3.7/site-packages --source-directory scrappers_collection check
