#!/bin/bash

wget https://oo-register-production.s3-eu-west-1.amazonaws.com/public/exports/statements.latest.jsonl.gz
mv statements.latest.jsonl.gz ../data/
