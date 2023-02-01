#!/bin/sh
docker-compose up -d
pip install -r requirements.txt
python producer.py
