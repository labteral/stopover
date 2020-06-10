#!/bin/bash
gunicorn -w 1 --threads 16 --keep-alive 300 -b 0.0.0.0:8080 server:api
