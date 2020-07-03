#!/bin/bash
gunicorn server:api --workers 1 --bind 0.0.0.0:5704 --threads 16 --keep-alive 300
