<p align="center">
<img src="misc/stopover.svg" alt="Stopover Logo" width="150"/></a>
</p>

<div align="center">
<b>Stopover</b> - A simple and robust message broker built on top of RocksDB
</div>
<br>

<p align="center">
    <a href="https://pepy.tech/project/stopover-server/"><img alt="Downloads" src="https://img.shields.io/badge/dynamic/json?style=flat-square&maxAge=3600&label=downloads&query=$.total_downloads&url=https://api.pepy.tech/api/projects/stopover-server"></a>
    <a href="https://pypi.python.org/pypi/stopover-server/"><img alt="PyPi" src="https://img.shields.io/pypi/v/stopover-server.svg?style=flat-square"></a>
    <a href="https://github.com/labteral/stopover/releases"><img alt="GitHub releases" src="https://img.shields.io/github/release/labteral/stopover.svg?style=flat-square"></a>
    <a href="https://github.com/labteral/stopover/blob/master/LICENSE"><img alt="License" src="https://img.shields.io/github/license/labteral/stopover.svg?style=flat-square&color=green"></a>
</p>

<p align="center">
    <a href="https://www.buymeacoffee.com/brunneis" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/default-orange.png" alt="Buy Me A Coffee" height="35px"></a>
</p>

# Initialize the data dir
```bash
mkdir -p ./data/streams
touch ./data/streams/.active
```

# Start the server
## docker-compose
```bash
docker-compose up -d
```
## Python
```bash
python3 -m stopover_server
```
