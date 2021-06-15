<p align="center">
<img src="misc/stopover.svg" alt="Stopover Logo" width="150"/></a>
</p>

<h3 align="center">
<b>Stopover</b> - A simple and robust message broker built on top of RocksDB
</h3>
<br>
<p align="center">
    <a href="https://www.buymeacoffee.com/brunneis" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/default-orange.png" alt="Buy Me A Coffee" height="35px"></a>
</p>
<br>

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
