<p align="center">
<img src="misc/stopover.svg" alt="Stopover Logo" width="150"/></a>
</p>

<h3 align="center">
<b>A simple message broker powered by RocksDB</b>
</h3>

<p align="center">
    <a href="https://www.buymeacoffee.com/brunneis" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/default-orange.png" alt="Buy Me A Coffee" height="35px"></a>
</p>

# Initialise the data dir
```bash
mkdir -p ./data/streams
touch ./data/streams/.active
```

# Start the server
```bash
docker-compose up -d
```