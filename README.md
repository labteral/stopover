# Initialise the data dir
```bash
mkdir -p ./data/streams
touch ./data/streams/.active
```

# Start the server
```bash
gunicorn -w 1 --threads 16 --keep-alive 300 -b 0.0.0.0:8080 server:api
```

# Usage example
```bash
pip install stopover
```

## Sender
```python
from stopover import Receiver

sender = Sender('http://localhost:8080', 'stream0')

index = 0
while True:
  sender.put(f'hello world #{index}')
  index += 1
```

## Receiver
```python
from stopover import Receiver

receiver = Receiver('http://localhost:8080', 'stream0', 'receiver1')

for message in receiver.get():
    print(message.index, message.value)
    receiver.commit(message)
```
