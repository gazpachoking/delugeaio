# Deluge Client for asyncio

This is a sorta port of the `deluge.ui.client` module from deluge, rebuilt to use asyncio rather than Twisted.
Works with deluge develop branch daemon for now.

Still very messy and untested.

## Example Client

```python
import asyncio

import delugeaio


client = delugeaio.Client()

async def run():
    await client.connect('127.0.0.1', 58846, 'test', 'test')
    print("Connection was successful!")
    print("download_location: {}".format(await client.core.get_config_value("download_location")))

asyncio.get_event_loop().run_until_complete(run())
```
