# AeternumDB Python SDK

Python SDK for AeternumDB - ideal for data science and automation.

## License

Apache 2.0

## Status

🚧 Under development

## Installation

```bash
pip install aeternumdb
```

## Example

```python
from aeternumdb import Client

async def main():
    client = await Client.connect('localhost:5432')

    # Execute query
    result = await client.query('SELECT * FROM users')

    for row in result:
        print(row)

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
```
