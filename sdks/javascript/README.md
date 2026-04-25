# AeternumDB JavaScript/TypeScript SDK

JavaScript and TypeScript SDK for AeternumDB.

## License

Apache 2.0

## Status

🚧 Under development

## Installation

```bash
npm install aeternumdb
# or
yarn add aeternumdb
```

## Example

```typescript
import { Client } from 'aeternumdb';

async function main() {
    const client = await Client.connect('localhost:5432');

    // Execute query
    const result = await client.query('SELECT * FROM users');

    console.log(result);
}

main();
```
