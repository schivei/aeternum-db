# AeternumDB Go SDK

Cloud-native Go SDK for AeternumDB.

## License

Apache 2.0

## Status

🚧 Under development

## Installation

```bash
go get github.com/schivei/aeternum-db/sdks/go
```

## Example

```go
package main

import (
    "context"
    "fmt"
    "github.com/schivei/aeternum-db/sdks/go"
)

func main() {
    ctx := context.Background()
    client, err := aeternumdb.Connect(ctx, "localhost:5432")
    if err != nil {
        panic(err)
    }
    defer client.Close()

    // Execute query
    result, err := client.Query(ctx, "SELECT * FROM users")
    if err != nil {
        panic(err)
    }

    fmt.Println(result)
}
```
