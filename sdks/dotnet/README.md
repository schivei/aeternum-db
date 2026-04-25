# AeternumDB .NET Core SDK

Cross-platform .NET SDK for AeternumDB (C#).

## License

Apache 2.0

## Status

🚧 Under development

## Installation

```bash
dotnet add package AeternumDB.SDK
```

## Example

```csharp
using AeternumDB;

class Program
{
    static async Task Main(string[] args)
    {
        await using var client = await Client.ConnectAsync("localhost:5432");

        // Execute query
        var result = await client.QueryAsync("SELECT * FROM users");

        await foreach (var row in result)
        {
            Console.WriteLine(row["name"]);
        }
    }
}
```
