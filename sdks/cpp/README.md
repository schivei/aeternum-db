# AeternumDB C++ SDK

Low-level C++ SDK for AeternumDB.

## License

Apache 2.0

## Status

🚧 Under development

## Requirements

- C++17 or later
- CMake 3.15+

## Building

```bash
mkdir build && cd build
cmake ..
make
```

## Example

```cpp
#include <aeternumdb/client.hpp>
#include <iostream>

int main() {
    auto client = aeternumdb::Client::connect("localhost:5432");

    // Execute query
    auto result = client.query("SELECT * FROM users");

    for (const auto& row : result) {
        std::cout << row["name"] << std::endl;
    }

    return 0;
}
```
