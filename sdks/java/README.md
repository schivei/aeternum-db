# AeternumDB Java/Kotlin SDK

Java and Kotlin SDK for AeternumDB.

## License

Apache 2.0

## Status

🚧 Under development

## Installation

### Maven

```xml
<dependency>
    <groupId>com.aeternumdb</groupId>
    <artifactId>aeternumdb-sdk</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Gradle

```groovy
implementation 'com.aeternumdb:aeternumdb-sdk:0.1.0'
```

## Example (Java)

```java
import com.aeternumdb.Client;

public class Example {
    public static void main(String[] args) throws Exception {
        Client client = Client.connect("localhost:5432");

        // Execute query
        ResultSet result = client.query("SELECT * FROM users");

        while (result.next()) {
            System.out.println(result.getString("name"));
        }

        client.close();
    }
}
```

## Example (Kotlin)

```kotlin
import com.aeternumdb.Client

fun main() {
    Client.connect("localhost:5432").use { client ->
        // Execute query
        val result = client.query("SELECT * FROM users")

        result.forEach { row ->
            println(row["name"])
        }
    }
}
```
