# swift-cdb

A Swift wrapper for the CDB (Constant Database) implementation from [howerj/cdb](https://github.com/howerj/cdb), providing a native Swift interface for creating and querying constant key-value databases.

## Installation

Add this package to your Swift project dependencies:

```swift
// In your Package.swift
dependencies: [
    .package(url: "https://github.com/aisk/swift-cdb.git", from: "0.1.0"),
]
```

## Usage

```swift
import CDB

// Open a CDB file and read values
do {
    let db = try CDB(filename: "example.cdb", mode: .read)

    // Read string value
    let value: String? = try db.get(key: "some_key")
    print("Value: \(value ?? "not found")")

    try db.close()
} catch {
    print("Error: \(error)")
}
```

## API Reference

### Main Methods

- `init(filename: String, mode: Mode) throws` - Open a CDB file
- `add(key: String, value: String) throws` - Add a string value
- `add(key: String, value: Data) throws` - Add binary data
- `get(key: String, at index: UInt64 = 0) throws -> String?` - Get string value
- `get(key: String, at index: UInt64 = 0) throws -> Data?` - Get binary data
- `count(key: String) throws -> UInt64` - Count values for a key
- `close() throws` - Close the database
- `subscript(key: String) -> String?` - Dictionary-like access

### Modes

- `.read` - Open for reading only
- `.write` - Open for writing (creates new database)

## License

This project is licensed under the same terms as the original [CDB library](https://github.com/howerj/cdb).
