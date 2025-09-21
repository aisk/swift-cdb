import XCTest
@testable import CDB

final class CDBTests: XCTestCase {
    func testExample() throws {
        // XCTest Documentation
        // https://developer.apple.com/documentation/xctest

        // Defining Test Cases and Test Methods
        // https://developer.apple.com/documentation/xctest/defining_test_cases_and_test_methods

        let db1 = try CDB(filename: "example.cdb", mode: .write)
        try db1.add(key: "foo", value: "bar")
        try db1.add(key: "hello", value: "world")
        let testData = Data([0x01, 0x02, 0x03, 0x04])
        try db1.add(key: "binary", value: testData)
        try db1.close()

        let db2 = try CDB(filename: "example.cdb", mode: .read)
        let value1: String? = try db2.get(key: "foo")
        XCTAssertEqual(value1, Optional("bar"))
        let count1 = try db2.count(key: "foo")
        XCTAssertEqual(count1, 1)
        let value2: String? = try db2.get(key: "not_exist")
        XCTAssertEqual(value2, nil)
        let count2 = try db2.count(key: "not_exist")
        XCTAssertEqual(count2, 0)

        var items: [String: String] = [:]
        for (key, value) in db2 {
            items[key] = value
        }
        XCTAssertEqual(items, ["foo": "bar", "hello": "world", "binary": "\u{01}\u{02}\u{03}\u{04}"])

        XCTAssertEqual(db2["foo"], "bar")
        XCTAssertEqual(db2["hello"], "world")
        XCTAssertNil(db2["nonexistent"])

        let retrievedData: Data? = try db2.get(key: "binary")
        XCTAssertEqual(retrievedData, testData)

        try db2.close()
    }
}
