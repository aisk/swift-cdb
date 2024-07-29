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
        try db1.close()

        let db2 = try CDB(filename: "example.cdb", mode: .read)
        let value1 = try db2.get(key: "foo")
        XCTAssertEqual(value1, Optional("bar"))
        let value2 = try db2.get(key: "not_exist")
        XCTAssertEqual(value2, nil)
        try db2.close()
    }
}
