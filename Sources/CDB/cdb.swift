// The Swift Programming Language
// https://docs.swift.org/swift-book

import cdbc

struct CDBError: Error {
    let errno: Int
}

private func makeCString(from str: String) -> UnsafeMutablePointer<Int8> {
    let count = str.utf8CString.count 
    let result: UnsafeMutableBufferPointer<Int8> = UnsafeMutableBufferPointer<Int8>.allocate(capacity: count)
    _ = result.initialize(from: str.utf8CString)
    return result.baseAddress!
}

class CDBWriter {
    private var db: OpaquePointer?

    init(name: String) throws {
        var raw_options = cdb_host_options;
        let ops = withUnsafeMutablePointer(to: &raw_options) {
            UnsafeMutablePointer($0)
        }

        let res = cdb_open(&self.db, ops, 1, name)
        if res != 0 {
            throw CDBError(errno: Int(res))
        }
    }

    func add(key: String, value: String) throws {
        var key = cdb_buffer_t(length: UInt64(key.utf8CString.count), buffer: makeCString(from: key))
        var value = cdb_buffer_t(length: UInt64(value.utf8CString.count), buffer: makeCString(from: value))

        let res = cdb_add(db, &key, &value)
        if res != 0 {
            throw CDBError(errno: Int(res))
        }

    }

    func close() throws {
        let res = cdb_close(db)
        if res != 0 {
            throw CDBError(errno: Int(res))
        }
    }
}
