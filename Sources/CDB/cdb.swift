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

enum Mode: Int32 {
    case read = 0
    case write = 1
}

class CDB {
    private var db: OpaquePointer?

    init(filename: String, mode: Mode) throws {
        var raw_options = cdb_host_options
        let ops = withUnsafeMutablePointer(to: &raw_options) {
            UnsafeMutablePointer($0)
        }

        let res = cdb_open(&self.db, ops, mode.rawValue, filename)
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

    func get(key: String) throws -> String? {
        var key = cdb_buffer_t(length: UInt64(key.utf8CString.count), buffer: makeCString(from: key))
        var value_info = cdb_file_pos_t(position: 0, length: 0)

        var res = cdb_get(self.db, &key, &value_info)
        if res == 0 {
            return nil
        }
        if res != 1 {
            throw CDBError(errno: Int(res))
        }

        res = cdb_seek(self.db, value_info.position)
        if res != 0 {
            throw CDBError(errno: Int(res))
        }
        let buffer: UnsafeMutablePointer<Int8> = UnsafeMutablePointer<Int8>.allocate(capacity: Int(value_info.length))
        res = cdb_read(self.db, buffer, value_info.length)
        if res != 0 {
            throw CDBError(errno: Int(res))
        }
        let value = String(cString: buffer)

        return value
    }

    func close() throws {
        let res = cdb_close(db)
        if res != 0 {
            throw CDBError(errno: Int(res))
        }
    }
}
