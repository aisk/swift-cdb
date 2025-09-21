// The Swift Programming Language
// https://docs.swift.org/swift-book

import cdbc

struct CDBError: Error, LocalizedError {
    let errno: Int

    var errorDescription: String? {
        return "CDB operation failed with error code: \(errno)"
    }
}

enum Mode: Int32 {
    case read = 0
    case write = 1
}

class CDB {
    private var db: OpaquePointer?
    private var isClosed = false

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
        guard !isClosed else {
            throw CDBError(errno: -1)
        }

        try key.withCString { cKey in
            try value.withCString { cValue in
                var keyBuffer = cdb_buffer_t(length: UInt64(key.utf8.count), buffer: UnsafeMutablePointer(mutating: cKey))
                var valueBuffer = cdb_buffer_t(length: UInt64(value.utf8.count), buffer: UnsafeMutablePointer(mutating: cValue))

                let res = cdb_add(db, &keyBuffer, &valueBuffer)
                if res != 0 {
                    throw CDBError(errno: Int(res))
                }
            }
        }
    }

    func get(key: String, at index: UInt64=0) throws -> String? {
        guard !isClosed else {
            throw CDBError(errno: -1)
        }

        return try key.withCString { cKey in
            var keyBuffer = cdb_buffer_t(length: UInt64(key.utf8.count), buffer: UnsafeMutablePointer(mutating: cKey))
            var value_info = cdb_file_pos_t(position: 0, length: 0)

            let res = cdb_lookup(self.db, &keyBuffer, &value_info, index)
            if res == 0 {
                return nil
            }
            if res != 1 {
                throw CDBError(errno: Int(res))
            }

            return try read(at: value_info)
        }
    }

    func count(key: String) throws -> UInt64 {
        guard !isClosed else {
            throw CDBError(errno: -1)
        }

        return try key.withCString { cKey in
            var keyBuffer = cdb_buffer_t(length: UInt64(key.utf8.count), buffer: UnsafeMutablePointer(mutating: cKey))
            var result: UInt64 = 0

            let res = cdb_count(self.db, &keyBuffer, &result)
            if res != 0 {
                throw CDBError(errno: Int(res))
            }

            return result
        }
    }

    func close() throws {
        guard !isClosed else { return }
        let res = cdb_close(db)
        if res != 0 {
            throw CDBError(errno: Int(res))
        }
        isClosed = true
    }

    fileprivate func read(at pos: cdb_file_pos_t) throws -> String {
        guard !isClosed else {
            throw CDBError(errno: -1)
        }

        let res = cdb_seek(self.db, pos.position)
        if res != 0 {
            throw CDBError(errno: Int(res))
        }
        let buffer: UnsafeMutablePointer<Int8> = UnsafeMutablePointer<Int8>.allocate(capacity: Int(pos.length) + 1)
        defer { buffer.deallocate() }
        let read_res = cdb_read(self.db, buffer, pos.length)
        if read_res != 0 {
            throw CDBError(errno: Int(read_res))
        }
        buffer[Int(pos.length)] = 0 // Null terminate
        return String(cString: buffer)
    }

    deinit {
        try? close()
    }
}

public struct CDBIterator: IteratorProtocol {
    public typealias Element = (key: String, value: String)

    private var items: [Element]
    private var currentIndex = 0

    init(items: [Element]) {
        self.items = items
    }

    public mutating func next() -> Element? {
        if currentIndex < items.count {
            let item = items[currentIndex]
            currentIndex += 1
            return item
        }
        return nil
    }
}

extension CDB: Sequence {
    public func makeIterator() -> CDBIterator {
        guard !isClosed else {
            return CDBIterator(items: [])
        }

        let helper = IteratorHelper(cdb: self)
        let helperPtr = Unmanaged.passUnretained(helper).toOpaque()

        let callback: cdb_callback = { cdb, key, value, param in
            let helper = Unmanaged<IteratorHelper>.fromOpaque(param!).takeUnretainedValue()
            do {
                try helper.append(keyPos: key!.pointee, valuePos: value!.pointee)
                return 0
            } catch {
                return -1
            }
        }

        cdb_foreach(self.db, callback, helperPtr)

        return CDBIterator(items: helper.items)
    }
}

private class IteratorHelper {
    var items: [(key: String, value: String)] = []
    private weak var cdb: CDB?

    init(cdb: CDB) {
        self.cdb = cdb
    }

    func append(keyPos: cdb_file_pos_t, valuePos: cdb_file_pos_t) throws {
        guard let cdb = cdb else { return }
        let key = try cdb.read(at: keyPos)
        let value = try cdb.read(at: valuePos)
        items.append((key: key, value: value))
    }
}
