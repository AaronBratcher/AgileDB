//
//  DBObjectDecoder.swift
//  AgileDB
//
//  Created by Aaron Bratcher  on 6/7/20.
//  Copyright © 2020 Aaron Bratcher. All rights reserved.
//

import Foundation

protocol DBObjectArrayMarker {
	static var elementType: DBObject.Type { get }
}

extension Array: DBObjectArrayMarker where Element: DBObject {
	static var elementType: DBObject.Type {
		return Element.self
	}
}

/**
Shared, mutable state used while decoding a DBObject graph.

Because `Decodable.init(from:)` is synchronous but loading nested DBObjects from the
actor requires `await`, nested object dictionaries are pre-loaded asynchronously and
stored in `cache`. When a needed nested object is not yet cached, the decoder records
the request in `misses` and aborts with `NeedsNestedLoad`; the async driver then loads
the missing dictionaries and retries the decode.
*/
final class DBObjectDecoderState {
	var cache: [String: [String: any Sendable]] = [:]
	var misses: [(table: DBTable, key: String)] = []

	static func cacheKey(table: DBTable, key: String) -> String {
		return "\(table.name)::\(key)"
	}
}

/// Thrown during decoding when a nested DBObject's data has not yet been loaded.
struct NeedsNestedLoad: Error {}

class DBObjectDecoder: Decoder {
	var codingPath: [CodingKey] = []
	var userInfo: [CodingUserInfoKey: Any] = [:]

	let dict: [String: any Sendable]
	let db: AgileDB
	let state: DBObjectDecoderState

	init(_ dict: [String: any Sendable], db: AgileDB, state: DBObjectDecoderState = DBObjectDecoderState()) {
		self.dict = dict
		self.db = db
		self.state = state
	}

	func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key: CodingKey {
		return KeyedDecodingContainer(DictKeyedContainer<Key>(dict: dict, db: db, state: state))
	}

	func unkeyedContainer() throws -> UnkeyedDecodingContainer {
		fatalError("AgileDB doesn't support unkeyed decoding")
	}

	func singleValueContainer() throws -> SingleValueDecodingContainer {
		fatalError("AgileDB doesn't support single value decoding")
	}
}

private enum DictDecoderError: Error {
	case missingValueForKey(String)
	case invalidDate(String)
	case invalidURL(String)
	case invalidUUID(String)
	case invalidJSON(String)
	case invalidNestedObject(String, String)
}

private extension Bool {
	init<T : Numeric>(_ number: T) {
		if number == 0 {
			self.init(false)
		} else {
			self.init(true)
		}
	}

	init(_ string: String) {
		self.init(string == "1" || string.uppercased() == "YES" || string.uppercased() == "TRUE")
	}
}

private class DictKeyedContainer<K: CodingKey>: KeyedDecodingContainerProtocol {
	typealias Key = K

	let codingPath: [CodingKey] = []
	var allKeys: [K] { return dict.keys.compactMap { K(stringValue: $0) } }

	private let dict: [String: any Sendable]
	private let db: AgileDB
	private let state: DBObjectDecoderState

	init(dict: [String: any Sendable], db: AgileDB, state: DBObjectDecoderState) {
		self.dict = dict
		self.db = db
		self.state = state
	}

	func contains(_ key: K) -> Bool {
		return dict[key.stringValue] != nil
	}

	func decodeNil(forKey key: K) throws -> Bool {
		if dict[key.stringValue] == nil {
			throw DictDecoderError.missingValueForKey(key.stringValue)
		}

		return false
	}

	func decodeObject(_ type: DBObject.Type, forKey key: K) throws -> DBObject {
		guard let storedKey = dict[key.stringValue] as? String else {
			throw DictDecoderError.missingValueForKey(key.stringValue)
		}

		guard let nestedDict = state.cache[DBObjectDecoderState.cacheKey(table: type.table, key: storedKey)] else {
			// Data not loaded yet — record the need and abort so the async driver can load it.
			state.misses.append((type.table, storedKey))
			throw NeedsNestedLoad()
		}

		return try decodeNested(type, dict: nestedDict, key: storedKey)
	}

	func decodeObjectArray(_ type: DBObjectArrayMarker.Type, forKey key: K) throws -> [DBObject] {
		guard let storedKeys = dict[key.stringValue] as? [String] else {
			throw DictDecoderError.missingValueForKey(key.stringValue)
		}

		let elementType = type.elementType

		// Record every element that hasn't been loaded yet before aborting, so a single
		// retry can load the whole array rather than one element at a time.
		var needsLoad = false
		for storedKey in storedKeys where state.cache[DBObjectDecoderState.cacheKey(table: elementType.table, key: storedKey)] == nil {
			state.misses.append((elementType.table, storedKey))
			needsLoad = true
		}
		if needsLoad {
			throw NeedsNestedLoad()
		}

		var objects: [DBObject] = []
		for storedKey in storedKeys {
			let nestedDict = state.cache[DBObjectDecoderState.cacheKey(table: elementType.table, key: storedKey)]!
			objects.append(try decodeNested(elementType, dict: nestedDict, key: storedKey))
		}

		return objects
	}

	private func decodeNested(_ type: DBObject.Type, dict: [String: any Sendable], key: String) throws -> DBObject {
		var nestedDict = dict
		nestedDict["key"] = key
		let nestedDecoder = DBObjectDecoder(nestedDict, db: db, state: state)
		return try type.init(from: nestedDecoder)
	}

	func decode(_ type: Bool.Type, forKey key: K) throws -> Bool {
		guard let value = dict[key.stringValue] else {
			throw DictDecoderError.missingValueForKey(key.stringValue)
		}

		if let intValue = value as? Int {
			return Bool(intValue)
		}

		if let stringValue = value as? String {
			return Bool(stringValue)
		}

		throw DictDecoderError.missingValueForKey(key.stringValue)
	}

	func decode(_ type: Int.Type, forKey key: K) throws -> Int {
		guard let value = dict[key.stringValue] else {
			throw DictDecoderError.missingValueForKey(key.stringValue)
		}

		if let intValue = value as? Int {
			return intValue
		}

		guard let stringValue = value as? String
		, let intValue = Int(stringValue)
			else {
			throw DictDecoderError.missingValueForKey(key.stringValue)
		}

		return intValue
	}

	func decodeArray(_ type: [Int].Type, forKey key: K) throws -> [Int] {
		guard let values = dict[key.stringValue] as? [AnyObject] else {
			throw DictDecoderError.missingValueForKey(key.stringValue)
		}

		var intValues = [Int]()

		for value in values {
			if let intValue = value as? Int {
				intValues.append(intValue)
				continue
			}

			guard let stringValue = value as? String
			, let intValue = Int(stringValue)
				else { throw DictDecoderError.missingValueForKey(key.stringValue) }

			intValues.append(intValue)
		}

		return intValues
	}

	func decode(_ type: Double.Type, forKey key: K) throws -> Double {
		guard let value = dict[key.stringValue] else {
			throw DictDecoderError.missingValueForKey(key.stringValue)
		}

		if let doubleValue = value as? Double {
			return doubleValue
		}

		guard let stringValue = value as? String
		, let doubleValue = Double(stringValue)
			else {
			throw DictDecoderError.missingValueForKey(key.stringValue)
		}

		return doubleValue
	}

	func decodeArray(_ type: [Double].Type, forKey key: K) throws -> [Double] {
		guard let values = dict[key.stringValue] as? [AnyObject] else {
			throw DictDecoderError.missingValueForKey(key.stringValue)
		}

		var doubleValues = [Double]()

		for value in values {
			if let doubleValue = value as? Double {
				doubleValues.append(doubleValue)
				continue
			}

			guard let stringValue = value as? String
			, let doubleValue = Double(stringValue)
				else { throw DictDecoderError.missingValueForKey(key.stringValue) }

			doubleValues.append(doubleValue)
		}

		return doubleValues
	}

	func decode(_ type: String.Type, forKey key: K) throws -> String {
		guard let value = dict[key.stringValue] as? String else {
			throw DictDecoderError.missingValueForKey(key.stringValue)
		}
		return value
	}

	func decodeArray(_ type: [String].Type, forKey key: K) throws -> [String] {
		guard let value = dict[key.stringValue] as? [String] else {
			throw DictDecoderError.missingValueForKey(key.stringValue)
		}
		return value
	}

	func decode(_ type: Data.Type, forKey key: K) throws -> Data {
		guard let value = dict[key.stringValue] as? Data else {
			throw DictDecoderError.missingValueForKey(key.stringValue)
		}
		return value
	}

	func decode(_ type: Date.Type, forKey key: K) throws -> Date {
		let string = try decode(String.self, forKey: key)
		if let date = AgileDB.dateFormatter.date(from: string) {
			return date
		} else {
			throw DictDecoderError.invalidDate(string)
		}
	}

	func decode(_ type: URL.Type, forKey key: K) throws -> URL {
		let string = try decode(String.self, forKey: key)
		if let url = URL(string: string) {
			return url
		} else {
			throw DictDecoderError.invalidURL(string)
		}
	}

	func decode(_ type: UUID.Type, forKey key: K) throws -> UUID {
		let string = try decode(String.self, forKey: key)
		if let uuid = UUID(uuidString: string) {
			return uuid
		} else {
			throw DictDecoderError.invalidUUID(string)
		}
	}

	func decode<T>(_ type: T.Type, forKey key: K) throws -> T where T: Decodable {
		if let dynamicType = T.self as? DBObject.Type {
			return try decodeObject(dynamicType, forKey: key) as! T
		} else if let dynamicType = T.self as? DBObjectArrayMarker.Type {
			return try decodeObjectArray(dynamicType, forKey: key) as! T
		} else if Data.self == T.self {
			return try decode(Data.self, forKey: key) as! T
		} else if Date.self == T.self {
			return try decode(Date.self, forKey: key) as! T
		} else if URL.self == T.self {
			return try decode(URL.self, forKey: key) as! T
		} else if UUID.self == T.self {
			return try decode(UUID.self, forKey: key) as! T
		} else if Bool.self == T.self {
			return try decode(Bool.self, forKey: key) as! T
		} else if [Int].self == T.self {
			let intArray = try decodeArray([Int].self, forKey: key)
			guard let jsonData = try? JSONSerialization.data(withJSONObject: intArray, options: .prettyPrinted) else {
				throw DictDecoderError.invalidJSON("Unknown data structure")
			}
			return try JSONDecoder().decode(T.self, from: jsonData)
		} else if [Double].self == T.self {
			let doubleArray = try decodeArray([Double].self, forKey: key)
			guard let jsonData = try? JSONSerialization.data(withJSONObject: doubleArray, options: .prettyPrinted) else {
				throw DictDecoderError.invalidJSON("Unknown data structure")
			}
			return try JSONDecoder().decode(T.self, from: jsonData)
		} else if [String].self == T.self {
			let stringArray = try decodeArray([String].self, forKey: key)
			guard let jsonData = try? JSONSerialization.data(withJSONObject: stringArray, options: .prettyPrinted) else {
				throw DictDecoderError.invalidJSON("Unknown data structure")
			}
			return try JSONDecoder().decode(T.self, from: jsonData)
		} else if [Date].self == T.self {
			let dateArray = try decodeArray([String].self, forKey: key)
			guard let jsonData = try? JSONSerialization.data(withJSONObject: dateArray, options: .prettyPrinted) else {
				throw DictDecoderError.invalidJSON("Unknown data structure")
			}
			let decoder = JSONDecoder()
			decoder.dateDecodingStrategy = .formatted(AgileDB.dateFormatter)
			return try decoder.decode(T.self, from: jsonData)
		} else {
			let jsonText = try decode(String.self, forKey: key)
			guard let jsonData = jsonText.data(using: .utf8) else {
				throw DictDecoderError.invalidJSON(jsonText)
			}
			return try JSONDecoder().decode(T.self, from: jsonData)
		}
	}

	func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: K) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
		fatalError("_KeyedContainer does not support nested containers.")
	}

	func nestedUnkeyedContainer(forKey key: K) throws -> UnkeyedDecodingContainer {
		fatalError("_KeyedContainer does not support nested containers.")
	}

	func superDecoder() throws -> Decoder {
		fatalError("_KeyedContainer does not support nested containers.")
	}

	func superDecoder(forKey key: K) throws -> Decoder {
		fatalError("_KeyedContainer does not support nested containers.")
	}
}
