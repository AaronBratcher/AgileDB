//
//  DBObjectEncoder.swift
//  AgileDB
//
//  Created by Aaron Bratcher on 6/8/20.
//  Copyright © 2020 Aaron Bratcher. All rights reserved.
//

import Foundation

class DBObjectEncoder: Encoder {
	let codingPath: [CodingKey] = []
	let userInfo: [CodingUserInfoKey: Any] = [:]

	typealias DBDict = [String: AnyObject]

	var dbDict: DBDict = [:]

	func encode(dbObject: DBObject) throws -> DBDict {
		try dbObject.encode(to: self)
		return dbDict
	}

	func container<Key>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> where Key: CodingKey {
		return KeyedEncodingContainer(KeyedContainer<Key>(self))
	}

	func unkeyedContainer() -> UnkeyedEncodingContainer {
		fatalError("AgileDB doesn't support unkeyed decoding")
	}

	func singleValueContainer() -> SingleValueEncodingContainer {
		fatalError("AgileDB doesn't support unkeyed decoding")
	}
}

private class KeyedContainer<K: CodingKey>: KeyedEncodingContainerProtocol {
	let codingPath: [CodingKey] = []
	typealias Key = K
	let encoder: DBObjectEncoder

	init(_ encoder: DBObjectEncoder) {
		self.encoder = encoder
	}

	func encodeNil(forKey key: K) throws { }

	func encode(_ value: Bool, forKey key: K) throws {
		encoder.dbDict[key.stringValue] = value as AnyObject
	}

	func encodeDate(_ date: Date, forKey key: K) throws {
		let dateString = AgileDB.stringValueForDate(date)
		encoder.dbDict[key.stringValue] = dateString as AnyObject
	}

	func encodeDateArray(_ dateArray: [Date], forKey key: K) throws {
		var dateStrings: [String] = []
		for date in dateArray {
			let dateString = AgileDB.stringValueForDate(date)
			dateStrings.append(dateString)
		}

		encoder.dbDict[key.stringValue] = dateStrings as AnyObject
	}

	func encodeDBObject(_ dbObject: DBObject, forKey key: K) throws {
		encoder.dbDict[key.stringValue] = dbObject.key as AnyObject
	}

	func encodeDBObjectArray(_ dbObjects: [DBObject], forKey key: K) throws {
		var objectKeys: [String] = []
		for dbObject in dbObjects {
			objectKeys.append(dbObject.key)
		}

		encoder.dbDict[key.stringValue] = objectKeys as AnyObject
	}

	func encode(_ value: String, forKey key: K) throws {
		if key.stringValue == "key" { return }
		encoder.dbDict[key.stringValue] = value as AnyObject
	}

	func encodeStringArray(_ value: [String], forKey key: K) throws {
		encoder.dbDict[key.stringValue] = value as AnyObject
	}

	func encodeIntArray(_ value: [Int], forKey key: K) throws {
		encoder.dbDict[key.stringValue] = value as AnyObject
	}

	func encodeDoubleArray(_ value: [Double], forKey key: K) throws {
		encoder.dbDict[key.stringValue] = value as AnyObject
	}

	func encode(_ value: Double, forKey key: K) throws {
		encoder.dbDict[key.stringValue] = value as AnyObject
	}

	func encode(_ value: Float, forKey key: K) throws { }

	func encode(_ value: Int, forKey key: K) throws {
		encoder.dbDict[key.stringValue] = value as AnyObject
	}

	func encode(_ value: Int8, forKey key: K) throws {
		encoder.dbDict[key.stringValue] = Int(value) as AnyObject
	}

	func encode(_ value: Int16, forKey key: K) throws {
		encoder.dbDict[key.stringValue] = Int(value) as AnyObject
	}

	func encode(_ value: Int32, forKey key: K) throws {
		encoder.dbDict[key.stringValue] = Int(value) as AnyObject
	}

	func encode(_ value: Data, forKey key: K) throws {
		encoder.dbDict[key.stringValue] = value as AnyObject
	}

	func encode(_ value: Int64, forKey key: K) throws { }
	func encode(_ value: UInt, forKey key: K) throws { }
	func encode(_ value: UInt8, forKey key: K) throws { }
	func encode(_ value: UInt16, forKey key: K) throws { }
	func encode(_ value: UInt32, forKey key: K) throws { }
	func encode(_ value: UInt64, forKey key: K) throws { }

	func encode<T>(_ value: T, forKey key: K) throws where T: Encodable {
		if let value = value as? Date {
			try encodeDate(value, forKey: key)
		} else if let value = value as? [Date] {
			try encodeDateArray(value, forKey: key)
		} else if let value = value as? [String] {
			try encodeStringArray(value, forKey: key)
		} else if let value = value as? [Int] {
			try encodeIntArray(value, forKey: key)
		} else if let value = value as? [Double] {
			try encodeDoubleArray(value, forKey: key)
		} else if let value = value as? DBObject {
			try encodeDBObject(value, forKey: key)
		} else if let value = value as? [DBObject] {
			try encodeDBObjectArray(value, forKey: key)
		} else {
			try encode(value, forKey: key)
		}
	}

	func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type, forKey key: K) -> KeyedEncodingContainer<NestedKey> where NestedKey: CodingKey {
		fatalError("_KeyedContainer does not support nested containers.")
	}

	func nestedUnkeyedContainer(forKey key: K) -> UnkeyedEncodingContainer {
		fatalError("_KeyedContainer does not support nested containers.")
	}

	func superEncoder() -> Encoder {
		fatalError("_KeyedContainer does not support nested containers.")
	}

	func superEncoder(forKey key: K) -> Encoder {
		fatalError("_KeyedContainer does not support nested containers.")
	}
}
