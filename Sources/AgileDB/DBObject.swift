//
//  DBObject.swift
//  AgileDB
//
//  Created by Aaron Bratcher  on 4/25/19.
//  Copyright © 2019 Aaron Bratcher. All rights reserved.
//

import Foundation

public protocol DBObject: Codable {
	static var table: DBTable { get }
	var key: String { get set }
	var codingKeys: [CodingKey] { get }
}

extension DBObject {
	/**
	 Default response for codingKeys is empty so all DBObject properties are encoded
	 */
	public var codingKeys: [CodingKey] {
		return []
	}

	/**
	Instantiate object and populate with values from the database. If instantiation fails, nil is returned.

	- parameter db: Database object holding the data.
	- parameter key: Key of the data entry.
	*/
	public init?(db: AgileDB, key: String) {
		guard let dictionaryValue = db.dictValueFromTable(Self.table, for: key)
		, let dbObject: Self = Self.dbObjectWithDict(dictionaryValue, db: db, for: key)
			else { return nil }

		self = dbObject
	}

	/**
	Asynchly instantiate object and populate with values from the database. If instantiation fails, nil is returned.

	- parameter db: Database object holding the data.
	- parameter key: Key of the data entry.
	*/
	public init?(db: AgileDB, key: String) async {
		guard let dictionaryValue = try? await db.dictValueFromTable(Self.table, for: key)
		, let dbObject: Self = Self.dbObjectWithDict(dictionaryValue, db: db, for: key)
			else { return nil }

		self = dbObject
	}


	/**
	Save the object to the database. This will update the values in the database if the object is already present. At this time, this is not an atomic operation for nested Objects.

	- parameter db: Database object to hold the data.
	- parameter expiration: Optional Date specifying when the data is to be automatically deleted. Default value is nil specifying no automatic deletion.
	- parameter saveNestedObjects: Save nested DBObjects and arrays of DBObjects. (autoDeleteAfter is not propegated to nested objects saves) Default value is true.

	- returns: Discardable Bool value of a successful save.
	*/
	@discardableResult
	public func save(to db: AgileDB, autoDeleteAfter expiration: Date? = nil, saveNestedObjects: Bool = true) -> Bool {
		if saveNestedObjects {
			let mirror = Mirror(reflecting: self)
			for child in mirror.children {
				if let dbObject = child.value as? DBObject {
					dbObject.save(to: db)
				}

				if let objectArray = child.value as? [DBObject] {
					for dbObject in objectArray {
						dbObject.save(to: db)
					}
				}
			}
		}

		guard let dictValue = dictValue
		, db.setValueInTable(Self.table, for: key, to: dictValue, autoDeleteAfter: expiration)
			else { return false }

		return true
	}

	/**
	Asynchronously save the object to the database. This will update the values in the database if the object is already present. At this time, this is not an atomic operation for nested Objects.

	- parameter db: Database object to hold the data.
	- parameter expiration: Optional Date specifying when the data is to be automatically deleted. Default value is nil specifying no automatic deletion.
	- parameter saveNestedObjects: Save nested DBObjects and arrays of DBObjects. (autoDeleteAfter is not propegated to nested objects saves) Default value is true.

	*/
	@discardableResult
	public func save(to db: AgileDB, autoDeleteAfter expiration: Date? = nil, saveNestedObjects: Bool = true) async -> Bool {
		if saveNestedObjects {
			let mirror = Mirror(reflecting: self)
			for child in mirror.children {
				if let dbObject = child.value as? DBObject {
					await dbObject.save(to: db)
				}

				if let objectArray = child.value as? [DBObject] {
					for dbObject in objectArray {
						await dbObject.save(to: db)
					}
				}
			}
		}

		guard let dictValue = dictValue
		, db.setValueInTable(Self.table, for: key, to: dictValue, autoDeleteAfter: expiration)
			else { return false }

		return true
	}


	/**
	Remove the object from the database

	- parameter db: Database object that holds the data. This does not delete nested objects.

	- returns: Discardable Bool value of a successful deletion.
	*/
	@discardableResult
	public func delete(from db: AgileDB) -> Bool {
		return db.deleteFromTable(Self.table, for: key)
	}

	/**
	Asynchronously remove the object from the database

	- parameter db: Database object that holds the data. This does not delete nested objects.

	- returns: Discardable Bool value of a successful deletion.
	*/
	@discardableResult
	public func delete(from db: AgileDB) async -> Bool {
		return await db.deleteFromTable(Self.table, for: key)
	}

	/**
    Asynchronously instantiate object and populate with values from the database.
    
    - parameter db: Database object to hold the data.
    - parameter key: Key of the data entry.
    
    - returns: DBObject.
    - throws: DBError
    */

	public static func load(from db: AgileDB, for key: String) async throws -> Self {
		let dictionaryValue = try await db.dictValueFromTable(table, for: key)
		guard let dbObject = dbObjectWithDict(dictionaryValue, db: db, for: key) else {
			throw DBError.cannotParseData
		}

		return dbObject
	}

	/**
	Asynchronously instantiate object and populate with values from the database before executing the passed block with object. If object could not be instantiated properly, block is not executed.
	
	- parameter db: Database object to hold the data.
	- parameter key: Key of the data entry.
	- parameter queue: DispatchQueue to run the execution block on. Default value is nil specifying the main queue.
	- parameter block: Block of code to execute with instantiated object.
	
	- returns: DBCommandToken that can be used to cancel the call before it executes. Nil is returned if database could not be opened.
	*/
	@available(*, deprecated, message: "Use await load instead")
	@discardableResult
	public static func loadObjectFromDB(_ db: AgileDB, for key: String, queue: DispatchQueue? = nil, completion: @escaping (Self) -> Void) -> DBCommandToken? {
		let token = db.dictValueFromTable(table, for: key, queue: queue, completion: { (results) in
			if case .success(let dictionaryValue) = results
				, let dbObject = dbObjectWithDict(dictionaryValue, db: db, for: key) {
				completion(dbObject)
			}
		})

		return token
	}

	private static func dbObjectWithDict(_ dictionaryValue: [String: AnyObject], db: AgileDB, for key: String) -> Self? {
		var dictionaryValue = dictionaryValue

		dictionaryValue["key"] = key as AnyObject
		let decoder = DBObjectDecoder(dictionaryValue, db: db)
		return try? Self(from: decoder)
	}

	/**
	JSON string value based on the what's saved in the encode method
	*/
	public var jsonValue: String? {
		let jsonEncoder = JSONEncoder()
		jsonEncoder.dateEncodingStrategy = .formatted(AgileDB.dateFormatter)

		do {
			let jsonData = try jsonEncoder.encode(self)
			let jsonString = String(data: jsonData, encoding: .utf8)
			return jsonString
		}

		catch _ {
			return nil
		}
	}

	/**
	Dictionary value of object for use in setting value in database. Nested DBObjects are not encoded into the dictionary, only the key is referenced.
	*/
	public var dictValue: [String: AnyObject]? {
		let dictEncoder = DBObjectEncoder()

		do {
			let dictValue = try dictEncoder.encode(dbObject: self)
			return dictValue
		}

		catch _ {
			return nil
		}
	}
}
