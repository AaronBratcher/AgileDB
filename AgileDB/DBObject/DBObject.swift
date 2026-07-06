//
//  DBObject.swift
//  AgileDB
//
//  Created by Aaron Bratcher  on 4/25/19.
//  Copyright © 2019 – 2026 Aaron Bratcher. All rights reserved.
//

import Foundation

public protocol DBObject: Codable, Sendable {
	static var table: DBTable { get }
	static var currentSchemaVersion: Int { get }
	var key: String { get set }
	var codingKeys: [CodingKey] { get }

	/**
	Converts a dictionary saved under an older schema version to the current schema. Called
	automatically during decoding when `currentSchemaVersion` is greater than the version the
	data was saved with.

	- parameter dictValue: The stored dictionary, as saved under `schemaVersion`.
	- parameter schemaVersion: The schema version `dictValue` was saved with.

	- returns: A dictionary compatible with `currentSchemaVersion`.
	*/
	static func convertToCurrentSchema(_ dictValue: [String: any Sendable], from schemaVersion: Int) -> [String: any Sendable]
}

extension DBObject {
	/**
	 Default response for codingKeys is empty so all DBObject properties are encoded
	 */
	public var codingKeys: [CodingKey] {
		return []
	}

	/**
	 Default schema version is 1.
	 */
	public static var currentSchemaVersion: Int {
		return 1
	}

	/**
	 Default implementation performs no conversion.
	 */
	public static func convertToCurrentSchema(_ dictValue: [String: any Sendable], from schemaVersion: Int) -> [String: any Sendable] {
		return dictValue
	}

	/**
	Asynchronously instantiate object and populate with values from the database.

	- parameter db: Database object holding the data.
	- parameter key: Key of the data entry.
	*/
	public init?(db: AgileDB, key: String) async {
		guard let dictionaryValue = try? await db.dictValueFromTable(Self.table, for: key) as [String: any Sendable],
		      let dbObject: Self = await Self.dbObjectWithDict(dictionaryValue, db: db, for: key)
		else { return nil }

		self = dbObject
	}

	/**
	Save the object to the database. At this time, this is not an atomic operation for nested Objects.

	Also reconciles this object's references to any nested DBObject/[DBObject] properties, so
	that a later `delete(from:cascadeDelete:)` can tell whether a referenced object is still
	needed elsewhere. This bookkeeping happens whether or not `saveNestedObjects` is true.

	- parameter db: Database object to hold the data.
	- parameter expiration: Optional Date specifying when the data is to be automatically deleted.
	- parameter saveNestedObjects: Save nested DBObjects and arrays of DBObjects. Default value is true.

	- returns: Discardable Bool value of a successful save.
	*/
	@discardableResult
	public func save(to db: AgileDB, autoDeleteAfter expiration: Date? = nil, saveNestedObjects: Bool = true) async -> Bool {
		var references: [DBReference] = []

		let mirror = Mirror(reflecting: self)
		for child in mirror.children {
			if let dbObject = child.value as? DBObject {
				if saveNestedObjects { await dbObject.save(to: db) }
				references.append(DBReference(table: type(of: dbObject).table, key: dbObject.key))
			}

			if let objectArray = child.value as? [DBObject] {
				for dbObject in objectArray {
					if saveNestedObjects { await dbObject.save(to: db) }
					references.append(DBReference(table: type(of: dbObject).table, key: dbObject.key))
				}
			}
		}

		guard let dictValue = dictValue else { return false }

		let oldReferences = await db.internalReferences(for: Self.table, key: key)

		guard await db.setValueInTable(Self.table, for: key, to: dictValue, autoDeleteAfter: expiration) else { return false }

		await db.updateReferences(from: DBReference(table: Self.table, key: key), oldReferences: oldReferences, newReferences: references)

		return true
	}

	/**
	Remove the object from the database.

	By default this does not delete nested objects. With `cascadeDelete: true`, any nested
	DBObject/[DBObject] this object references is also deleted, but only if nothing else
	still references it — an object shared with another still-existing referrer is left in
	place. This walk isn't atomic: if the app is interrupted partway through a cascade, some
	now-unreferenced objects may be left behind rather than deleted.

	- parameter db: Database object that holds the data.
	- parameter cascadeDelete: Also delete referenced objects that would otherwise be left orphaned. Default value is false.

	- returns: DBDeleteResult indicating whether the delete failed outright, completed in full, or completed but left some still-referenced objects in place.
	*/
	@discardableResult
	public func delete(from db: AgileDB, cascadeDelete: Bool = false) async -> DBDeleteResult {
		guard cascadeDelete else {
			return await db.deleteFromTable(Self.table, for: key) ? .completed : .failed
		}

		let outcome = await db.cascadeDelete(DBReference(table: Self.table, key: key), visited: [])
		guard outcome.succeeded else { return .failed }
		return outcome.retained.isEmpty ? .completed : .partial(retained: outcome.retained)
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
		guard let dbObject = await dbObjectWithDict(dictionaryValue, db: db, for: key) else {
			throw DBError.cannotParseData
		}

		return dbObject
	}

	private static func dbObjectWithDict(_ dictionaryValue: [String: any Sendable], db: AgileDB, for key: String) async -> Self? {
		var dictionaryValue = dictionaryValue
		dictionaryValue["key"] = key as any Sendable

		let savedSchemaVersion = (dictionaryValue["schemaVersion"] as? Int) ?? 1
		if currentSchemaVersion > savedSchemaVersion {
			dictionaryValue = convertToCurrentSchema(dictionaryValue, from: savedSchemaVersion)
		}

		// Nested DBObjects are stored only by key. Decoding is synchronous but loading a
		// nested object requires `await`, so decode in a loop: each pass that encounters
		// not-yet-loaded nested objects records them and aborts, then the missing
		// dictionaries are loaded asynchronously and the decode is retried.
		let state = DBObjectDecoderState()

		while true {
			do {
				let decoder = DBObjectDecoder(dictionaryValue, db: db, state: state)
				return try Self(from: decoder)
			} catch is NeedsNestedLoad {
				let misses = state.misses
				state.misses = []
				if misses.isEmpty { return nil }

				for miss in misses {
					let cacheKey = DBObjectDecoderState.cacheKey(table: miss.table, key: miss.key)
					if state.cache[cacheKey] != nil { continue }
					guard let nestedDict = try? await db.dictValueFromTable(miss.table, for: miss.key) as [String: any Sendable] else {
						return nil
					}
					state.cache[cacheKey] = nestedDict
				}
			} catch {
				return nil
			}
		}
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
	public var dictValue: [String: any Sendable]? {
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
