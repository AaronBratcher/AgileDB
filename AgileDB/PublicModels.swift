//
//  DBModels.swift
//  AgileDB
//
//  Created by Aaron Bratcher  on 5/1/20.
//  Copyright © 2020 – 2026 Aaron Bratcher. All rights reserved.
//

import Foundation

public typealias BoolResults = Result<Bool, DBError>
public typealias KeyResults = Result<[String], DBError>
public typealias IntResults = Result<Int, DBError>
public typealias RowResults = Result<[DBRow], DBError>
public typealias JsonResults = Result<String, DBError>
public typealias DictResults = Result<[String: any Sendable], DBError>

/**
DBTable is used to identify the table data is stored in
*/
public struct DBTable: Equatable, Hashable, Sendable {
	let name: String

	public init(name: String) {
		assert(name != "", "name cannot be empty")
		assert(!AgileDB.reservedTable(name), "reserved table")
		self.name = name
	}
}

extension DBTable: ExpressibleByStringLiteral {
	public init(stringLiteral name: String) {
		self.init(name: name)
	}
}

extension DBTable: CustomStringConvertible {
	public var description: String {
		return name
	}
}

/**
DBCommandToken is returned by asynchronous methods. Call the token's cancel method to cancel the command before it executes.
*/
public struct DBCommandToken: Sendable {
	private let database: AgileDB
	private let identifier: UInt

	init(database: AgileDB, identifier: UInt) {
		self.database = database
		self.identifier = identifier
	}

	/**
    Cancel the asynchronous command before it executes.

    - returns: Bool Returns if the cancel was successful.
    */
	@discardableResult
	public func cancel() -> Bool {
		// dequeueCommand is nonisolated so this stays synchronous
		return database.dequeueCommand(identifier)
	}
}

public enum DBConditionOperator: String {
	case equal = "="
	case notEqual = "<>"
	case lessThan = "<"
	case greaterThan = ">"
	case lessThanOrEqual = "<="
	case greaterThanOrEqual = ">="
	case contains = "..."
	case inList = "()"
	/// Fuzzy string equality: compares only the letters and digits of each side, ignoring case
	/// and diacritics, so "Sams Club" matches "Sam's Club". See `AgileDB.alphanumericKey(_:)`.
	case almostEqual = "~="
	/// Fuzzy substring match on the same letters-and-digits form as `almostEqual`, so "sams"
	/// matches "Sam's Club". A value with no letters or digits matches nothing.
	case almostContains = "~..."
}

public struct DBCondition: @unchecked Sendable {
	public var set = 0
	public var objectKey = ""
	public var conditionOperator = DBConditionOperator.equal
	public var value: any Sendable

	public init(set: Int, objectKey: String, conditionOperator: DBConditionOperator, value: any Sendable) {
		self.set = set
		self.objectKey = objectKey
		self.conditionOperator = conditionOperator
		self.value = value
	}
}

public struct DBRow: @unchecked Sendable {
	public var values = [(any Sendable)?]()
}

/**
Identifies a specific DBObject by its table and key. Used to describe references between
saved objects, e.g. the retained references reported by a cascading `DBObject.delete`.
*/
public struct DBReference: Equatable, Hashable, Sendable {
	public let table: DBTable
	public let key: String

	public init(table: DBTable, key: String) {
		self.table = table
		self.key = key
	}
}

/**
Outcome of a `DBObject.delete(from:cascadeDelete:)` call.
*/
public enum DBDeleteResult: Equatable, Sendable {
	/// The object's own row could not be deleted.
	case failed
	/// The object and every referenced object that had no other referrers were deleted.
	case completed
	/// The object itself was deleted, but one or more referenced objects were left in place
	/// because something else still refers to them.
	case partial(retained: [DBReference])
}

public enum DBError: Error, Equatable, Sendable {
	case cannotWriteToFile
	case diskError
	case damagedFile
	case cannotOpenFile
	case tableNotFound
	case cannotParseData
	case other(Int)
}

extension DBError: RawRepresentable {
	public typealias RawValue = Int

	public init(rawValue: RawValue) {
		switch rawValue {
		case 8: self = .cannotWriteToFile
		case 10: self = .diskError
		case 11: self = .damagedFile
		case 14: self = .cannotOpenFile
		case -1: self = .tableNotFound
		case -2: self = .cannotParseData
		default: self = .other(rawValue)
		}
	}

	public var rawValue: RawValue {
		switch self {
		case .cannotWriteToFile: return 8
		case .diskError: return 10
		case .damagedFile: return 11
		case .cannotOpenFile: return 14
		case .tableNotFound: return -1
		case .cannotParseData: return -2
		case .other(let value): return value
		}
	}
}
