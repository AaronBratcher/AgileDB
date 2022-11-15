//
// AgileDB.swift
//
// Created by Aaron Bratcher on 01/08/2015.
// Copyright (c) 2015 – 2020 Aaron L Bratcher. All rights reserved.
//

import Foundation
import SQLite3

public final class AgileDB {
	enum ValueType: String {
		case textArray = "stringArray"
		case intArray
		case doubleArray
		case text
		case int
		case double
		case bool
		case null
		case unknown

		static func fromRaw(_ rawValue: String) -> ValueType {
			if let valueType = ValueType(rawValue: rawValue.lowercased()) {
				return valueType
			}

			return .unknown
		}
	}

	public static let shared = AgileDB()

	/// Path of the database file. Nil if database hasn't been opened yet
	private(set) public var dbFilePath: String?

	/**
	Used for testing purposes. Slows the speed of the operations and gives lots of output.
	*/
	public var isDebugging = false {
		didSet {
			dbCore.isDebugging = isDebugging
		}
	}

	/**
	The number of seconds to wait after inactivity before automatically closing the file. File is automatically opened for next activity. A value of 0 means never close automatically
	*/
	public var autoCloseTimeout = 0

	/**
	Print the SQL commands
	 */
	public var printSQL = false

	/**
	Read-only array of unsynced tables.  Any tables not in this array will be synced.
	*/
	private(set) public var unsyncedTables: [DBTable] = []

	public static var dateFormatter: DateFormatter = {
		let dateFormatter = DateFormatter()
		dateFormatter.calendar = Calendar(identifier: .gregorian)
		dateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss'.'SSSZZZZZ"

		return dateFormatter
	}()

	// MARK: - Private properties
	private struct DBTables {
		private var tables: [DBTable] = []
		static let tableQueue = DispatchQueue(label: "com.AaronLBratcher.AgileDBTableQueue", attributes: [])

		func allTables() -> [DBTable] {
			return tables
		}

		mutating func addTable(_ table: DBTable) {
			DBTables.tableQueue.sync {
				tables.append(DBTable(name: table.name))
			}
		}

		mutating func dropTable(_ table: DBTable) {
			DBTables.tableQueue.sync {
				tables = tables.filter({ $0 != table })
			}
		}

		mutating func dropAllTables() {
			DBTables.tableQueue.sync {
				tables = []
			}
		}

		func hasTable(_ table: DBTable) -> Bool {
			var exists = false

			DBTables.tableQueue.sync {
				exists = tables.contains(table)
			}

			return exists
		}
	}

	private let dbCore = SQLiteCore()
	private var lock = DispatchSemaphore(value: 0)
	private var dbFileLocation: URL?
	private var dbInstanceKey = ""
	private var tables = DBTables()
	private var indexes = [String: [String]]()
	private let dbQueue = DispatchQueue(label: "com.AaronLBratcher.AgileDBQueue")
	private let publisherQueue = DispatchQueue(label: "com.AaronLBratcher.AgileDBPublisherQueue")
	private var syncingEnabled = false
	private var publishers = [UpdatablePublisher]()
	private lazy var autoDeleteTimer: RepeatingTimer = {
		return RepeatingTimer(timeInterval: 60) {
			self.autoDelete()
		}
	}()

	// MARK: - Init
	/**
	Instantiates an instance of AgileDB

	- parameter location: Optional file location if different than the default.
	*/
	public init(fileLocation: URL? = nil) {
		dbFileLocation = fileLocation
		dbCore.start()
	}

	// MARK: - Open / Close
	/**
	Opens the database file.

	- parameter location: Optional file location if different than the default.

	- returns: Bool Returns if the database could be successfully opened.
	*/
	@discardableResult
	public func open(_ location: URL? = nil) -> Bool {
		let dbFileLocation = location ?? self.dbFileLocation ?? URL(fileURLWithPath: defaultFileLocation())
		// if we already have a db file open at a different location, close it first
		if dbCore.isOpen && dbFileLocation != dbFileLocation {
			close()
		}

		if let location = location {
			self.dbFileLocation = location
		}

		let openResults = openDB()
		if case .success(_) = openResults {
			return true
		} else {
			return false
		}
	}

	/**
	Close the database.
	*/
	public func close() {
		autoDeleteTimer.suspend()
		dbQueue.sync { () -> Void in
			dbCore.close()
		}
	}

	// MARK: - Keys

	/**
	Checks if the given table contains the given key.

	- parameter table: The table to search.
	- parameter key: The key to look for.

	- returns: Bool? Returns if the key exists in the table. Is nil when database could not be opened or other error occured.
	*/
	public func tableHasKey(table: DBTable, key: String) -> Bool? {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return nil
		}

		if !tables.hasTable(table) {
			return false
		}

		let sql = "select 1 from \(table) where key = '\(key)'"
		let results = sqlSelect(sql)
		if let results = results {
			return results.isNotEmpty
		}

		return nil
	}

	/**
	 Checks if the given table contains all the given key.s

	 - parameter table: The table to search.
	 - parameter keys: The keys to look for.

	 - returns: Bool? Returns if the key exists in the table. Is nil when database could not be opened or other error occured.
	 */
	public func tableHasAllKeys(table: DBTable, keys: [String]) -> Bool? {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return nil
		}

		if !tables.hasTable(table) {
			return false
		}

		let keyString = keys.map({ "'\($0)'" }).joined(separator: ",")

		let sql = "select 1 from \(table) where key in (\(keyString))"
		let results = sqlSelect(sql)
		if let results = results {
			return results.count == keys.count
		}

		return nil
	}

	/**
	 Asynchronously checks if the given table contains the given key.

	 - parameter table: The table to search.
	 - parameter key: The key to look for.

	  - returns: Bool
	  - throws: DBError
	  */

	public func tableHasKey(table: DBTable, key: String) async throws -> Bool {
		let results = await bridgingTableHasKey(table: table, key: key)
		switch results {
		case .success(let hasKey):
			return hasKey
		case .failure(let error):
			throw error
		}
	}

	private func bridgingTableHasKey(table: DBTable, key: String) async -> BoolResults {
		await withCheckedContinuation { continuation in
			self.tableHasKey(table: table, key: key) { results in
				continuation.resume(returning: results)
			}
		}
	}

	/**
	Asynchronously checks if the given table contains the given key.

	- parameter table: The table to search.
	- parameter key: The key to look for.
	- parameter queue: Dispatch queue to use when running the completion closure. Default value is main queue.
	- parameter completion: Closure to use for results.

	- returns: DBActivityToken Returns a DBCommandToken that can be used to cancel the command before it executes If the database file cannot be opened nil is returned.
	*/
	@discardableResult
	public func tableHasKey(table: DBTable, key: String, queue: DispatchQueue? = nil, completion: @escaping (BoolResults) -> Void) -> DBCommandToken? {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return nil
		}

		if !tables.hasTable(table) {
			let dispatchQueue = queue ?? DispatchQueue.main
			dispatchQueue.async {
				completion(Result<Bool, DBError>.success(false))
			}
			return DBCommandToken(database: self, identifier: 0)
		}

		let sql = "select 1 from \(table) where key = '\(key)'"
		let blockReference = dbCore.sqlSelect(sql, completion: { (rowResults) -> Void in
			let dispatchQueue = queue ?? DispatchQueue.main
			dispatchQueue.async {
				let results: BoolResults

				switch rowResults {
				case .success(let rows):
					results = .success(rows.isNotEmpty)

				case .failure(let error):
					results = .failure(error)
				}

				completion(results)
			}
		})

		return DBCommandToken(database: self, identifier: blockReference)
	}

	/**
	 Asynchronously checks if the given table contains the all the given keys.

	 - parameter table: The table to search.
	 - parameter keys: The keys to look for.

	  - returns: Bool
	  - throws: DBError
	  */

	public func tableHasAllKeys(table: DBTable, keys: [String]) async throws -> Bool {
		let results = await bridgingTableHasAllKeys(table: table, keys: keys)
		switch results {
		case .success(let hasKeys):
			return hasKeys
		case .failure(let error):
			throw error
		}
	}

	private func bridgingTableHasAllKeys(table: DBTable, keys: [String]) async -> BoolResults {
		await withCheckedContinuation { continuation in
			self.tableHasAllKeys(table: table, keys: keys) { results in
				continuation.resume(returning: results)
			}
		}
	}

	/**
	 Asynchronously checks if the given table contains all the given keys.

	 - parameter table: The table to search.
	 - parameter keys: The keys to look for.
	 - parameter queue: Dispatch queue to use when running the completion closure. Default value is main queue.
	 - parameter completion: Closure to use for results.

	 - returns: DBActivityToken Returns a DBCommandToken that can be used to cancel the command before it executes If the database file cannot be opened nil is returned.
	 */
	@discardableResult
	public func tableHasAllKeys(table: DBTable, keys: [String], queue: DispatchQueue? = nil, completion: @escaping (BoolResults) -> Void) -> DBCommandToken? {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return nil
		}

		if !tables.hasTable(table) {
			let dispatchQueue = queue ?? DispatchQueue.main
			dispatchQueue.async {
				completion(Result<Bool, DBError>.success(false))
			}
			return DBCommandToken(database: self, identifier: 0)
		}

		let keyString = keys.map({ "'\($0)'" }).joined(separator: ",")

		let sql = "select 1 from \(table) where key in (\(keyString))"
		let blockReference = dbCore.sqlSelect(sql, completion: { (rowResults) -> Void in
			let dispatchQueue = queue ?? DispatchQueue.main
			dispatchQueue.async {
				let results: BoolResults

				switch rowResults {
				case .success(let rows):
					results = .success(rows.count == keys.count)

				case .failure(let error):
					results = .failure(error)
				}

				completion(results)
			}
		})

		return DBCommandToken(database: self, identifier: blockReference)
	}

	/**
	Returns an array of keys from the given table sorted in the way specified matching the given conditions. All conditions in the same set are ANDed together. Separate sets are ORed against each other.  (set:0 AND set:0 AND set:0) OR (set:1 AND set:1 AND set:1) OR (set:2)

	Unsorted Example:

	let accountCondition = DBCondition(set:0,objectKey:"account",conditionOperator:.equal, value:"ACCT1")
	if let keys = AgileDB.keysInTable("table1", sortOrder:nil, conditions:accountCondition) {
		// use keys
	} else {
		// handle error
	}

	- parameter table: The DBTable to return keys from.
	- parameter sortOrder: Optional string that gives a comma delimited list of properties to sort by.
	- parameter conditions: Optional array of DBConditions that specify what conditions must be met.
	- parameter validateObjects: Optional bool that condition sets will be validated against the table. Any set that refers to json objects that do not exist in the table will be ignored. Default value is false.

	- returns: [String]? Returns an array of keys from the table. Is nil when database could not be opened or other error occured.
	*/
	public func keysInTable(_ table: DBTable, sortOrder: String? = nil, conditions: [DBCondition]? = nil, validateObjects: Bool = false) -> [String]? {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return nil
		}

		if !tables.hasTable(table) {
			return []
		}

		guard let sql = keysInTableSQL(table: table, sortOrder: sortOrder, conditions: conditions, validateObjecs: validateObjects) else { return [] }

		if let results = sqlSelect(sql) {
			return results.map({ $0.values[0] as! String })
		}

		return nil
	}

	/**
	 Asynchronously keys in given table.

	  - parameter table: The table to return keys from.
	  - parameter sortOrder: Optional string that gives a comma delimited list of properties to sort by.
	  - parameter conditions: Optional array of DBConditions that specify what conditions must be met.
	  - parameter validateObjects: Optional bool that condition sets will be validated against the table. Any set that refers to json objects that do not exist in the table will be ignored. Default value is false.

	  - returns: [String]
	  - throws: DBError
	  */

	public func keysInTable(_ table: DBTable, sortOrder: String? = nil, conditions: [DBCondition]? = nil, validateObjects: Bool = false) async throws -> [String] {
		let results = await bridgingKeysInTable(table, sortOrder: sortOrder, conditions: conditions, validateObjects: validateObjects)
		switch results {
		case .success(let keys):
			return keys
		case .failure(let error):
			throw error
		}
	}

	private func bridgingKeysInTable(_ table: DBTable, sortOrder: String? = nil, conditions: [DBCondition]? = nil, validateObjects: Bool = false) async -> KeyResults {
		await withCheckedContinuation { continuation in
			self.keysInTable(table, sortOrder: sortOrder, conditions: conditions, validateObjects: validateObjects) { results in
				continuation.resume(returning: results)
			}
		}
	}

	/**
	Asynchronously returns the keys in the given table.

	Runs a query asynchronously and calls the completion closure with the results. Successful results are keys from the given table sorted in the way specified matching the given conditions. All conditions in the same set are ANDed together. Separate sets are ORed against each other.  (set:0 AND set:0 AND set:0) OR (set:1 AND set:1 AND set:1) OR (set:2)

	Unsorted Example:

	let accountCondition = DBCondition(set:0,objectKey:"account",conditionOperator:.equal, value:"ACCT1")
	if let keys = AgileDB.keysInTable("table1", sortOrder:nil, conditions:accountCondition) {
		// use keys
	} else {
		// handle error
	}

	- parameter table: The table to return keys from.
	- parameter sortOrder: Optional string that gives a comma delimited list of properties to sort by.
	- parameter conditions: Optional array of DBConditions that specify what conditions must be met.
	- parameter validateObjects: Optional bool that condition sets will be validated against the table. Any set that refers to json objects that do not exist in the table will be ignored. Default value is false.
	- parameter queue: Optional dispatch queue to use when running the completion closure. Default value is main queue.
	- parameter completion: Closure with DBRowResults.

	- returns: DBCommandToken that can be used to cancel the command before it executes If the database file cannot be opened nil is returned.
	*/

	@discardableResult
	public func keysInTable(_ table: DBTable, sortOrder: String? = nil, conditions: [DBCondition]? = nil, validateObjects: Bool = false, queue: DispatchQueue? = nil, completion: @escaping (KeyResults) -> Void) -> DBCommandToken? {
		let openResults = openDB()
		if case .failure(_) = openResults {
			completion(.failure(.cannotOpenFile))
			return nil
		}

		if !tables.hasTable(table) {
			completion(.failure(.tableNotFound))
			return DBCommandToken(database: self, identifier: 0)
		}

		guard let sql = keysInTableSQL(table: table, sortOrder: sortOrder, conditions: conditions, validateObjecs: validateObjects) else {
			completion(.failure(.cannotParseData))
			return nil
		}

		let blockReference = dbCore.sqlSelect(sql, completion: { (rowResults) -> Void in
			let dispatchQueue = queue ?? DispatchQueue.main
			dispatchQueue.async {
				let results: KeyResults

				switch rowResults {
				case .success(let rows):
					results = .success(rows.map({ $0.values[0] as! String }))

				case .failure(let error):
					results = .failure(error)
				}

				completion(results)
			}
		})

		return DBCommandToken(database: self, identifier: blockReference)
	}

	/**
	 Returns a  Publisher for generic DBResults. Uses the table of the DBObject for results.

	 - parameter sortOrder: Optional string that gives a comma delimited list of properties to sort by.
	 - parameter conditions: Optional array of DBConditions that specify what conditions must be met.
	 - parameter validateObjects: Optional bool that condition sets will be validated against the table. Any set that refers to json objects that do not exist in the table will be ignored. Default value is false.

	 - returns: DBResultssPublisher
	 */

	@discardableResult
	public func publisher<T>(sortOrder: String? = nil, conditions: [DBCondition]? = nil, validateObjects: Bool = false) -> DBResultsPublisher<T> {
		let publisher = DBResultsPublisher<T>(db: self, table: T.table, sortOrder: sortOrder, conditions: conditions, validateObjects: validateObjects)
		dbQueue.sync {
			publishers.append(publisher)
			DispatchQueue.global().async {
				publisher.updateSubject()
			}
		}

		return publisher
	}

	// MARK: - Indexing
	/**
	Sets the indexes desired for a given table.

	Example:

	AgileDB.setIndexesForTable(kTransactionsTable, to: ["accountKey","date"]) // index accountKey and date each individually

	- parameter table: The table to return keys from.
	- parameter indexes: An array of table properties to be indexed. An array entry can be compound.
	*/
	@discardableResult
	public func setIndexesForTable(_ table: DBTable, to indexes: [String]) -> BoolResults {
		let openResults = openDB()
		if case .success(_) = openResults {
			self.indexes[table.name] = indexes
			// TODO: Return results from call
			createIndexesForTable(table)
		}

		return openResults
	}

	// MARK: - Set Values
	/**
	Sets the value of an entry in the given table for a given key optionally deleted automatically after a given date. Supported values are dictionaries that consist of String, Int, Double and arrays of these. If more complex objects need to be stored, a string value of those objects need to be stored.

	Example:

	if !AgileDB.setValueInTable("table5", for: "testKey1", to: "{\"numValue\":1,\"account\":\"ACCT1\",\"dateValue\":\"2014-8-19T18:23:42.434-05:00\",\"arrayValue\":[1,2,3,4,5]}", autoDeleteAfter: nil) {
		// handle error
	}

	- parameter table: The table to return keys from.
	- parameter key: The key for the entry.
	- parameter value: A JSON string representing the value to be stored. Top level object provided must be a dictionary. If a key node is in the value, it will be ignored.
	- parameter autoDeleteAfter: Optional date of when the value should be automatically deleted from the table.

	- returns: Bool If the value was set successfully.
	*/
	@discardableResult
	public func setValueInTable(_ table: DBTable, for key: String, to value: String, autoDeleteAfter: Date? = nil) -> Bool {
		assert(key != "", "key must be provided")
		assert(value != "", "value must be provided")

		guard let dataValue = value.data(using: .utf8) else { return false }

		let objectValues = (try? JSONSerialization.jsonObject(with: dataValue, options: .mutableContainers)) as? [String: AnyObject]
		assert(objectValues != nil, "Value must be valid JSON string that is a dictionary for the top-level object")

		return setValueInTable(table, for: key, to: objectValues!, autoDeleteAfter: autoDeleteAfter)
	}

	/**
	Sets the value of an entry in the given table for a given key optionally deleted automatically after a given date. Supported values are dictionaries with string keys and values that consist of String, Int, Double, Bool and arrays of String, Int, and Double. If more complex objects need to be stored, a string value of those objects need to be stored.

	Example:

	if !AgileDB.setValueInTable("table5", for: "testKey1", to: dictValue, autoDeleteAfter: nil) {
		// handle error
	}

	- parameter table: The table to return keys from.
	- parameter key: The key for the entry.
	- parameter value: A dictionary object representing the value to be stored. If a key named "key" exists, it will be ignored.
	- parameter autoDeleteAfter: Optional date of when the value should be automatically deleted from the table.

	- returns: Bool If the value was set successfully.
	*/
	@discardableResult
	public func setValueInTable(_ table: DBTable, for key: String, to objectValues: [String: AnyObject], autoDeleteAfter: Date? = nil) -> Bool {
		let now = AgileDB.stringValueForDate(Date())
		let deleteDateTime = (autoDeleteAfter == nil ? "NULL" : "'" + AgileDB.stringValueForDate(autoDeleteAfter!) + "'")

		let successful = setValue(table: table, key: key, objectValues: objectValues, addedDateTime: now, updatedDateTime: now, deleteDateTime: deleteDateTime, sourceDB: dbInstanceKey, originalDB: dbInstanceKey)

		if successful {
			updatePublisherResults(for: key, in: table)
		}

		return successful
	}

	// MARK: - Return Values
	/**
	Returns the JSON value of what was stored for a given table and key.

	Example:
	if let jsonValue = AgileDB.valueFromTable("table1", for: "58D200A048F9") {
		// process JSON text
	} else {
		// handle error
	}

	- parameter table: The table to return keys from.
	- parameter key: The key for the entry.

	- returns: JSON value of what was stored. Is nil when database could not be opened or other error occured.
	*/
	public func valueFromTable(_ table: DBTable, for key: String) -> String? {
		if let dictionaryValue = dictValueFromTable(table, for: key) {
			let dataValue = try? JSONSerialization.data(withJSONObject: dictionaryValue, options: JSONSerialization.WritingOptions(rawValue: 0))
			let jsonValue = String(data: dataValue!, encoding: .utf8)
			return jsonValue! as String
		}

		return nil
	}

	/**
	  Asynchronously returns the value for a given table and key.

	  - parameter table: The table to return keys from.
	  - parameter key: The key for the entry.

	  - returns: String
	  - throws: DBError
	  */

	public func valueFromTable(_ table: DBTable, for key: String) async throws -> String {
		let results = await bridgingValueFromTable(table, for: key)

		switch results {
		case .success(let value):
			return value
		case .failure(let error):
			throw error
		}
	}

	private func bridgingValueFromTable(_ table: DBTable, for key: String) async -> JsonResults {
		await withCheckedContinuation { continuation in
			self.valueFromTable(table, for: key) { results in
				continuation.resume(returning: results)
			}
		}
	}

	/**
	Asynchronously returns the value for a given table and key.

	Runs a query asynchronously and calls the completion closure with the results. Successful result is a String.

	- parameter table: The table to return keys from.
	- parameter key: The key for the entry.
	- parameter queue: Optional dispatch queue to use when running the completion closure. Default value is main queue.
	- parameter completion: Closure to use for JSON results.

	- returns: Returns a DBCommandToken that can be used to cancel the command before it executes. If the database file cannot be opened or table does not exist nil is returned.

	*/
	@discardableResult
	public func valueFromTable(_ table: DBTable, for key: String, queue: DispatchQueue? = nil, completion: @escaping (JsonResults) -> Void) -> DBCommandToken? {
		let openResults = openDB()
		if case .failure(_) = openResults, !tables.hasTable(table) {
			return nil
		}

		let (sql, columns) = dictValueForKeySQL(table: table, key: key, includeDates: false)

		let blockReference = dbCore.sqlSelect(sql, completion: { [weak self] (rowResults) -> Void in
			guard let self = self else { return }

			let dispatchQueue = queue ?? DispatchQueue.main
			dispatchQueue.async {
				let results: Result<String, DBError>

				switch rowResults {
				case .success(let rows):
					guard let dictionaryValue = self.dictValueResults(table: table, key: key, results: rows, columns: columns)
					, let dataValue = try? JSONSerialization.data(withJSONObject: dictionaryValue, options: JSONSerialization.WritingOptions(rawValue: 0))
					, let jsonValue = String(data: dataValue, encoding: .utf8)
						else {
						results = .failure(.other(0))
						completion(results)
						return
					}

					results = .success(jsonValue)

				case .failure(let error):
					results = .failure(error)
				}

				completion(results)
			}
		})

		return DBCommandToken(database: self, identifier: blockReference)
	}

	/**
	Returns the dictionary value of what was stored for a given table and key.

	Example:
	if let dictValue = AgileDB.dictValueForKey(table: "table1", key: "58D200A048F9") {
		// process dictionary
	} else {
		// handle error
	}

	- parameter table: The table to return keys from.
	- parameter key: The key for the entry.

	- returns: [String:AnyObject]? Dictionary value of what was stored. Is nil when database could not be opened or other error occured.
	*/
	public func dictValueFromTable(_ table: DBTable, for key: String) -> [String: AnyObject]? {
		return dictValueFromTable(table, for: key, includeDates: false)
	}

	/**
	  Asynchronously returns the dictionary value of what was stored for a given table and key.

	  - parameter table: The table to return keys from.
	  - parameter key: The key for the entry.

	  - returns: [String: AnyObject]
	  - throws: DBError
	  */

	public func dictValueFromTable(_ table: DBTable, for key: String) async throws -> [String: AnyObject] {
		let results = await bridgingDictValueFromTable(table, for: key)

		switch results {
		case .success(let value):
			return value
		case .failure(let error):
			throw error
		}
	}

	private func bridgingDictValueFromTable(_ table: DBTable, for key: String) async -> DictResults {
		await withCheckedContinuation { continuation in
			self.dictValueFromTable(table, for: key) { results in
				continuation.resume(returning: results)
			}
		}
	}

	/**
	Returns the dictionary value of what was stored for a given table and key.

	Example:
	if let dictValue = AgileDB.dictValueForKey(table: "table1", key: "58D200A048F9") {
		// process dictionary
	} else {
		// handle error
	}

	- parameter table: The table to return keys from.
	- parameter key: The key for the entry.
	- parameter queue: Optional dispatch queue to use when running the completion closure. Default value is main queue.
	- parameter completion: Closure to use for dictionary results.

	- returns: Returns a DBCommandToken that can be used to cancel the command before it executes. If the database file cannot be opened or table does not exist nil is returned.
	*/
	@discardableResult
	public func dictValueFromTable(_ table: DBTable, for key: String, queue: DispatchQueue? = nil, completion: @escaping (DictResults) -> Void) -> DBCommandToken? {
		let openResults = openDB()
		if case .failure(_) = openResults, !tables.hasTable(table) {
			return nil
		}

		let (sql, columns) = dictValueForKeySQL(table: table, key: key, includeDates: false)

		let blockReference = dbCore.sqlSelect(sql, completion: { [weak self] (rowResults) -> Void in
			guard let self = self else { return }

			let dispatchQueue = queue ?? DispatchQueue.main
			dispatchQueue.async {
				let results: Result<[String: AnyObject], DBError>

				switch rowResults {
				case .success(let rows):
					guard let dictionaryValue = self.dictValueResults(table: table, key: key, results: rows, columns: columns)
						else {
						results = .failure(.other(0))
						completion(results)
						return
					}

					results = .success(dictionaryValue)

				case .failure(let error):
					results = .failure(error)
				}

				completion(results)
			}
		})

		return DBCommandToken(database: self, identifier: blockReference)
	}

	// MARK: - Delete
	/**
	Delete the value from the given table for the given key.

	- parameter table: The table to return keys from.
	- parameter key: The key for the entry.

	- returns: Bool Value was successfuly removed.
	*/
	@discardableResult
	public func deleteFromTable(_ table: DBTable, for key: String) -> Bool {
		assert(key != "", "key must be provided")
		var deleted = false

		publisherQueue.sync {
			let publishers = publishersContaining(key: key, in: table)

			deleted = deleteForKey(table: table, key: key, autoDelete: false, sourceDB: dbInstanceKey, originalDB: dbInstanceKey)
			if deleted {
				for publisher in publishers {
					publisher.updateSubject()
				}
			}
		}

		return deleted
	}

	/**
	Asynchronously delete the value from the given table for the given key.

	- parameter table: The table to return keys from.
	- parameter key: The key for the entry.

	- returns: Bool Value was successfuly removed.
	*/
	@discardableResult
	public func deleteFromTable(_ table: DBTable, for key: String) async -> Bool {
		assert(key != "", "key must be provided")

		return publisherQueue.sync { () -> Bool in
			let publishers = publishersContaining(key: key, in: table)

			let successful = deleteForKey(table: table, key: key, autoDelete: false, sourceDB: dbInstanceKey, originalDB: dbInstanceKey)
			if successful {
				for publisher in publishers {
					publisher.updateSubject()
				}
			}

			return successful
		}
	}

	/**
	Removes the given table and associated values.

	- parameter table: The table to return keys from.

	- returns: Bool Table was successfuly removed.
	*/
	@discardableResult
	public func dropTable(_ table: DBTable) -> Bool {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return false
		}

		if !sqlExecute("drop table \(table)")
			|| !sqlExecute("drop table \(table)_arrayValues")
			|| !sqlExecute("delete from __tableArrayColumns where tableName = '\(table)'") {
			return false
		}

		tables.dropTable(table)

		if syncingEnabled && unsyncedTables.doesNotContain(table) {
			let now = AgileDB.stringValueForDate(Date())
			if !sqlExecute("insert into __synclog(timestamp, sourceDB, originalDB, tableName, activity, key) values('\(now)','\(dbInstanceKey)','\(dbInstanceKey)','\(table)','X',NULL)") {
				return false
			}

			let lastID = lastInsertID()

			if !sqlExecute("delete from __synclog where tableName = '\(table)' and rowid < \(lastID)") {
				return false
			}
		}

		clearPublisherResults(in: table)

		return true
	}

	/**
	Removes all tables and associated values.

	- returns: Bool Tables were successfuly removed.
	*/
	@discardableResult
	public func dropAllTables() -> Bool {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return false
		}

		var successful = true
		let dbTables = tables.allTables()
		for table in dbTables {
			successful = dropTable(table)
			if !successful {
				return false
			}
		}

		tables.dropAllTables()

		return true
	}

	// MARK: - Sync
	/**
	Current syncing status. Nil if the database could not be opened.
	*/
	public var isSyncingEnabled: Bool? {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return nil
		}

		return syncingEnabled
	}

	/**
	Enables syncing. Once enabled, a log is created for all current values in the tables.

	- returns: Bool If syncing was successfully enabled.
	*/
	public func enableSyncing() -> Bool {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return false
		}

		if syncingEnabled {
			return true
		}

		if !sqlExecute("create table __synclog(timestamp text, sourceDB text, originalDB text, tableName text, activity text, key text)") {
			return false
		}
		sqlExecute("create index __synclog_index on __synclog(tableName,key)")
		sqlExecute("create index __synclog_source on __synclog(sourceDB,originalDB)")
		sqlExecute("create table __unsyncedTables(tableName text)")

		let now = AgileDB.stringValueForDate(Date())
		let dbTables = tables.allTables()
		for table in dbTables {
			if !sqlExecute("insert into __synclog(timestamp, sourceDB, originalDB, tableName, activity, key) select '\(now)','\(dbInstanceKey)','\(dbInstanceKey)','\(table.name)','U',key from \(table.name)") {
				return false
			}
		}

		syncingEnabled = true
		return true
	}

	/**
	Disables syncing.

	- returns: Bool If syncing was successfully disabled.
	*/
	public func disableSyncing() -> Bool {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return false
		}

		if !syncingEnabled {
			return true
		}

		if !sqlExecute("drop table __synclog") || !sqlExecute("drop table __unsyncedTables") {
			return false
		}

		syncingEnabled = false

		return true
	}

	/**
	Sets the tables that are not to be synced.

	- parameter tables: Array of tables that are not to be synced.

	- returns: Bool If list was set successfully.
	*/
	@discardableResult
	public func setUnsyncedTables(_ tables: [DBTable]) -> Bool {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return false
		}

		if !syncingEnabled {
			print("syncing must be enabled before setting unsynced tables")
			return false
		}

		unsyncedTables = [DBTable]()
		for table in tables {
			sqlExecute("delete from __synclog where tableName = '\(table)'")
			unsyncedTables.append(table)
		}

		return true
	}

	/**
	Creates a sync file that can be used on another AgileDB instance to sync data. This is a synchronous call.

	- parameter filePath: The full path, including the file itself, to be used for the log file.
	- parameter lastSequence: The last sequence used for the given target  Initial sequence is 0.
	- parameter targetDBInstanceKey: The dbInstanceKey of the target database. Use the dbInstanceKey method to get the DB's instanceKey.

	- returns: (Bool,Int) If the file was successfully created and the lastSequence that should be used in subsequent calls to this instance for the given targetDBInstanceKey.
	*/
	public func createSyncFileAtURL(_ localURL: URL!, lastSequence: Int, targetDBInstanceKey: String) -> (Bool, Int) {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return (false, lastSequence)
		}

		if !syncingEnabled {
			print("syncing must be enabled before creating sync file")
			return (false, lastSequence)
		}

		let filePath = localURL.path

		if FileManager.default.fileExists(atPath: filePath) {
			do {
				try FileManager.default.removeItem(atPath: filePath)
			} catch _ as NSError {
				return (false, lastSequence)
			}
		}

		FileManager.default.createFile(atPath: filePath, contents: nil, attributes: nil)

		if let fileHandle = FileHandle(forWritingAtPath: filePath) {
			if let results = sqlSelect("select rowid,timestamp,originalDB,tableName,activity,key from __synclog where rowid > \(lastSequence) and sourceDB <> '\(targetDBInstanceKey)' and originalDB <> '\(targetDBInstanceKey)' order by rowid") {
				var lastRowID = lastSequence
				fileHandle.write("{\"sourceDB\":\"\(dbInstanceKey)\",\"logEntries\":[\n".dataValue())
				var firstEntry = true
				for row in results {
					lastRowID = row.values[0] as! Int
					let timeStamp = row.values[1] as! String
					let originalDB = row.values[2] as! String
					let tableName = row.values[3] as! String
					let activity = row.values[4] as! String
					let key = row.values[5] as! String?

					var entryDict = [String: AnyObject]()
					entryDict["timeStamp"] = timeStamp as AnyObject
					if originalDB != dbInstanceKey {
						entryDict["originalDB"] = originalDB as AnyObject
					}
					entryDict["tableName"] = tableName as AnyObject
					entryDict["activity"] = activity as AnyObject
					if let key = key {
						entryDict["key"] = key as AnyObject
						if activity == "U" {
							guard let dictValue = dictValueFromTable(DBTable(name: tableName), for: key, includeDates: true) else { continue }
							entryDict["value"] = dictValue as AnyObject
						}
					}

					let dataValue = try? JSONSerialization.data(withJSONObject: entryDict, options: JSONSerialization.WritingOptions(rawValue: 0))
					if firstEntry {
						firstEntry = false
					} else {
						fileHandle.write("\n,".dataValue())
					}

					fileHandle.write(dataValue!)
				}

				fileHandle.write("\n],\"lastSequence\":\(lastRowID)}".dataValue())
				fileHandle.closeFile()
				return (true, lastRowID)
			} else {
				do {
					try FileManager.default.removeItem(atPath: filePath)
				} catch _ {
					return (false, lastSequence)
				}
			}
		}

		return (false, lastSequence)
	}


	/**
	Processes a sync file created by another instance of AgileDB. This is a synchronous call.

	- parameter filePath: The path to the sync file.
	- parameter syncProgress: Optional function that will be called periodically giving the percent complete.

	- returns: (Bool,String,Int)  If the sync file was successfully processed,the instanceKey of the submiting DB, and the lastSequence that should be used in subsequent calls to the createSyncFile method of the instance that was used to create this file. If the database couldn't be opened or syncing hasn't been enabled, then the instanceKey will be empty and the lastSequence will be equal to zero.
	*/
	public typealias syncProgressUpdate = (_ percentComplete: Double) -> Void
	public func processSyncFileAtURL(_ localURL: URL!, syncProgress: syncProgressUpdate?) -> (Bool, String, Int) {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return (false, "", 0)
		}

		if !syncingEnabled {
			print("syncing must be enabled before processing sync file")
			return (false, "", 0)
		}

		autoDelete()

		let filePath = localURL.path

		if let _ = FileHandle(forReadingAtPath: filePath) {
			// TODO: Stream in the file and parse as needed instead of parsing the entire thing at once to save on memory use
			let now = AgileDB.stringValueForDate(Date())
			if let fileText = try? String(contentsOfFile: filePath, encoding: String.Encoding.utf8) {
				let dataValue = fileText.dataValue()

				if let objectValues = (try? JSONSerialization.jsonObject(with: dataValue, options: .mutableContainers)) as? [String: AnyObject] {
					let sourceDB = objectValues["sourceDB"] as! String
					let logEntries = objectValues["logEntries"] as! [[String: AnyObject]]
					let lastSequence = objectValues["lastSequence"] as! Int
					var index = 0
					for entry in logEntries {
						index += 1
						if index % 20 == 0 {
							if let syncProgress = syncProgress {
								let percent = (Double(index) / Double(logEntries.count))
								syncProgress(percent)
							}
						}

						let activity = entry["activity"] as! String
						let timeStamp = entry["timeStamp"] as! String
						let tableName = entry["tableName"] as! String
						let originalDB = (entry["originalDB"] == nil ? sourceDB : entry["originalDB"] as! String)

						// for entry activity U,D only process log entry if no local entry for same table/key that is greater than one received
						if activity == "D" || activity == "U" {
							if let key = entry["key"] as? String, let results = sqlSelect("select 1 from __synclog where tableName = '\(tableName)' and key = '\(key)' and timestamp > '\(timeStamp)'") {
								if results.isEmpty {
									if activity == "U" {
										// strip out the dates to send separately
										var objectValues = entry["value"] as! [String: AnyObject]
										let addedDateTime = objectValues["addedDateTime"] as! String
										let updatedDateTime = objectValues["updatedDateTime"] as! String
										let deleteDateTime = (objectValues["deleteDateTime"] == nil ? "NULL" : objectValues["deleteDateTime"] as! String)
										objectValues.removeValue(forKey: "addedDateTime")
										objectValues.removeValue(forKey: "updatedDateTime")
										objectValues.removeValue(forKey: "deleteDateTime")

										_ = setValue(table: DBTable(name: tableName), key: key, objectValues: objectValues, addedDateTime: addedDateTime, updatedDateTime: updatedDateTime, deleteDateTime: deleteDateTime, sourceDB: sourceDB, originalDB: originalDB)
									} else {
										_ = deleteForKey(table: DBTable(name: tableName), key: key, autoDelete: false, sourceDB: sourceDB, originalDB: originalDB)
									}
								}
							}
						} else {
							// for table activity X, delete any entries that occured BEFORE this event
							sqlExecute("delete from \(tableName) where key in (select key from __synclog where tableName = '\(tableName)' and timeStamp < '\(timeStamp)')")
							sqlExecute("delete from \(tableName)_arrayValues where key in (select key from __synclog where tableName = '\(tableName)' and timeStamp < '\(timeStamp)')")
							sqlExecute("delete from __synclog where tableName = '\(tableName)' and timeStamp < '\(timeStamp)'")
							sqlExecute("insert into __synclog(timestamp, sourceDB, originalDB, tableName, activity, key) values('\(now)','\(sourceDB)','\(originalDB)','\(tableName)','X',NULL)")
						}
					}

					publisherQueue.sync {
						for publisher in publishers {
							publisher.updateSubject()
						}
					}

					return (true, sourceDB, lastSequence)
				} else {
					return (false, "", 0)
				}
			} else {
				return (false, "", 0)
			}
		}

		return (false, "", 0)
	}

	// MARK: - Misc
	/**
	 Check for the existance of a given table
	 - parameter table: The table to check the existence of

	 - returns: the existence of a specified table
	 */
	public func hasTable(_ table: DBTable) -> Bool {
		let openResults = openDB()
		if case .success(_) = openResults {
			return tables.hasTable(table)
		}

		return false
	}


	/**
	The instanceKey for this database instance. Each AgileDB database is created with a unique instanceKey. Is nil when database could not be opened.
	*/
	public var instanceKey: String? {
		let openResults = openDB()
		if case .success(_) = openResults {
			return dbInstanceKey
		}

		return nil
	}

	/**
	Replace single quotes with two single quotes for use in SQL commands.

	- returns: An escaped string.
	*/
	public func esc(_ source: String) -> String {
		return source.replacingOccurrences(of: "'", with: "''")
	}

	/**
	String value for a given date.

	- parameter date: Date to get string value of

	- returns: String Date presented as a string
	*/
	public class func stringValueForDate(_ date: Date) -> String {
		return AgileDB.dateFormatter.string(from: date)
	}

	/**
	Date value for given string

	- parameter stringValue: String representation of date given in ISO format "yyyy-MM-dd'T'HH:mm:ss'.'SSSZZZZZ"

	- returns: NSDate? Date value. Is nil if the string could not be converted to date.
	*/
	public class func dateValueForString(_ stringValue: String) -> Date? {
		return AgileDB.dateFormatter.date(from: stringValue)
	}

	// MARK: - Internal Initialization Methods
	private func openDB() -> BoolResults {
		if dbCore.isOpen {
			return BoolResults.success(true)
		}

		let filePath: String

		if let _dbFileLocation = self.dbFileLocation {
			filePath = _dbFileLocation.path
		} else {
			filePath = defaultFileLocation()
			dbFileLocation = URL(fileURLWithPath: filePath)
		}

		dbFilePath = filePath

		var fileExists = false

		var openResults: BoolResults = .success(true)
		var previouslyOpened = false

		dbQueue.sync { [weak self]() -> Void in
			guard let self = self else { return }

			self.dbCore.openDBFile(filePath, autoCloseTimeout: self.autoCloseTimeout) { (results, alreadyOpen, alreadyExists) -> Void in
				openResults = results
				previouslyOpened = alreadyOpen
				fileExists = alreadyExists

				self.lock.signal()
			}
			self.lock.wait()
		}

		if case .success(_) = openResults, !previouslyOpened {
			// if this fails, then the DB file has issues and should not be used
			if !sqlExecute("ANALYZE") {
				return BoolResults.failure(.damagedFile)
			}

			if !fileExists {
				makeDB()
			}

			checkSchema()
			autoDeleteTimer.resume()
		}

		return openResults
	}

	private func defaultFileLocation() -> String {
		let searchPaths = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true)
		let documentFolderPath = searchPaths[0]
		let dbFilePath = documentFolderPath + "/ABNoSQLDB.db"
		return dbFilePath
	}

	private func makeDB() {
		assert(sqlExecute("create table __settings(key text, value text)"), "Unable to make DB")
		assert(sqlExecute("insert into __settings(key,value) values('schema',1)"), "Unable to make DB")
		assert(sqlExecute("create table __tableArrayColumns(tableName text, arrayColumns text)"), "Unable to make DB")
	}

	private func checkSchema() {
		tables.dropAllTables()
		let tableList = sqlSelect("SELECT name FROM sqlite_master WHERE type = 'table'")
		if let tableList = tableList {
			for tableRow in tableList {
				let table = tableRow.values[0] as! String
				if !AgileDB.reservedTable(table) && !table.hasSuffix("_arrayValues") {
					tables.addTable(DBTable(name: table))
				}

				if table == "__synclog" {
					syncingEnabled = true
				}
			}
		}

		if syncingEnabled {
			unsyncedTables = [DBTable]()
			let unsyncedTables = sqlSelect("select tableName from __unsyncedTables")
			if let unsyncedTables = unsyncedTables {
				self.unsyncedTables = unsyncedTables.map({ $0.values[0] as! DBTable })
			}
		}

		if let keyResults = sqlSelect("select value from __settings where key = 'dbInstanceKey'") {
			if keyResults.isEmpty {
				dbInstanceKey = UUID().uuidString
				let parts = dbInstanceKey.components(separatedBy: "-")
				dbInstanceKey = parts[parts.count - 1]
				sqlExecute("insert into __settings(key,value) values('dbInstanceKey','\(dbInstanceKey)')")
			} else {
				dbInstanceKey = keyResults[0].values[0] as! String
			}
		}

		if let schemaResults = sqlSelect("select value from __settings where key = 'schema'") {
			var schemaVersion = Int((schemaResults[0].values[0] as! String))!
			if schemaVersion == 1 {
				sqlExecute("update __settings set value = 2 where key = 'schema'")
				schemaVersion = 2
			}

			// use this space to update the schema value in __settings and to update any other tables that need updating with the new schema
		}
	}
}

// MARK: - Internal Publisher Updates
extension AgileDB {
	func removePublisher(_ publisher: UpdatablePublisher) {
		publisherQueue.sync {
			publishers = publishers.filter({ $0.id != publisher.id })
		}
	}

	fileprivate func updatePublisherResults(for key: String, in table: DBTable) {
		publisherQueue.sync {
			for publisher in publishersContaining(key: key, in: table) {
				publisher.updateSubject()
			}
		}
	}

	fileprivate func clearPublisherResults(in table: DBTable) {
		publisherQueue.sync {
			for publisher in publishers {
				publisher.clearResults(in: table)
			}
		}
	}

	fileprivate func publishersContaining(key: String, in table: DBTable) -> [UpdatablePublisher] {
		var matchingPublishers = [UpdatablePublisher]()

		for publisher in publishers where publisher.table == table {
			guard let sql = keysInTableSQL(table: table, sortOrder: nil, conditions: publisher.conditions, validateObjecs: publisher.validateObjects, testKey: key)
			, let results = sqlSelect(sql)
				else { continue }

			let keys = results.map({ $0.values[0] as! String })
			if keys.count > 0 {
				matchingPublishers.append(publisher)
			}
		}

		return matchingPublishers
	}
}

// MARK: - Internal data handling methods
extension AgileDB {
	fileprivate func keysInTableSQL(table: DBTable, sortOrder: String?, conditions: [DBCondition]?, validateObjecs: Bool, testKey: String? = nil) -> String? {
		var arrayColumns = [String]()
		if let results = sqlSelect("select arrayColumns from __tableArrayColumns where tableName = '\(table)'") {
			if results.isNotEmpty {
				arrayColumns = (results[0].values[0] as! String).split { $0 == "," }.map { String($0) }
			}
		} else {
			return nil
		}

		let tableColumns = columnsInTable(table).map({ $0.name }) + ["key"]
		var selectClause = "select distinct a.key from \(table) a"

		var whereClause = ""

		// if we have the include operator on an array object, do a left outer join
		if var conditionSet = conditions {
			if validateObjecs {
				let invalidSets = conditionSet.filter({ !tableColumns.contains($0.objectKey) }).compactMap({ $0.set })
				let validConditions = conditionSet.filter({ !invalidSets.contains($0.set) })
				conditionSet = validConditions
			}

			for condition in conditionSet {
				if condition.conditionOperator == .contains && arrayColumns.filter({ $0 == condition.objectKey }).count == 1 {
					selectClause += " left outer join \(table)_arrayValues b on a.key = b.key"
					break
				}
			}

			if conditionSet.count > 0 {
				let pages = conditionSetPages(from: conditionSet)
				var pageClauses: [String] = []

				for page in pages {
					pageClauses.append(pageClause(table: table, conditions: conditionSet.filter({ $0.set == page }), arrayColumns: arrayColumns))
				}

				for (index, pageClause) in pageClauses.enumerated() {
					if index > 0 {
						whereClause += "\nOR \(pageClause)"
					} else {
						whereClause += pageClause
					}
				}
			}
		}

		if let testKey = testKey {
			whereClause = " where a.key = '\(esc(testKey))'"
		} else if (conditions ?? []).isNotEmpty {
			whereClause = " where 1=1 AND (\n\(whereClause)\n)"
		}

		if let sortOrder = sortOrder {
			whereClause += " order by \(sortOrder)"
		}

		let sql = selectClause + whereClause
		if printSQL {
			print("^^^ SQL: \(NSString(string: sql))")
		}
		return sql
	}

	private func conditionSetPages(from conditions: [DBCondition]) -> Set<Int> {
		var pages: Set<Int> = []
		for condition in conditions {
			pages.insert(condition.set)
		}

		return pages
	}

	private func pageClause(table: DBTable, conditions: [DBCondition], arrayColumns: [String]) -> String {
		var whereClause = "a.key in (select key from \(table.name) where "
		for (index, condition) in conditions.enumerated() {
			let conditionClause = conditionClause(from: condition, arrayColumns: arrayColumns)
			if index > 0 {
				whereClause += " AND \(conditionClause)"
			} else {
				whereClause += conditionClause
			}
		}
		whereClause += ")"

		return whereClause
	}

	private func conditionClause(from condition: DBCondition, arrayColumns: [String]) -> String {
		let valueType = SQLiteCore.typeOfValue(condition.value)
		var whereClause = ""
		switch condition.conditionOperator {
		case .contains:
			if arrayColumns.contains(condition.objectKey) {
				switch valueType {
				case .text:
					whereClause += "b.objectKey = '\(condition.objectKey)' and b.stringValue = '\(esc(condition.value as! String))'"
				case .int:
					whereClause += "b.objectKey = '\(condition.objectKey)' and b.intValue = \(condition.value)"
				case .double:
					whereClause += "b.objectKey = '\(condition.objectKey)' and b.doubleValue = \(condition.value)"
				default:
					break
				}
			} else {
				whereClause += " \(condition.objectKey) like '%%\(esc(condition.value as! String))%%'"
			}

		case .inList:
			var listItems = ""

			if let valueArray = condition.value as? [String], valueArray.isNotEmpty {
				for (index, value) in valueArray.enumerated() {
					listItems += "'\(esc(value))'"
					if index < valueArray.count - 1 {
						listItems += ", "
					}
				}
			} else if let valueArray = condition.value as? [Int], valueArray.isNotEmpty {
				for (index, value) in valueArray.enumerated() {
					listItems += "\(value)"
					if index < valueArray.count - 1 {
						listItems += ", "
					}
				}
			} else if let valueArray = condition.value as? [Double], valueArray.isNotEmpty {
				for (index, value) in valueArray.enumerated()  {
					listItems += "\(value)"
					if index < valueArray.count - 1 {
						listItems += ", "
					}
				}
			}

			if listItems.isNotEmpty {
				whereClause += " \(condition.objectKey) in (\(listItems))"
			}

		default:
			if let conditionValue = condition.value as? String {
				whereClause += " \(condition.objectKey) \(condition.conditionOperator.rawValue) '\(esc(conditionValue))'"
			} else if let conditionValue = condition.value as? Date {
				whereClause += " \(condition.objectKey) \(condition.conditionOperator.rawValue) '\(AgileDB.stringValueForDate(conditionValue))'"
			} else if let conditionValue = condition.value as? Bool {
				let boolValue = conditionValue ? 1 : 0
				whereClause += " \(condition.objectKey) \(condition.conditionOperator.rawValue) \(boolValue)"
			} else {
				whereClause += " \(condition.objectKey) \(condition.conditionOperator.rawValue) \(condition.value)"
			}
		}

		return whereClause
	}

	private func setValue(table: DBTable, key: String, objectValues: [String: AnyObject], addedDateTime: String, updatedDateTime: String, deleteDateTime: String, sourceDB: String, originalDB: String) -> Bool {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return false
		}

		if !createTable(table) {
			return false
		}

		// look for any array objects
		var arrayKeys = [String]()
		var arrayKeyTypes = [String]()
		var arrayTypes = [ValueType]()
		var arrayValues = [AnyObject]()

		for (objectKey, objectValue) in objectValues {
			let valueType = SQLiteCore.typeOfValue(objectValue)
			if [.textArray, .intArray, .doubleArray].contains(valueType) {
				arrayKeys.append(objectKey)
				arrayTypes.append(valueType)
				arrayKeyTypes.append("\(objectKey):\(valueType.rawValue)")
				arrayValues.append(objectValue)
			}
		}

		let joinedArrayKeys = arrayKeyTypes.joined(separator: ",")

		var sql = "select key from \(esc(table.name)) where key = '\(esc(key))'"

		var tableHasKey = false
		guard let results = sqlSelect(sql) else { return false }

		if results.isEmpty {
			// key doesn't exist, insert values
			sql = "insert into \(table) (key,addedDateTime,updatedDateTime,autoDeleteDateTime,hasArrayValues"
			var placeHolders = "'\(key)','\(addedDateTime)','\(updatedDateTime)',\(deleteDateTime),'\(joinedArrayKeys)'"

			for (objectKey, objectValue) in objectValues {
				if objectKey == "key" {
					continue
				}

				let valueType = SQLiteCore.typeOfValue(objectValue)
				if [.int, .double, .text, .bool].contains(valueType) {
					sql += ",\(objectKey)"
					placeHolders += ",?"
				}
			}

			sql += ") values(\(placeHolders))"
		} else {
			tableHasKey = true
			sql = "update \(table) set updatedDateTime='\(updatedDateTime)',autoDeleteDateTime=\(deleteDateTime),hasArrayValues='\(joinedArrayKeys)'"
			for (objectKey, objectValue) in objectValues {
				if objectKey == "key" {
					continue
				}

				let valueType = SQLiteCore.typeOfValue(objectValue)
				if [.int, .double, .text, .bool].contains(valueType) {
					sql += ",\(objectKey)=?"
				}
			}
			// set unused columns to NULL
			let objectKeys = objectValues.keys
			let columns = columnsInTable(table)
			for column in columns {
				let filteredKeys = objectKeys.filter({ $0 == column.name })
				if filteredKeys.isEmpty {
					sql += ",\(column.name)=NULL"
				}
			}
			sql += " where key = '\(key)'"
		}

		if !setTableValues(objectValues: objectValues, sql: sql) {
			// adjust table columns
			validateTableColumns(table: table, objectValues: objectValues as [String: AnyObject])
			// try again
			if !setTableValues(objectValues: objectValues, sql: sql) {
				return false
			}
		}

		// process any array values
		for index in 0 ..< arrayKeys.count {
			if !setArrayValues(table: table, arrayValues: arrayValues[index] as! [AnyObject], valueType: arrayTypes[index], key: key, objectKey: arrayKeys[index]) {
				return false
			}
		}

		if syncingEnabled && unsyncedTables.doesNotContain(table) {
			let now = AgileDB.stringValueForDate(Date())
			sql = "insert into __synclog(timestamp, sourceDB, originalDB, tableName, activity, key) values('\(now)','\(sourceDB)','\(originalDB)','\(table)','U','\(esc(key))')"

			// TODO: Rework this so if the synclog stuff fails we do a rollback and return false
			if sqlExecute(sql) {
				let lastID = self.lastInsertID()

				if tableHasKey {
					sql = "delete from __synclog where tableName = '\(table)' and key = '\(self.esc(key))' and rowid < \(lastID)"
					self.sqlExecute(sql)
				}
			}
		}

		return true
	}

	private func setTableValues(objectValues: [String: AnyObject], sql: String) -> Bool {
		var successful = false

		dbQueue.sync { [weak self]() -> Void in
			guard let self = self else { return }

			self.dbCore.setTableValues(objectValues: objectValues, sql: sql, completion: { (success) -> Void in
				successful = success
				self.lock.signal()
			})
			self.lock.wait()
		}

		return successful
	}

	private func setArrayValues(table: DBTable, arrayValues: [AnyObject], valueType: ValueType, key: String, objectKey: String) -> Bool {
		var successful = sqlExecute("delete from \(table)_arrayValues where key='\(key)' and objectKey='\(objectKey)'")
		if !successful {
			return false
		}

		for value in arrayValues {
			switch valueType {
			case .textArray:
				successful = sqlExecute("insert into \(table)_arrayValues(key,objectKey,stringValue) values('\(key)','\(objectKey)','\(esc(value as! String))')")
			case .intArray:
				successful = sqlExecute("insert into \(table)_arrayValues(key,objectKey,intValue) values('\(key)','\(objectKey)',\(value as! Int))")
			case .doubleArray:
				successful = sqlExecute("insert into \(table)_arrayValues(key,objectKey,doubleValue) values('\(key)','\(objectKey)',\(value as! Double))")
			default:
				successful = true
			}

			if !successful {
				return false
			}
		}

		return true
	}

	private func deleteForKey(table: DBTable, key: String, autoDelete: Bool, sourceDB: String, originalDB: String) -> Bool {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return false
		}

		if !tables.hasTable(table) {
			return false
		}

		if !sqlExecute("delete from \(table) where key = '\(esc(key))'") || !sqlExecute("delete from \(table)_arrayValues where key = '\(esc(key))'") {
			return false
		}

		let now = AgileDB.stringValueForDate(Date())
		if syncingEnabled && unsyncedTables.doesNotContain(table) {
			var sql = ""
			// auto-deleted entries will be automatically removed from any other databases too. Don't need to log this deletion.
			if !autoDelete {
				sql = "insert into __synclog(timestamp, sourceDB, originalDB, tableName, activity, key) values('\(now)','\(sourceDB)','\(originalDB)','\(table)','D','\(esc(key))')"
				_ = sqlExecute(sql)

				let lastID = lastInsertID()
				sql = "delete from __synclog where tableName = '\(table)' and key = '\(esc(key))' and rowid < \(lastID)"
				_ = sqlExecute(sql)
			} else {
				sql = "delete from __synclog where tableName = '\(table)' and key = '\(esc(key))'"
				_ = sqlExecute(sql)
			}
		}

		return true
	}

	private func autoDelete() {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return
		}

		let now = AgileDB.stringValueForDate(Date())
		let dbTables = tables.allTables()
		for table in dbTables {
			if !AgileDB.reservedTable(table.name) {
				let sql = "select key from \(table) where autoDeleteDateTime < '\(now)'"
				if let results = sqlSelect(sql) {
					for row in results {
						let key = row.values[0] as! String
						_ = deleteForKey(table: table, key: key, autoDelete: true, sourceDB: dbInstanceKey, originalDB: dbInstanceKey)
					}
				}
			}
		}
	}

	private func dictValueFromTable(_ table: DBTable, for key: String, includeDates: Bool) -> [String: AnyObject]? {
		assert(key != "", "key value must be provided")
		let openResults = openDB()
		if case .failure(_) = openResults, !tables.hasTable(table) {
			return nil
		}

		let (sql, columns) = dictValueForKeySQL(table: table, key: key, includeDates: includeDates)
		let results = sqlSelect(sql)

		return dictValueResults(table: table, key: key, results: results, columns: columns)
	}

	private func dictValueResults(table: DBTable, key: String, results: [DBRow]?, columns: [TableColumn]) -> [String: AnyObject]? {
		guard let results = results, results.isNotEmpty else { return nil }

		var valueDict = [String: AnyObject]()
		for (columnIndex, column) in columns.enumerated() {
			let valueIndex = columnIndex + 1
			if results[0].values[valueIndex] != nil {
				if column.type == .bool, let intValue = results[0].values[valueIndex] as? Int {
					valueDict[column.name] = (intValue == 0 ? false : true) as AnyObject
				} else {
					valueDict[column.name] = results[0].values[valueIndex]
				}
			}
		}

		// handle any arrayValues
		let arrayObjects = (results[0].values[0] as! String).split { $0 == "," }.map { String($0) }
		for object in arrayObjects {
			if object == "" {
				continue
			}

			let keyType = object.split { $0 == ":" }.map { String($0) }
			let objectKey = keyType[0]
			let valueType = ValueType(rawValue: keyType[1] as String)!
			var stringArray = [String]()
			var intArray = [Int]()
			var doubleArray = [Double]()

			var arrayQueryResults: [DBRow]?
			switch valueType {
			case .textArray:
				arrayQueryResults = sqlSelect("select stringValue from \(table)_arrayValues where key = '\(key)' and objectKey = '\(objectKey)'")
			case .intArray:
				arrayQueryResults = sqlSelect("select intValue from \(table)_arrayValues where key = '\(key)' and objectKey = '\(objectKey)'")
			case .doubleArray:
				arrayQueryResults = sqlSelect("select doubleValue from \(table)_arrayValues where key = '\(key)' and objectKey = '\(objectKey)'")
				valueDict[objectKey] = doubleArray as AnyObject
			default:
				break
			}

			guard let arrayResults = arrayQueryResults else { return nil }

			for index in 0 ..< arrayResults.count {
				switch valueType {
				case .textArray:
					stringArray.append(arrayResults[index].values[0] as! String)
				case .intArray:
					intArray.append(arrayResults[index].values[0] as! Int)
				case .doubleArray:
					doubleArray.append(arrayResults[index].values[0] as! Double)
				default:
					break
				}
			}

			switch valueType {
			case .textArray:
				valueDict[objectKey] = stringArray as AnyObject
			case .intArray:
				valueDict[objectKey] = intArray as AnyObject
			case .doubleArray:
				valueDict[objectKey] = doubleArray as AnyObject
			default:
				break
			}
		}

		return valueDict
	}

	private func dictValueForKeySQL(table: DBTable, key: String, includeDates: Bool) -> (String, [TableColumn]) {
		var columns = columnsInTable(table)
		if includeDates {
			columns.append(TableColumn(name: "autoDeleteDateTime", type: .text))
			columns.append(TableColumn(name: "addedDateTime", type: .text))
			columns.append(TableColumn(name: "updatedDateTime", type: .text))
		}

		var sql = "select hasArrayValues"
		for column in columns {
			sql += ",\(column.name)"
		}
		sql += " from \(table) where key = '\(esc(key))'"

		return (sql, columns)
	}

	// MARK: - Internal Table methods
	struct TableColumn {
		fileprivate var name: String
		fileprivate var type: ValueType

		fileprivate init(name: String, type: ValueType) {
			self.name = name
			self.type = type
		}
	}

	static func reservedTable(_ table: String) -> Bool {
		return table.hasPrefix("__") || table.hasPrefix("sqlite_stat")
	}

	private func reservedColumn(_ column: String) -> Bool {
		return column == "key"
			|| column == "addedDateTime"
			|| column == "updatedDateTime"
			|| column == "autoDeleteDateTime"
			|| column == "hasArrayValues"
			|| column == "arrayValues"
	}

	private func createTable(_ table: DBTable) -> Bool {
		if tables.hasTable(table) {
			return true
		}

		if !sqlExecute("create table \(table) (key text PRIMARY KEY, autoDeleteDateTime text, addedDateTime text, updatedDateTime text, hasArrayValues text)") || !sqlExecute("create index idx_\(table)_autoDeleteDateTime on \(table)(autoDeleteDateTime)") {
			return false
		}

		if !sqlExecute("create table \(table)_arrayValues (key text, objectKey text, stringValue text, intValue int, doubleValue double)") || !sqlExecute("create index idx_\(table)_arrayValues_keys on \(table)_arrayValues(key,objectKey)") {
			return false
		}

		tables.addTable(table)

		return true
	}

	private func createIndexesForTable(_ table: DBTable) {
		if !tables.hasTable(table) {
			return
		}

		if let indexes = indexes[table.name] {
			for index in indexes {
				var indexName = index.replacingOccurrences(of: ",", with: "_")
				indexName = "idx_\(table)_\(indexName)"

				var sql = "select * from sqlite_master where tbl_name = '\(table)' and name = '\(indexName)'"
				if let results = sqlSelect(sql), results.isEmpty {
					sql = "CREATE INDEX \(indexName) on \(table)(\(index))"
					_ = sqlExecute(sql)
				}
			}
		}
	}

	private func columnsInTable(_ table: DBTable) -> [TableColumn] {
		guard let tableInfo = sqlSelect("pragma table_info(\(table))") else { return [] }
		var columns = [TableColumn]()
		for info in tableInfo {
			let columnName = info.values[1] as! String
			if !reservedColumn(columnName) {
				let rawValue = info.values[2] as! String
				let valueType = ValueType.fromRaw(rawValue)
				columns.append(TableColumn(name: columnName, type: valueType))
			}
		}

		return columns
	}

	private func validateTableColumns(table: DBTable, objectValues: [String: AnyObject]) {
		let columns = columnsInTable(table)
		// determine missing columns and add them
		for (objectKey, value) in objectValues {
			if objectKey == "key" {
				continue
			}

			assert(!reservedColumn(objectKey as String), "Reserved column")
			assert((objectKey as String).range(of: "'") == nil, "Single quote not allowed in column names")

			let found = columns.filter({ $0.name == objectKey }).isNotEmpty

			if !found {
				let valueType = SQLiteCore.typeOfValue(value)
				assert(valueType != .unknown, "column types are int, double, string, bool or arrays of int, double, or string")

				if valueType == .null {
					continue
				}

				if [.int, .double, .text].contains(valueType) {
					let sql = "alter table \(table) add column \(objectKey) \(valueType.rawValue)"
					_ = sqlExecute(sql)
				} else if valueType == .bool {
					let sql = "alter table \(table) add column \(objectKey) int"
					_ = sqlExecute(sql)
				} else {
					// array type
					let sql = "select arrayColumns from __tableArrayColumns where tableName = '\(table)'"
					if let results = sqlSelect(sql) {
						var arrayColumns = ""
						if results.isNotEmpty {
							arrayColumns = results[0].values[0] as! String
							arrayColumns += ",\(objectKey)"
							_ = sqlExecute("delete from __tableArrayColumns where tableName = '\(table)'")
						} else {
							arrayColumns = objectKey as String
						}
						_ = sqlExecute("insert into __tableArrayColumns(tableName,arrayColumns) values('\(table)','\(arrayColumns)')")
					}
				}
			}
		}

		createIndexesForTable(table)
	}

	// MARK: - SQLite execute/query
	@discardableResult
	private func sqlExecute(_ sql: String) -> Bool {
		var successful = false

		dbQueue.sync { [weak self]() -> Void in
			guard let self = self else { return }

			_ = self.dbCore.sqlExecute(sql, completion: { (success) in
				successful = success
				self.lock.signal()
			})
			self.lock.wait()
		}

		return successful
	}

	private func lastInsertID() -> sqlite3_int64 {
		var lastID: sqlite3_int64 = 0

		dbQueue.sync(execute: { [weak self]() -> Void in
			guard let self = self else { return }

			self.dbCore.lastID({ (lastInsertionID) -> Void in
				lastID = lastInsertionID
				self.lock.signal()
			})
			self.lock.wait()
		})

		return lastID
	}

	public func sqlSelect(_ sql: String) -> [DBRow]? {
		var results: RowResults = .success([])

		let openResults = openDB()
		if case .failure(_) = openResults {
			return nil
		}

		dbQueue.sync { [weak self]() -> Void in
			guard let self = self else { return }

			_ = self.dbCore.sqlSelect(sql, completion: { (rowResults) -> Void in
				results = rowResults
				self.lock.signal()
			})
			self.lock.wait()
		}

		switch results {
		case .success(let rows):
			return rows

		case .failure(_):
			return nil
		}
	}

	/**
	 Asynchronously runs a SQL command.

	 - parameter sql: The `select` SQL command to run.

	  - returns: [DBRow]
	  - throws: DBError
	  */

	public func sqlSelect(_ sql: String) async throws -> [DBRow] {
		let results = await bridgingSqlSelect(sql)

		switch results {
		case .success(let value):
			return value
		case .failure(let error):
			throw error
		}
	}

	private func bridgingSqlSelect(_ sql: String) async -> RowResults {
		await withCheckedContinuation { continuation in
			self.sqlSelect(sql) { results in
				continuation.resume(returning: results)
			}
		}
	}

	/**
	 Runs a SQL command and returns the results.

	 - parameter sql: The `select` SQL command to run.

	  - returns: Result<[DBRow], DBError>
	  */
	@discardableResult
	public func sqlSelect(_ sql: String, queue: DispatchQueue? = nil, completion: @escaping (RowResults) -> Void) -> DBCommandToken? {
		let openResults = openDB()
		if case .failure(_) = openResults {
			return nil
		}

		let blockReference: UInt = self.dbCore.sqlSelect(sql, completion: { (rowResults) -> Void in
			let dispatchQueue = queue ?? DispatchQueue.main
			dispatchQueue.async {
				let results: RowResults

				switch rowResults {
				case .success(let rows):
					results = .success(rows)

				case .failure(let error):
					results = .failure(error)
				}

				completion(results)
			}
		})

		return DBCommandToken(database: self, identifier: blockReference)
	}

	func dequeueCommand(_ commandReference: UInt) -> Bool {
		var removed = true

		dbQueue.sync { [weak self]() -> Void in
			guard let self = self else { return }

			self.dbCore.removeExecutionBlock(commandReference, completion: { (results) -> Void in
				removed = results
				self.lock.signal()
			})
			self.lock.wait()
		}

		return removed
	}
}

// MARK: - SQLiteCore
private extension AgileDB {
	final class SQLiteCore: Thread {
		var isOpen = false
		var isDebugging = false

		private struct ExecutionBlock {
			var block: Any
			var blockReference: UInt
		}

		private var sqliteDB: OpaquePointer?
		private var threadLock = DispatchSemaphore(value: 0)
		private var queuedBlocks = [ExecutionBlock]()
		private var autoCloseTimer: RepeatingTimer?
		private var dbFilePath = ""
		private var autoCloseTimeout: TimeInterval = 0
		private var lastActivity: Double = 0
		private var automaticallyClosed = false
		private let blockQueue = DispatchQueue(label: "com.AaronLBratcher.AgileDBBlockQueue", attributes: [])
		private var blockReference: UInt = 1

		private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

		class func typeOfValue(_ value: AnyObject) -> ValueType {
			let valueType: ValueType

			switch value {
			case is [String]:
				valueType = .textArray
			case is [Int]:
				valueType = .intArray
			case is [Double]:
				valueType = .doubleArray
			case is String:
				valueType = .text
			case is Int:
				valueType = .int
			case is Double:
				valueType = .double
			case is Bool:
				valueType = .bool
			case is NSNull:
				valueType = .null
			default:
				valueType = .unknown
			}

			return valueType
		}

		func openDBFile(_ dbFilePath: String, autoCloseTimeout: Int, completion: @escaping (_ successful: BoolResults, _ openedFromOtherThread: Bool, _ fileExists: Bool) -> Void) {
			self.autoCloseTimeout = TimeInterval(exactly: autoCloseTimeout) ?? 0.0
			self.dbFilePath = dbFilePath
			if isDebugging {
				print(dbFilePath)
			}

			let block = { [unowned self] in
				let fileExists = FileManager.default.fileExists(atPath: dbFilePath)
				if self.isOpen {
					completion(BoolResults.success(true), true, fileExists)
					return
				}

				if autoCloseTimeout > 0 {
					self.autoCloseTimer = RepeatingTimer(timeInterval: self.autoCloseTimeout) {
						self.close(automatically: true)
					}
				}

				let openResults = self.openFile()
				switch openResults {
				case .success(_):
					self.isOpen = true
					completion(BoolResults.success(true), false, fileExists)

				case .failure(let error):
					self.isOpen = false
					completion(BoolResults.failure(error), false, fileExists)
				}

				return
			}

			addBlock(block)
		}

		func close(automatically: Bool = false) {
			let block = { [unowned self] in
				if automatically {
					if self.automaticallyClosed || Date().timeIntervalSince1970 < (self.lastActivity + Double(self.autoCloseTimeout)) {
						return
					}

					self.automaticallyClosed = true
				} else {
					self.isOpen = false
				}

				sqlite3_close_v2(self.sqliteDB)
				self.sqliteDB = nil
			}

			addBlock(block)
		}

		func lastID(_ completion: @escaping (_ lastInsertionID: sqlite3_int64) -> Void) {
			let block = { [unowned self] in
				completion(sqlite3_last_insert_rowid(self.sqliteDB))
			}

			addBlock(block)
		}

		func sqlExecute(_ sql: String, completion: @escaping (_ success: Bool) -> Void) -> UInt {
			let block = { [unowned self] in
				var dbps: OpaquePointer?
				defer {
					if dbps != nil {
						sqlite3_finalize(dbps)
					}
				}

				var status = sqlite3_prepare_v2(self.sqliteDB, sql, -1, &dbps, nil)
				if status != SQLITE_OK {
					self.displaySQLError(sql)
					completion(false)
					return
				}

				status = sqlite3_step(dbps)
				if status != SQLITE_DONE && status != SQLITE_OK {
					self.displaySQLError(sql)
					completion(false)
					return
				}

				completion(true)
				return
			}

			return addBlock(block)
		}

		func sqlSelect(_ sql: String, completion: @escaping (_ results: RowResults) -> Void) -> UInt {
			let block = { [unowned self] in
				var rows = [DBRow]()
				var dbps: OpaquePointer?
				defer {
					if dbps != nil {
						sqlite3_finalize(dbps)
					}
				}

				var status = sqlite3_prepare_v2(self.sqliteDB, sql, -1, &dbps, nil)
				if status != SQLITE_OK {
					self.displaySQLError(sql)
					completion(RowResults.failure(DBError(rawValue: Int(status))))
					return
				}

				if self.isDebugging {
					self.explain(sql)
				}

				repeat {
					status = sqlite3_step(dbps)
					if status == SQLITE_ROW {
						var row = DBRow()
						let count = sqlite3_column_count(dbps)
						for index in 0 ..< count {
							let columnType = sqlite3_column_type(dbps, index)
							switch columnType {
							case SQLITE_TEXT:
								let value = String(cString: sqlite3_column_text(dbps, index))
								row.values.append(value as AnyObject)
							case SQLITE_INTEGER:
								row.values.append(Int(sqlite3_column_int64(dbps, index)) as AnyObject)
							case SQLITE_FLOAT:
								row.values.append(Double(sqlite3_column_double(dbps, index)) as AnyObject)
							default:
								row.values.append(nil)
							}
						}

						rows.append(row)
					}
				} while status == SQLITE_ROW

				if status != SQLITE_DONE {
					self.displaySQLError(sql)
					completion(RowResults.failure(DBError(rawValue: Int(status))))
					return
				}

				completion(RowResults.success(rows))
				return
			}

			return addBlock(block)
		}

		func removeExecutionBlock(_ blockReference: UInt, completion: @escaping (_ success: Bool) -> Void) {
			let block = {
				var blockArrayIndex: Int?
				for i in 0..<self.queuedBlocks.count {
					if self.queuedBlocks[i].blockReference == blockReference {
						blockArrayIndex = i
						break
					}
				}

				if let blockArrayIndex = blockArrayIndex {
					self.queuedBlocks.remove(at: blockArrayIndex)
					completion(true)
				} else {
					completion(false)
				}
			}

			blockQueue.sync {
				if blockReference > (UInt.max - 5) {
					self.blockReference = 1
				} else {
					self.blockReference += 1
				}

				let executionBlock = ExecutionBlock(block: block, blockReference: blockReference)

				queuedBlocks.insert(executionBlock, at: 0)
				threadLock.signal()
			}

		}

		func setTableValues(objectValues: [String: AnyObject], sql: String, completion: @escaping (_ success: Bool) -> Void) {
			let block = { [unowned self] in
				var dbps: OpaquePointer?
				defer {
					if dbps != nil {
						sqlite3_finalize(dbps)
					}
				}

				var status = sqlite3_prepare_v2(self.sqliteDB, sql, -1, &dbps, nil)
				if status != SQLITE_OK {
					self.displaySQLError(sql)
					completion(false)
					return
				} else {
					// try to bind the object properties to table fields.
					var index: Int32 = 1

					for (objectKey, objectValue) in objectValues {
						if objectKey == "key" {
							continue
						}

						let valueType = SQLiteCore.typeOfValue(objectValue)
						guard [.int, .double, .text, .bool].contains(valueType) else { continue }

						let value: AnyObject
						if valueType == .bool, let boolValue = objectValue as? Bool {
							value = (boolValue ? 1 : 0) as AnyObject
						} else {
							value = objectValue
						}

						status = self.bindValue(dbps!, index: index, value: value)
						if status != SQLITE_OK {
							self.displaySQLError(sql)
							completion(false)
							return
						}

						index += 1
					}

					status = sqlite3_step(dbps)
					if status != SQLITE_DONE && status != SQLITE_OK {
						self.displaySQLError(sql)
						completion(false)
						return
					}
				}

				completion(true)
				return
			}

			addBlock(block)
		}

		private func bindValue(_ statement: OpaquePointer, index: Int32, value: AnyObject) -> Int32 {
			var status = SQLITE_OK
			let valueType = SQLiteCore.typeOfValue(value)

			switch valueType {
			case .text:
				status = sqlite3_bind_text(statement, index, value as! String, -1, SQLITE_TRANSIENT)
			case .int:
				let int64Value = Int64(value as! Int)
				status = sqlite3_bind_int64(statement, index, int64Value)
			case .double:
				status = sqlite3_bind_double(statement, index, value as! Double)
			case .bool:
				status = sqlite3_bind_int(statement, index, Int32(value as! Int))
			default:
				status = SQLITE_OK
			}

			return status
		}

		private func displaySQLError(_ sql: String) {
			if !isDebugging { return }

			print("Error: \(dbErrorMessage)")
			print("     on command - \(sql)")
			print("")
		}

		private var dbErrorMessage: String {
			guard let message = UnsafePointer<Int8>(sqlite3_errmsg(sqliteDB)) else { return "Unknown Error" }
			return String(cString: message)
		}

		private func explain(_ sql: String) {
			var dbps: OpaquePointer?
			let explainCommand = "EXPLAIN QUERY PLAN \(sql)"
			sqlite3_prepare_v2(sqliteDB, explainCommand, -1, &dbps, nil)
			print("\n\n.  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  \nQuery:\(sql)\n\nAnalysis:\n")
			while (sqlite3_step(dbps) == SQLITE_ROW) {
				let iSelectid = sqlite3_column_int(dbps, 0)
				let iOrder = sqlite3_column_int(dbps, 1)
				let iFrom = sqlite3_column_int(dbps, 2)
				let value = String(cString: sqlite3_column_text(dbps, 3))

				print("\(iSelectid) \(iOrder) \(iFrom) \(value)\n=================================================\n\n")
			}

			sqlite3_finalize(dbps)
		}

		@discardableResult
		private func addBlock(_ block: Any) -> UInt {
			var executionBlockReference: UInt = 0
			
			blockQueue.sync {
				if blockReference > (UInt.max - 5) {
					blockReference = 1
				} else {
					blockReference += 1
				}
				executionBlockReference = blockReference
			}

			blockQueue.async {
				let executionBlock = ExecutionBlock(block: block, blockReference: self.blockReference)

				self.queuedBlocks.append(executionBlock)
				self.threadLock.signal()
			}

			return executionBlockReference
		}

		override func main() {
			while true {
				autoCloseTimer?.suspend()

				if automaticallyClosed {
					let results = openFile()
					if case .failure(_) = results {
						fatalError("Unable to open DB")
					}
				}

				var hasBlocks = false
				blockQueue.sync {
					hasBlocks = queuedBlocks.isNotEmpty
				}

				while hasBlocks {
					if isDebugging {
						Thread.sleep(forTimeInterval: 0.1)
					}

					blockQueue.sync {
						if let executionBlock = queuedBlocks.first, let block = executionBlock.block as? () -> Void {
							queuedBlocks.removeFirst()
							block()
						}

						hasBlocks = queuedBlocks.isNotEmpty
					}
				}

				lastActivity = Date().timeIntervalSince1970
				if !automaticallyClosed {
					autoCloseTimer?.resume()
				}

				threadLock.wait()
			}
		}

		private func openFile() -> Result<Bool, DBError> {
			sqliteDB = nil
			let status = sqlite3_open_v2(dbFilePath.cString(using: .utf8)!, &self.sqliteDB, SQLITE_OPEN_FILEPROTECTION_COMPLETE | SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, nil)

			if status != SQLITE_OK {
				if isDebugging {
					print("Error opening SQLite Database: \(status)")
				}
				return BoolResults.failure(DBError(rawValue: Int(status)))
			}

			autoCloseTimer?.resume()
			automaticallyClosed = false
			return BoolResults.success(true)
		}
	}
}

// MARK: - String Extensions
private extension String {
	func dataValue() -> Data {
		return data(using: .utf8, allowLossyConversion: false)!
	}
}

private class RepeatingTimer {
	private enum State {
		case suspended
		case resumed
	}

	private let timeInterval: TimeInterval
	private let eventHandler: (() -> Void)
	private var state = State.suspended
	private lazy var timer: DispatchSourceTimer = makeTimer()

	init(timeInterval: TimeInterval = 60.0, eventHandler: @escaping (() -> Void)) {
		self.timeInterval = timeInterval
		self.eventHandler = eventHandler
	}

	private func makeTimer() -> DispatchSourceTimer {
		let timer = DispatchSource.makeTimerSource()
		timer.schedule(deadline: .now() + self.timeInterval, repeating: self.timeInterval)
		timer.setEventHandler(handler: { [weak self] in
			self?.eventHandler()
		})
		return timer
	}

	deinit {
		timer.setEventHandler { }
		timer.cancel()
		resume()
	}

	func resume() {
		if state == .resumed { return }

		state = .resumed
		timer.resume()
	}

	func suspend() {
		if state == .suspended { return }

		state = .suspended
		timer.suspend()
	}
}

fileprivate extension Collection {
	var isNotEmpty: Bool {
		return !isEmpty
	}
}

fileprivate extension Array where Element: Equatable {
	func doesNotContain(_ element: Element) -> Bool {
		return !contains(element)
	}
}

