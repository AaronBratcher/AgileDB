//
// AgileDB.swift
//
// Created by Aaron Bratcher on 01/08/2015.
// Copyright (c) 2015 – 2020 Aaron L Bratcher. All rights reserved.
//

import Foundation
import SQLite3

public actor AgileDB {
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
	private var isDebugging = false {
		didSet {
			dbCore.isDebugging = isDebugging
		}
	}

	/**
	The number of seconds to wait after inactivity before automatically closing the file. File is automatically opened for next activity. A value of 0 means never close automatically. Default is 2.
	*/
	public var autoCloseTimeout = 2

	/**
	Print the SQL commands
	 */
	public var printSQL = false

	/**
	Read-only array of unsynced tables.  Any tables not in this array will be synced.
	*/
	private(set) public var unsyncedTables: [DBTable] = []

	nonisolated(unsafe) public static var dateFormatter: DateFormatter = {
		let dateFormatter = DateFormatter()
		dateFormatter.calendar = Calendar(identifier: .gregorian)
		dateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss'.'SSSZZZZZ"

		return dateFormatter
	}()

	// MARK: - Private properties
	private struct DBTables {
		private var tables: [DBTable] = []

		func allTables() -> [DBTable] {
			return tables
		}

		mutating func addTable(_ table: DBTable) {
			tables.append(DBTable(name: table.name))
		}

		mutating func dropTable(_ table: DBTable) {
			tables = tables.filter({ $0 != table })
		}

		mutating func dropAllTables() {
			tables = []
		}

		func hasTable(_ table: DBTable) -> Bool {
			return tables.contains(table)
		}
	}

	// nonisolated: SQLiteCore manages its own concurrency via blockQueue + Thread
	private nonisolated let dbCore = SQLiteCore()
	private var dbFileLocation: URL?
	private var dbInstanceKey = ""
	private var tables = DBTables()
	private var indexes = [String: [String]]()
	private var syncingEnabled = false
	private var publishers = [UpdatablePublisher]()
	private lazy var autoDeleteTimer: RepeatingTimer = {
		return RepeatingTimer(timeInterval: 60) { [weak self] in
			guard let self = self else { return }
			Task { await self.autoDelete() }
		}
	}()

	// MARK: - Init
	/**
	Instantiates an instance of AgileDB

	- parameter location: Optional file location if different than the default.
	*/
	public init(fileLocation: URL? = nil, isDebugging: Bool = false) {
		dbFileLocation = fileLocation
		self.isDebugging = isDebugging
		dbCore.qualityOfService = .userInteractive
		dbCore.start()
	}

	var fileLocation: URL? {
		dbFileLocation
	}

	// MARK: - Open / Close
	/**
	Opens the database file.

	- parameter location: Optional file location if different than the default.

	- returns: Bool Returns if the database could be successfully opened.
	*/
	@discardableResult
	public func open(_ location: URL? = nil) async -> Bool {
		if let location = location {
			if dbCore.isOpen { close() }
			self.dbFileLocation = location
		}

		let openResults = await openDB()
		if case .success = openResults {
			return true
		}
		return false
	}

	/**
	Close the database.
	*/
	public func close() {
		autoDeleteTimer.suspend()
		dbCore.close()
	}

	// MARK: - Keys

	/**
	 Asynchronously checks if the given table contains the given key.

	 - parameter table: The table to search.
	 - parameter key: The key to look for.

	  - returns: Bool
	  - throws: DBError
	  */
	public func tableHasKey(table: DBTable, key: String) async throws -> Bool {
		let openResults = await openDB()
		if case .failure(let error) = openResults { throw error }
		if !tables.hasTable(table) { return false }
		let sql = "select 1 from \(table) where key = '\(key)'"
		guard let results = await sqlRows(sql) else { throw DBError.other(0) }
		return results.isNotEmpty
	}

	/**
	 Asynchronously checks if the given table contains all the given keys.

	 - parameter table: The table to search.
	 - parameter keys: The keys to look for.

	  - returns: Bool
	  - throws: DBError
	  */
	public func tableHasAllKeys(table: DBTable, keys: [String]) async throws -> Bool {
		let openResults = await openDB()
		if case .failure(let error) = openResults { throw error }
		if !tables.hasTable(table) { return false }
		let keyString = keys.map({ "'\($0)'" }).joined(separator: ",")
		let sql = "select 1 from \(table) where key in (\(keyString))"
		guard let results = await sqlRows(sql) else { throw DBError.other(0) }
		return results.count == keys.count
	}

	/**
	Asynchronously checks if the given table contains the given key.

	- parameter table: The table to search.
	- parameter key: The key to look for.
	- parameter queue: Dispatch queue to use when running the completion closure. Default value is main queue.
	- parameter completion: Closure to use for results.

	- returns: DBCommandToken that can be used to cancel the command before it executes. Nil if the database file cannot be opened.
	*/
	@discardableResult
	public func tableHasKey(table: DBTable, key: String, queue: DispatchQueue? = nil, completion: @escaping (BoolResults) -> Void) -> DBCommandToken? {
		let openResults = openDB_sync()
		if case .failure = openResults { return nil }

		if !tables.hasTable(table) {
			let dispatchQueue = queue ?? DispatchQueue.main
			dispatchQueue.async { completion(.success(false)) }
			return DBCommandToken(database: self, identifier: 0)
		}

		let sql = "select 1 from \(table) where key = '\(key)'"
		let blockReference = dbCore.sqlSelect(sql, completion: { (rowResults) -> Void in
			let dispatchQueue = queue ?? DispatchQueue.main
			dispatchQueue.async {
				switch rowResults {
				case .success(let rows): completion(.success(rows.isNotEmpty))
				case .failure(let error): completion(.failure(error))
				}
			}
		})

		return DBCommandToken(database: self, identifier: blockReference)
	}

	/**
	 Asynchronously checks if the given table contains all the given keys.

	 - parameter table: The table to search.
	 - parameter keys: The keys to look for.
	 - parameter queue: Dispatch queue to use when running the completion closure. Default value is main queue.
	 - parameter completion: Closure to use for results.

	 - returns: DBCommandToken that can be used to cancel the command before it executes. Nil if the database file cannot be opened.
	 */
	@discardableResult
	public func tableHasAllKeys(table: DBTable, keys: [String], queue: DispatchQueue? = nil, completion: @escaping (BoolResults) -> Void) -> DBCommandToken? {
		let openResults = openDB_sync()
		if case .failure = openResults { return nil }

		if !tables.hasTable(table) {
			let dispatchQueue = queue ?? DispatchQueue.main
			dispatchQueue.async { completion(.success(false)) }
			return DBCommandToken(database: self, identifier: 0)
		}

		let keyString = keys.map({ "'\($0)'" }).joined(separator: ",")
		let sql = "select 1 from \(table) where key in (\(keyString))"
		let blockReference = dbCore.sqlSelect(sql, completion: { (rowResults) -> Void in
			let dispatchQueue = queue ?? DispatchQueue.main
			dispatchQueue.async {
				switch rowResults {
				case .success(let rows): completion(.success(rows.count == keys.count))
				case .failure(let error): completion(.failure(error))
				}
			}
		})

		return DBCommandToken(database: self, identifier: blockReference)
	}

	/**
	 Asynchronously returns keys in given table.

	  - parameter table: The table to return keys from.
	  - parameter sortOrder: Optional string that gives a comma delimited list of properties to sort by.
	  - parameter conditions: Optional array of DBConditions that specify what conditions must be met.
	  - parameter validateObjects: Optional bool. Default value is false.

	  - returns: [String]
	  - throws: DBError
	  */
	public func keysInTable(_ table: DBTable, sortOrder: String? = nil, conditions: [DBCondition]? = nil, validateObjects: Bool = false) async throws -> [String] {
		let openResults = await openDB()
		if case .failure(let error) = openResults { throw error }
		if !tables.hasTable(table) { throw DBError.tableNotFound }

		guard let sql = await keysInTableSQL(table: table, sortOrder: sortOrder, conditions: conditions, validateObjecs: validateObjects) else {
			throw DBError.cannotParseData
		}

		let rowResults: RowResults = await withCheckedContinuation { continuation in
			_ = dbCore.sqlSelect(sql) { continuation.resume(returning: $0) }
		}

		switch rowResults {
		case .success(let rows): return rows.map({ $0.values[0] as! String })
		case .failure(let error): throw error
		}
	}

	/**
	Asynchronously returns the keys in the given table via a completion closure.

	- parameter table: The table to return keys from.
	- parameter sortOrder: Optional string that gives a comma delimited list of properties to sort by.
	- parameter conditions: Optional array of DBConditions.
	- parameter validateObjects: Optional bool. Default value is false.
	- parameter queue: Optional dispatch queue to use when running the completion closure. Default value is main queue.
	- parameter completion: Closure with KeyResults.

	- returns: DBCommandToken that can be used to cancel the command before it executes.
	*/
	@discardableResult
	public func keysInTable(_ table: DBTable, sortOrder: String? = nil, conditions: [DBCondition]? = nil, validateObjects: Bool = false, queue: DispatchQueue? = nil, completion: @escaping (KeyResults) -> Void) -> DBCommandToken? {
		let openResults = openDB_sync()
		if case .failure = openResults {
			completion(.failure(.cannotOpenFile))
			return nil
		}

		if !tables.hasTable(table) {
			completion(.failure(.tableNotFound))
			return DBCommandToken(database: self, identifier: 0)
		}

		// keysInTableSQL needs actor isolation; run async and capture the token
		Task {
			guard let sql = await keysInTableSQL(table: table, sortOrder: sortOrder, conditions: conditions, validateObjecs: validateObjects) else {
				(queue ?? .main).async { completion(.failure(.cannotParseData)) }
				return
			}

			_ = dbCore.sqlSelect(sql) { rowResults in
				let dispatchQueue = queue ?? DispatchQueue.main
				dispatchQueue.async {
					switch rowResults {
					case .success(let rows): completion(.success(rows.map({ $0.values[0] as! String })))
					case .failure(let error): completion(.failure(error))
					}
				}
			}
		}

		return DBCommandToken(database: self, identifier: 0)
	}

	/**
	 Returns a Publisher for generic DBResults.

	 - parameter sortOrder: Optional string that gives a comma delimited list of properties to sort by.
	 - parameter conditions: Optional array of DBConditions.
	 - parameter validateObjects: Default value is false.

	 - returns: DBResultsPublisher
	 */
	@discardableResult
	public func publisher<T>(sortOrder: String? = nil, conditions: [DBCondition]? = nil, validateObjects: Bool = false) -> DBResultsPublisher<T> {
		let publisher = DBResultsPublisher<T>(db: self, table: T.table, sortOrder: sortOrder, conditions: conditions, validateObjects: validateObjects)
		publishers.append(publisher)
		return publisher
	}

	// MARK: - Indexing
	/**
	Sets the indexes desired for a given table.

	- parameter table: The table to return keys from.
	- parameter indexes: An array of table properties to be indexed.
	*/
	@discardableResult
	public func setIndexesForTable(_ table: DBTable, to indexes: [String]) async -> BoolResults {
		let openResults = await openDB()
		if case .success = openResults {
			self.indexes[table.name] = indexes
			await createIndexesForTable(table)
		}

		return openResults
	}

	// MARK: - Set Values
	/**
	Sets the value of an entry in the given table for a given key.

	- parameter table: The table to set the value in.
	- parameter key: The key for the entry.
	- parameter value: A JSON string representing the value to be stored.
	- parameter autoDeleteAfter: Optional date of when the value should be automatically deleted.

	- returns: Bool If the value was set successfully.
	*/
	@discardableResult
	public func setValueInTable(_ table: DBTable, for key: String, to value: String, autoDeleteAfter: Date? = nil) async -> Bool {
		assert(key != "", "key must be provided")
		assert(value != "", "value must be provided")

		guard let dataValue = value.data(using: .utf8) else { return false }

		let objectValues = (try? JSONSerialization.jsonObject(with: dataValue, options: .mutableContainers)) as? [String: AnyObject]
		assert(objectValues != nil, "Value must be valid JSON string that is a dictionary for the top-level object")

		return await setValueInTable(table, for: key, to: objectValues!, autoDeleteAfter: autoDeleteAfter)
	}

	/**
	Sets the value of an entry in the given table for a given key.

	- parameter table: The table to set the value in.
	- parameter key: The key for the entry.
	- parameter value: A dictionary object representing the value to be stored.
	- parameter autoDeleteAfter: Optional date of when the value should be automatically deleted.

	- returns: Bool If the value was set successfully.
	*/
	@discardableResult
	public func setValueInTable(_ table: DBTable, for key: String, to objectValues: [String: AnyObject], autoDeleteAfter: Date? = nil) async -> Bool {
		let now = AgileDB.stringValueForDate(Date())
		let deleteDateTime = (autoDeleteAfter == nil ? "NULL" : "'" + AgileDB.stringValueForDate(autoDeleteAfter!) + "'")

		let successful = await setValue(table: table, key: key, objectValues: objectValues, addedDateTime: now, updatedDateTime: now, deleteDateTime: deleteDateTime, sourceDB: dbInstanceKey, originalDB: dbInstanceKey)

		if successful {
			await updatePublisherResults(for: key, in: table)
		}

		return successful
	}

	// MARK: - Return Values
	/**
	Asynchronously returns the JSON value of what was stored for a given table and key.

	- parameter table: The table to return the value from.
	- parameter key: The key for the entry.

	- returns: String JSON representation.
	- throws: DBError
	*/
	public func valueFromTable(_ table: DBTable, for key: String) async throws -> String {
		let dictionaryValue = try await dictValueFromTable(table, for: key)
		guard let dataValue = try? JSONSerialization.data(withJSONObject: dictionaryValue, options: JSONSerialization.WritingOptions(rawValue: 0)),
			  let jsonValue = String(data: dataValue, encoding: .utf8) else {
			throw DBError.other(0)
		}
		return jsonValue
	}

	/**
	  Asynchronously returns the dictionary value of what was stored for a given table and key.

	  - parameter table: The table to return the value from.
	  - parameter key: The key for the entry.

	  - returns: [String: AnyObject]
	  - throws: DBError
	  */
	public func dictValueFromTable(_ table: DBTable, for key: String) async throws -> [String: AnyObject] {
		guard let value = await dictValueFromTable(table, for: key, includeDates: false) else {
			throw DBError.other(0)
		}
		return value
	}

	// MARK: - Delete
	/**
	Delete the value from the given table for the given key.

	- parameter table: The table to delete from.
	- parameter key: The key for the entry.

	- returns: Bool Value was successfuly removed.
	*/
	@discardableResult
	public func deleteFromTable(_ table: DBTable, for key: String) async -> Bool {
		assert(key != "", "key must be provided")

		// Capture the publishers whose results currently include this key before deleting it.
		// Afterwards the key no longer matches any query, so membership can't be determined.
		let affectedPublishers = await publishersContaining(key: key, in: table)

		let deleted = await deleteForKey(table: table, key: key, autoDelete: false, sourceDB: dbInstanceKey, originalDB: dbInstanceKey)
		if deleted {
			for publisher in affectedPublishers {
				publisher.updateSubject()
			}
		}

		return deleted
	}

	/**
	Removes the given table and associated values.

	- parameter table: The table to remove.

	- returns: Bool Table was successfuly removed.
	*/
	@discardableResult
	public func dropTable(_ table: DBTable) async -> Bool {
		let openResults = await openDB()
		if case .failure = openResults {
			return false
		}

		let d1 = await sqlExecute("drop table \(table)")
		let d2 = await sqlExecute("drop table \(table)_arrayValues")
		let d3 = await sqlExecute("delete from __tableArrayColumns where tableName = '\(table)'")
		if !d1 || !d2 || !d3 {
			return false
		}

		tables.dropTable(table)

		if syncingEnabled && unsyncedTables.doesNotContain(table) {
			let now = AgileDB.stringValueForDate(Date())
			if !(await sqlExecute("insert into __synclog(timestamp, sourceDB, originalDB, tableName, activity, key) values('\(now)','\(dbInstanceKey)','\(dbInstanceKey)','\(table)','X',NULL)")) {
				return false
			}

			let lastID = await lastInsertID()

			if !(await sqlExecute("delete from __synclog where tableName = '\(table)' and rowid < \(lastID)")) {
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
	public func dropAllTables() async -> Bool {
		let openResults = await openDB()
		if case .failure = openResults {
			return false
		}

		let dbTables = tables.allTables()
		for table in dbTables {
			if !(await dropTable(table)) {
				return false
			}
		}

		tables.dropAllTables()

		return true
	}

	// MARK: - Sync
	/**
	Current syncing status. Nil if the database has not been opened yet.
	*/
	public var isSyncingEnabled: Bool? {
		dbCore.isOpen ? syncingEnabled : nil
	}

	/**
	Enables syncing.

	- returns: Bool If syncing was successfully enabled.
	*/
	public func enableSyncing() async -> Bool {
		let openResults = await openDB()
		if case .failure = openResults { return false }

		if syncingEnabled { return true }

		if !(await sqlExecute("create table __synclog(timestamp text, sourceDB text, originalDB text, tableName text, activity text, key text)")) {
			return false
		}
		await sqlExecute("create index __synclog_index on __synclog(tableName,key)")
		await sqlExecute("create index __synclog_source on __synclog(sourceDB,originalDB)")
		await sqlExecute("create table __unsyncedTables(tableName text)")

		let now = AgileDB.stringValueForDate(Date())
		let dbTables = tables.allTables()
		for table in dbTables {
			if !(await sqlExecute("insert into __synclog(timestamp, sourceDB, originalDB, tableName, activity, key) select '\(now)','\(dbInstanceKey)','\(dbInstanceKey)','\(table.name)','U',key from \(table.name)")) {
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
	public func disableSyncing() async -> Bool {
		let openResults = await openDB()
		if case .failure = openResults { return false }

		if !syncingEnabled { return true }

		let ds1 = await sqlExecute("drop table __synclog")
		let ds2 = await sqlExecute("drop table __unsyncedTables")
		if !ds1 || !ds2 {
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
	public func setUnsyncedTables(_ tables: [DBTable]) async -> Bool {
		let openResults = await openDB()
		if case .failure = openResults { return false }

		if !syncingEnabled {
			print("syncing must be enabled before setting unsynced tables")
			return false
		}

		unsyncedTables = [DBTable]()
		await sqlExecute("delete from __unsyncedTables")
		for table in tables {
			await sqlExecute("delete from __synclog where tableName = '\(table)'")
			await sqlExecute("insert into __unsyncedTables(tableName) values('\(table)')")
			unsyncedTables.append(table)
		}

		return true
	}

	/**
	Creates a sync file that can be used on another AgileDB instance to sync data.

	- parameter filePath: The full path to be used for the log file.
	- parameter lastSequence: The last sequence used for the given target.
	- parameter targetDBInstanceKey: The dbInstanceKey of the target database.

	- returns: (Bool,Int) If the file was successfully created and the lastSequence to use in subsequent calls.
	*/
	public func createSyncFileAtURL(_ localURL: URL!, lastSequence: Int, targetDBInstanceKey: String) async -> (Bool, Int) {
		let openResults = await openDB()
		if case .failure = openResults { return (false, lastSequence) }

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
			guard let results = await sqlRows("select rowid,timestamp,originalDB,tableName,activity,key from __synclog where rowid > \(lastSequence) and sourceDB <> '\(targetDBInstanceKey)' and originalDB <> '\(targetDBInstanceKey)' order by rowid") else {
				try? FileManager.default.removeItem(atPath: filePath)
				return (false, lastSequence)
			}

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
						guard let dictValue = await dictValueFromTable(DBTable(name: tableName), for: key, includeDates: true) else { continue }
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
		}

		return (false, lastSequence)
	}


	/**
	Processes a sync file created by another instance of AgileDB.

	- parameter filePath: The path to the sync file.
	- parameter syncProgress: Optional function that will be called periodically giving the percent complete.

	- returns: (Bool,String,Int) If the sync file was successfully processed, the instanceKey of the submiting DB, and the lastSequence.
	*/
	public typealias syncProgressUpdate = (_ percentComplete: Double) -> Void
	public func processSyncFileAtURL(_ localURL: URL!, syncProgress: syncProgressUpdate?) async -> (Bool, String, Int) {
		let openResults = await openDB()
		if case .failure = openResults { return (false, "", 0) }

		if !syncingEnabled {
			print("syncing must be enabled before processing sync file")
			return (false, "", 0)
		}

		await autoDelete()

		let filePath = localURL.path

		if FileHandle(forReadingAtPath: filePath) != nil {
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

						if activity == "D" || activity == "U" {
							if let key = entry["key"] as? String,
							   let results = await sqlRows("select 1 from __synclog where tableName = '\(tableName)' and key = '\(key)' and timestamp > '\(timeStamp)'") {
								if results.isEmpty {
									if activity == "U" {
										var objectValues = entry["value"] as! [String: AnyObject]
										let addedDateTime = objectValues["addedDateTime"] as! String
										let updatedDateTime = objectValues["updatedDateTime"] as! String
										let deleteDateTime = (objectValues["deleteDateTime"] == nil ? "NULL" : objectValues["deleteDateTime"] as! String)
										objectValues.removeValue(forKey: "addedDateTime")
										objectValues.removeValue(forKey: "updatedDateTime")
										objectValues.removeValue(forKey: "deleteDateTime")

										_ = await setValue(table: DBTable(name: tableName), key: key, objectValues: objectValues, addedDateTime: addedDateTime, updatedDateTime: updatedDateTime, deleteDateTime: deleteDateTime, sourceDB: sourceDB, originalDB: originalDB)
									} else {
										_ = await deleteForKey(table: DBTable(name: tableName), key: key, autoDelete: false, sourceDB: sourceDB, originalDB: originalDB)
									}
								}
							}
						} else {
							await sqlExecute("delete from \(tableName) where key in (select key from __synclog where tableName = '\(tableName)' and timeStamp < '\(timeStamp)')")
							await sqlExecute("delete from \(tableName)_arrayValues where key in (select key from __synclog where tableName = '\(tableName)' and timeStamp < '\(timeStamp)')")
							await sqlExecute("delete from __synclog where tableName = '\(tableName)' and timeStamp < '\(timeStamp)'")
							await sqlExecute("insert into __synclog(timestamp, sourceDB, originalDB, tableName, activity, key) values('\(now)','\(sourceDB)','\(originalDB)','\(tableName)','X',NULL)")
						}
					}

					for publisher in publishers {
						publisher.updateSubject()
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
	 Check for the existance of a given table.
	 - parameter table: The table to check.

	 - returns: Bool the existence of a specified table
	 */
	public func hasTable(_ table: DBTable) async -> Bool {
		let openResults = await openDB()
		if case .success = openResults {
			return tables.hasTable(table)
		}
		return false
	}

	/**
	The instanceKey for this database instance. Nil when database has not been opened.
	*/
	public var instanceKey: String? {
		dbCore.isOpen ? dbInstanceKey : nil
	}

	/**
	Replace single quotes with two single quotes for use in SQL commands.
	*/
	public nonisolated func esc(_ source: String) -> String {
		return source.replacingOccurrences(of: "'", with: "''")
	}

	/**
	String value for a given date.
	*/
	public static func stringValueForDate(_ date: Date) -> String {
		return AgileDB.dateFormatter.string(from: date)
	}

	/**
	Date value for given string.
	*/
	public static func dateValueForString(_ stringValue: String) -> Date? {
		return AgileDB.dateFormatter.date(from: stringValue)
	}

	// MARK: - Internal Initialization Methods
	private func openDB() async -> BoolResults {
		if dbCore.isOpen {
			return BoolResults.success(true)
		}

		return await openDB_async()
	}

	/// Synchronous open check — only checks isOpen, does not open. Used by callback methods.
	private func openDB_sync() -> BoolResults {
		dbCore.isOpen ? .success(true) : .failure(.cannotOpenFile)
	}

	private func openDB_async() async -> BoolResults {
		let filePath: String

		if let _dbFileLocation = self.dbFileLocation {
			filePath = _dbFileLocation.path
		} else {
			filePath = defaultFileLocation()
			dbFileLocation = URL(fileURLWithPath: filePath)
		}

		dbFilePath = filePath

		let (openResults, previouslyOpened, fileExists): (BoolResults, Bool, Bool) = await withCheckedContinuation { continuation in
			dbCore.openDBFile(filePath, autoCloseTimeout: self.autoCloseTimeout) { results, alreadyOpen, alreadyExists in
				continuation.resume(returning: (results, alreadyOpen, alreadyExists))
			}
		}

		if case .success = openResults, !previouslyOpened {
			if !(await sqlExecute("ANALYZE")) {
				return BoolResults.failure(.damagedFile)
			}

			if !fileExists {
				await makeDB()
			}

			await checkSchema()
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

	private func makeDB() async {
		await sqlExecute("create table __settings(key text, value text)")
		await sqlExecute("insert into __settings(key,value) values('schema',1)")
		await sqlExecute("create table __tableArrayColumns(tableName text, arrayColumns text)")
	}

	private func checkSchema() async {
		tables.dropAllTables()
		let tableList = await sqlRows("SELECT name FROM sqlite_master WHERE type = 'table'")
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
			let unsyncedTableResults = await sqlRows("select tableName from __unsyncedTables")
			if let unsyncedTableResults = unsyncedTableResults {
				self.unsyncedTables = unsyncedTableResults.map({ DBTable(name: $0.values[0] as! String) })
			}
		}

		if let keyResults = await sqlRows("select value from __settings where key = 'dbInstanceKey'") {
			if keyResults.isEmpty {
				dbInstanceKey = UUID().uuidString
				let parts = dbInstanceKey.components(separatedBy: "-")
				dbInstanceKey = parts[parts.count - 1]
				await sqlExecute("insert into __settings(key,value) values('dbInstanceKey','\(dbInstanceKey)')")
			} else {
				dbInstanceKey = keyResults[0].values[0] as! String
			}
		}

		if let schemaResults = await sqlRows("select value from __settings where key = 'schema'") {
			var schemaVersion = Int((schemaResults[0].values[0] as! String))!
			if schemaVersion == 1 {
				await sqlExecute("update __settings set value = 2 where key = 'schema'")
				schemaVersion = 2
			}
		}
	}
}

// MARK: - Internal Publisher Updates
extension AgileDB {
	func removePublisher(_ publisher: UpdatablePublisher) {
		publishers = publishers.filter({ $0.id != publisher.id })
	}

	func removePublisherWithID(_ id: UUID) {
		publishers = publishers.filter({ $0.id != id })
	}

	private func updatePublisherResults(for key: String, in table: DBTable) async {
		for publisher in await publishersContaining(key: key, in: table) {
			publisher.updateSubject()
		}
	}

	private func clearPublisherResults(in table: DBTable) {
		for publisher in publishers {
			publisher.clearResults(in: table)
		}
	}

	private func publishersContaining(key: String, in table: DBTable) async -> [UpdatablePublisher] {
		var matchingPublishers = [UpdatablePublisher]()

		for publisher in publishers where publisher.table == table {
			guard let sql = await keysInTableSQL(table: table, sortOrder: nil, conditions: publisher.conditions, validateObjecs: publisher.validateObjects, testKey: key) else { continue }

			guard let results = await sqlRows(sql) else { continue }

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
	func keysInTableSQL(table: DBTable, sortOrder: String?, conditions: [DBCondition]?, validateObjecs: Bool, testKey: String? = nil) async -> String? {
		var arrayColumns = [String]()
		if let results = await sqlRows("select arrayColumns from __tableArrayColumns where tableName = '\(table)'") {
			if results.isNotEmpty {
				arrayColumns = (results[0].values[0] as! String).split { $0 == "," }.map { String($0) }
			}
		} else {
			return nil
		}

		let tableColumns = (await columnsInTable(table)).map({ $0.name }) + ["key"]
		var selectClause = "select distinct a.key from \(table) a"

		var whereClause = ""

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
			// Test whether the specific key satisfies the publisher's conditions; the key
			// clause must be ANDed with the conditions, not replace them, otherwise every
			// publisher for the table would match regardless of its conditions.
			let keyClause = "a.key = '\(esc(testKey))'"
			if whereClause.isNotEmpty {
				whereClause = " where \(keyClause) AND (\n\(whereClause)\n)"
			} else {
				whereClause = " where \(keyClause)"
			}
		} else if (conditions ?? []).isNotEmpty && whereClause.isNotEmpty {
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
				for (index, value) in valueArray.enumerated() {
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

	private func setValue(table: DBTable, key: String, objectValues: [String: AnyObject], addedDateTime: String, updatedDateTime: String, deleteDateTime: String, sourceDB: String, originalDB: String) async -> Bool {
		let openResults = await openDB()
		if case .failure = openResults {
			return false
		}

		if !(await createTable(table)) {
			return false
		}

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
		guard let results = await sqlRows(sql) else { return false }

		if results.isEmpty {
			sql = "insert into \(table) (key,addedDateTime,updatedDateTime,autoDeleteDateTime,hasArrayValues"
			var placeHolders = "'\(key)','\(addedDateTime)','\(updatedDateTime)',\(deleteDateTime),'\(joinedArrayKeys)'"

			for (objectKey, objectValue) in objectValues {
				if objectKey == "key" { continue }

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
				if objectKey == "key" { continue }

				let valueType = SQLiteCore.typeOfValue(objectValue)
				if [.int, .double, .text, .bool].contains(valueType) {
					sql += ",\(objectKey)=?"
				}
			}
			let objectKeys = objectValues.keys
			let columns = await columnsInTable(table)
			for column in columns {
				let filteredKeys = objectKeys.filter({ $0 == column.name })
				if filteredKeys.isEmpty {
					sql += ",\(column.name)=NULL"
				}
			}
			sql += " where key = '\(key)'"
		}

		if !(await setTableValues(objectValues: objectValues, sql: sql)) {
			await validateTableColumns(table: table, objectValues: objectValues as [String: AnyObject])
			if !(await setTableValues(objectValues: objectValues, sql: sql)) {
				return false
			}
		}

		for index in 0 ..< arrayKeys.count {
			if !(await setArrayValues(table: table, arrayValues: arrayValues[index] as! [AnyObject], valueType: arrayTypes[index], key: key, objectKey: arrayKeys[index])) {
				return false
			}
		}

		if syncingEnabled && unsyncedTables.doesNotContain(table) {
			let now = AgileDB.stringValueForDate(Date())
			sql = "insert into __synclog(timestamp, sourceDB, originalDB, tableName, activity, key) values('\(now)','\(sourceDB)','\(originalDB)','\(table)','U','\(esc(key))')"

			if await sqlExecute(sql) {
				let lastID = await lastInsertID()

				if tableHasKey {
					sql = "delete from __synclog where tableName = '\(table)' and key = '\(self.esc(key))' and rowid < \(lastID)"
					await sqlExecute(sql)
				}
			}
		}

		return true
	}

	private func setTableValues(objectValues: [String: AnyObject], sql: String) async -> Bool {
		await withCheckedContinuation { continuation in
			dbCore.setTableValues(objectValues: objectValues, sql: sql) { success in
				continuation.resume(returning: success)
			}
		}
	}

	private func setArrayValues(table: DBTable, arrayValues: [AnyObject], valueType: ValueType, key: String, objectKey: String) async -> Bool {
		var successful = await sqlExecute("delete from \(table)_arrayValues where key='\(key)' and objectKey='\(objectKey)'")
		if !successful { return false }

		for value in arrayValues {
			switch valueType {
			case .textArray:
				successful = await sqlExecute("insert into \(table)_arrayValues(key,objectKey,stringValue) values('\(key)','\(objectKey)','\(esc(value as! String))')")
			case .intArray:
				successful = await sqlExecute("insert into \(table)_arrayValues(key,objectKey,intValue) values('\(key)','\(objectKey)',\(value as! Int))")
			case .doubleArray:
				successful = await sqlExecute("insert into \(table)_arrayValues(key,objectKey,doubleValue) values('\(key)','\(objectKey)',\(value as! Double))")
			default:
				successful = true
			}

			if !successful { return false }
		}

		return true
	}

	private func deleteForKey(table: DBTable, key: String, autoDelete: Bool, sourceDB: String, originalDB: String) async -> Bool {
		let openResults = await openDB()
		if case .failure = openResults { return false }

		if !tables.hasTable(table) { return false }

		let del1 = await sqlExecute("delete from \(table) where key = '\(esc(key))'")
		let del2 = await sqlExecute("delete from \(table)_arrayValues where key = '\(esc(key))'")
		if !del1 || !del2 { return false }

		let now = AgileDB.stringValueForDate(Date())
		if syncingEnabled && unsyncedTables.doesNotContain(table) {
			var sql = ""
			if !autoDelete {
				sql = "insert into __synclog(timestamp, sourceDB, originalDB, tableName, activity, key) values('\(now)','\(sourceDB)','\(originalDB)','\(table)','D','\(esc(key))')"
				_ = await sqlExecute(sql)

				let lastID = await lastInsertID()
				sql = "delete from __synclog where tableName = '\(table)' and key = '\(esc(key))' and rowid < \(lastID)"
				_ = await sqlExecute(sql)
			} else {
				sql = "delete from __synclog where tableName = '\(table)' and key = '\(esc(key))'"
				_ = await sqlExecute(sql)
			}
		}

		return true
	}

	private func autoDelete() async {
		let openResults = await openDB()
		if case .failure = openResults { return }

		let now = AgileDB.stringValueForDate(Date())
		let dbTables = tables.allTables()
		for table in dbTables {
			if !AgileDB.reservedTable(table.name) {
				let sql = "select key from \(table) where autoDeleteDateTime < '\(now)'"
				if let results = await sqlRows(sql) {
					for row in results {
						let key = row.values[0] as! String
						_ = await deleteForKey(table: table, key: key, autoDelete: true, sourceDB: dbInstanceKey, originalDB: dbInstanceKey)
					}
				}
			}
		}
	}

	private func dictValueFromTable(_ table: DBTable, for key: String, includeDates: Bool) async -> [String: AnyObject]? {
		assert(key != "", "key value must be provided")
		let openResults = await openDB()
		if case .failure = openResults { return nil }
		if !tables.hasTable(table) { return nil }

		var columns = await columnsInTable(table)
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

		let results = await sqlRows(sql)

		return await dictValueResults(table: table, key: key, results: results, columns: columns)
	}

	private func dictValueResults(table: DBTable, key: String, results: [DBRow]?, columns: [TableColumn]) async -> [String: AnyObject]? {
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

		let arrayObjects = (results[0].values[0] as! String).split { $0 == "," }.map { String($0) }
		for object in arrayObjects {
			if object == "" { continue }

			let keyType = object.split { $0 == ":" }.map { String($0) }
			let objectKey = keyType[0]
			let valueType = ValueType(rawValue: keyType[1] as String)!
			var stringArray = [String]()
			var intArray = [Int]()
			var doubleArray = [Double]()

			var arrayQueryResults: [DBRow]?
			switch valueType {
			case .textArray:
				arrayQueryResults = await sqlRows("select stringValue from \(table)_arrayValues where key = '\(key)' and objectKey = '\(objectKey)'")
			case .intArray:
				arrayQueryResults = await sqlRows("select intValue from \(table)_arrayValues where key = '\(key)' and objectKey = '\(objectKey)'")
			case .doubleArray:
				arrayQueryResults = await sqlRows("select doubleValue from \(table)_arrayValues where key = '\(key)' and objectKey = '\(objectKey)'")
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

	private func createTable(_ table: DBTable) async -> Bool {
		if tables.hasTable(table) { return true }

		let ct1 = await sqlExecute("create table \(table) (key text PRIMARY KEY, autoDeleteDateTime text, addedDateTime text, updatedDateTime text, hasArrayValues text)")
		let ct2 = await sqlExecute("create index idx_\(table)_autoDeleteDateTime on \(table)(autoDeleteDateTime)")
		if !ct1 || !ct2 { return false }

		let ct3 = await sqlExecute("create table \(table)_arrayValues (key text, objectKey text, stringValue text, intValue int, doubleValue double)")
		let ct4 = await sqlExecute("create index idx_\(table)_arrayValues_keys on \(table)_arrayValues(key,objectKey)")
		if !ct3 || !ct4 { return false }

		tables.addTable(table)

		return true
	}

	private func createIndexesForTable(_ table: DBTable) async {
		if !tables.hasTable(table) { return }

		if let tableIndexes = indexes[table.name] {
			for index in tableIndexes {
				var indexName = index.replacingOccurrences(of: ",", with: "_")
				indexName = "idx_\(table)_\(indexName)"

				var sql = "select * from sqlite_master where tbl_name = '\(table)' and name = '\(indexName)'"
				if let results = await sqlRows(sql), results.isEmpty {
					sql = "CREATE INDEX \(indexName) on \(table)(\(index))"
					_ = await sqlExecute(sql)
				}
			}
		}
	}

	private func columnsInTable(_ table: DBTable) async -> [TableColumn] {
		guard let tableInfo = await sqlRows("pragma table_info(\(table))") else { return [] }
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

	private func validateTableColumns(table: DBTable, objectValues: [String: AnyObject]) async {
		let columns = await columnsInTable(table)
		for (objectKey, value) in objectValues {
			if objectKey == "key" { continue }

			assert(!reservedColumn(objectKey as String), "Reserved column")
			assert((objectKey as String).range(of: "'") == nil, "Single quote not allowed in column names")

			let found = columns.filter({ $0.name == objectKey }).isNotEmpty

			if !found {
				let valueType = SQLiteCore.typeOfValue(value)
				assert(valueType != .unknown, "column types are int, double, string, bool or arrays of int, double, or string")

				if valueType == .null { continue }

				if [.int, .double, .text].contains(valueType) {
					let sql = "alter table \(table) add column \(objectKey) \(valueType.rawValue)"
					_ = await sqlExecute(sql)
				} else if valueType == .bool {
					let sql = "alter table \(table) add column \(objectKey) int"
					_ = await sqlExecute(sql)
				} else {
					let sql = "select arrayColumns from __tableArrayColumns where tableName = '\(table)'"
					if let results = await sqlRows(sql) {
						var arrayColumns = ""
						if results.isNotEmpty {
							arrayColumns = results[0].values[0] as! String
							arrayColumns += ",\(objectKey)"
							_ = await sqlExecute("delete from __tableArrayColumns where tableName = '\(table)'")
						} else {
							arrayColumns = objectKey as String
						}
						_ = await sqlExecute("insert into __tableArrayColumns(tableName,arrayColumns) values('\(table)','\(arrayColumns)')")
					}
				}
			}
		}

		await createIndexesForTable(table)
	}

	// MARK: - SQLite execute/query
	@discardableResult
	private func sqlExecute(_ sql: String) async -> Bool {
		await withCheckedContinuation { continuation in
			_ = dbCore.sqlExecute(sql) { success in
				continuation.resume(returning: success)
			}
		}
	}

	private func lastInsertID() async -> sqlite3_int64 {
		await withCheckedContinuation { continuation in
			dbCore.lastID { id in
				continuation.resume(returning: id)
			}
		}
	}

	private func sqlRows(_ sql: String) async -> [DBRow]? {
		let result: RowResults = await withCheckedContinuation { continuation in
			_ = dbCore.sqlSelect(sql) { continuation.resume(returning: $0) }
		}
		switch result {
		case .success(let rows): return rows
		case .failure: return nil
		}
	}

	/**
	 Asynchronously runs a SQL select command.

	 - parameter sql: The `select` SQL command to run.

	  - returns: [DBRow]
	  - throws: DBError
	  */
	public func sqlSelect(_ sql: String) async throws -> [DBRow] {
		let openResults = await openDB()
		if case .failure(let error) = openResults { throw error }

		let result: RowResults = await withCheckedContinuation { continuation in
			_ = dbCore.sqlSelect(sql) { continuation.resume(returning: $0) }
		}

		switch result {
		case .success(let rows): return rows
		case .failure(let error): throw error
		}
	}

	/**
	 Runs a SQL select and returns results via a completion closure.

	 - parameter sql: The `select` SQL command to run.
	 - parameter queue: Optional dispatch queue for the completion closure. Default is main queue.
	 - parameter completion: Closure receiving the result.

	  - returns: DBCommandToken that can be used to cancel the command before it executes.
	  */
	@discardableResult
	public func sqlSelect(_ sql: String, queue: DispatchQueue? = nil, completion: @escaping (RowResults) -> Void) -> DBCommandToken? {
		guard dbCore.isOpen else { return nil }

		let blockReference: UInt = dbCore.sqlSelect(sql, completion: { rowResults in
			let dispatchQueue = queue ?? DispatchQueue.main
			dispatchQueue.async { completion(rowResults) }
		})

		return DBCommandToken(database: self, identifier: blockReference)
	}

	/// Cancels a queued command by its block reference. `nonisolated` so `DBCommandToken.cancel()` stays synchronous.
	nonisolated func dequeueCommand(_ commandReference: UInt) -> Bool {
		let semaphore = DispatchSemaphore(value: 0)
		var removed = false
		dbCore.removeExecutionBlock(commandReference) { result in
			removed = result
			semaphore.signal()
		}
		semaphore.wait()
		return removed
	}
}

// MARK: - SQLiteCore
private extension AgileDB {
	final class SQLiteCore: Thread, @unchecked Sendable {
		var isOpen = false
		var isDebugging = false

		override init() {
			super.init()
			self.name = "AgileDB"
		}

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
					var index: Int32 = 1

					for (objectKey, objectValue) in objectValues {
						if objectKey == "key" { continue }

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
