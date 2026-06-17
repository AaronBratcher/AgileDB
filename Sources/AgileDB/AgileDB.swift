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

	public func setAutoCloseTimeout(_ timeout: Int) {
		self.autoCloseTimeout = timeout
	}
	
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
	public func tableHasKey(table: DBTable, key: String, queue: DispatchQueue? = nil, completion: @escaping @Sendable (BoolResults) -> Void) -> DBCommandToken? {
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
	public func tableHasAllKeys(table: DBTable, keys: [String], queue: DispatchQueue? = nil, completion: @escaping @Sendable (BoolResults) -> Void) -> DBCommandToken? {
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
	public func keysInTable(_ table: DBTable, sortOrder: String? = nil, conditions: [DBCondition]? = nil, validateObjects: Bool = false, queue: DispatchQueue? = nil, completion: @escaping @Sendable (KeyResults) -> Void) -> DBCommandToken? {
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

		let objectValues = (try? JSONSerialization.jsonObject(with: dataValue, options: .mutableContainers)) as? [String: any Sendable]
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
	public func setValueInTable(_ table: DBTable, for key: String, to objectValues: [String: any Sendable], autoDeleteAfter: Date? = nil) async -> Bool {
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

	  - returns: [String: any Sendable]
	  - throws: DBError
	  */
	public func dictValueFromTable(_ table: DBTable, for key: String) async throws -> [String: any Sendable] {
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
		if !d1 {
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

				var entryDict = [String: any Sendable]()
				entryDict["timeStamp"] = timeStamp as any Sendable
				if originalDB != dbInstanceKey {
					entryDict["originalDB"] = originalDB as any Sendable
				}
				entryDict["tableName"] = tableName as any Sendable
				entryDict["activity"] = activity as any Sendable
				if let key = key {
					entryDict["key"] = key as any Sendable
					if activity == "U" {
						guard let dictValue = await dictValueFromTable(DBTable(name: tableName), for: key, includeDates: true) else { continue }
						entryDict["value"] = dictValue as any Sendable
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

				if let objectValues = (try? JSONSerialization.jsonObject(with: dataValue, options: .mutableContainers)) as? [String: any Sendable] {
					let sourceDB = objectValues["sourceDB"] as! String
					let logEntries = objectValues["logEntries"] as! [[String: any Sendable]]
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
										var objectValues = entry["value"] as! [String: any Sendable]
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
		await sqlExecute("insert into __settings(key,value) values('schema',3)")
	}

	private func checkSchema() async {
		tables.dropAllTables()
		let tableList = await sqlRows("SELECT name FROM sqlite_master WHERE type = 'table'")
		if let tableList = tableList {
			for tableRow in tableList {
				let table = tableRow.values[0] as! String
				if !AgileDB.reservedTable(table) {
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
		// All object properties live in the `value` JSON document, so conditions are
		// expressed with json_extract / json_each rather than physical columns. There is
		// no schema to validate against, so `validateObjecs` is intentionally ignored.
		let selectClause = "select distinct a.key from \(table) a"

		var whereClause = ""

		if let conditionSet = conditions, conditionSet.isNotEmpty {
			let pages = conditionSetPages(from: conditionSet)
			var pageClauses: [String] = []

			for page in pages {
				pageClauses.append(pageClause(table: table, conditions: conditionSet.filter({ $0.set == page })))
			}

			for (index, pageClause) in pageClauses.enumerated() {
				if index > 0 {
					whereClause += "\nOR \(pageClause)"
				} else {
					whereClause += pageClause
				}
			}
		}

		if let testKey {
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
			whereClause += " order by \(jsonSortClause(sortOrder))"
		}

		let sql = selectClause + whereClause
		if printSQL {
			print("^^^ SQL: \(NSString(string: sql))")
		}
		return sql
	}

	/// Translates a comma-delimited list of property names (each optionally followed by
	/// `asc`/`desc`) into json_extract expressions over the `value` document.
	private func jsonSortClause(_ sortOrder: String) -> String {
		var terms: [String] = []
		for part in sortOrder.split(separator: ",") {
			let tokens = part.split(separator: " ").map(String.init).filter({ $0.isNotEmpty })
			guard let field = tokens.first else { continue }
			var term = fieldExpression(for: field)
			if tokens.count > 1 {
				let direction = tokens[1].lowercased()
				if direction == "asc" || direction == "desc" {
					term += " \(direction)"
				}
			}
			terms.append(term)
		}

		return terms.joined(separator: ", ")
	}

	/// `key` and the date fields are stored as real columns; every other property lives in
	/// the `value` JSON document and is reached with json_extract.
	private func fieldExpression(for objectKey: String) -> String {
		let reservedColumns: Set<String> = ["key", "addedDateTime", "updatedDateTime", "autoDeleteDateTime"]
		if reservedColumns.contains(objectKey) {
			return objectKey
		}

		return "json_extract(value, '$.\(objectKey)')"
	}

	private func conditionSetPages(from conditions: [DBCondition]) -> Set<Int> {
		var pages: Set<Int> = []
		for condition in conditions {
			pages.insert(condition.set)
		}

		return pages
	}

	private func pageClause(table: DBTable, conditions: [DBCondition]) -> String {
		var whereClause = "a.key in (select key from \(table.name) where "
		for (index, condition) in conditions.enumerated() {
			let conditionClause = conditionClause(from: condition, table: table)
			if index > 0 {
				whereClause += " AND \(conditionClause)"
			} else {
				whereClause += conditionClause
			}
		}
		whereClause += ")"

		return whereClause
	}

	private func conditionClause(from condition: DBCondition, table: DBTable) -> String {
		let path = "'$.\(condition.objectKey)'"
		let extract = fieldExpression(for: condition.objectKey)
		let isReserved = extract == condition.objectKey
		var whereClause = ""

		switch condition.conditionOperator {
		case .contains:
			// `contains` is overloaded: membership for JSON arrays, substring for JSON text.
			// Resolve which at query time via json_type so no array metadata is needed.
			// Array membership re-joins the document by key with json_each — a correlated
			// json_each(value, …) inside a subquery is not evaluated by SQLite.
			if let stringValue = condition.value as? String {
				let substring = "\(extract) like '%\(esc(stringValue))%'"
				if isReserved {
					whereClause += substring
				} else {
					let membership = "key in (select jt.key from \(table.name) jt, json_each(jt.value, \(path)) je where je.value = '\(esc(stringValue))')"
					whereClause += "((json_type(value, \(path)) = 'array' AND \(membership)) OR (json_type(value, \(path)) <> 'array' AND \(substring)))"
				}
			} else if !isReserved {
				whereClause += "key in (select jt.key from \(table.name) jt, json_each(jt.value, \(path)) je where je.value = \(condition.value))"
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
				whereClause += " \(extract) in (\(listItems))"
			}

		default:
			if let conditionValue = condition.value as? String {
				whereClause += " \(extract) \(condition.conditionOperator.rawValue) '\(esc(conditionValue))'"
			} else if let conditionValue = condition.value as? Date {
				whereClause += " \(extract) \(condition.conditionOperator.rawValue) '\(AgileDB.stringValueForDate(conditionValue))'"
			} else if let conditionValue = condition.value as? Bool {
				// json_extract returns 1/0 for JSON booleans.
				let boolValue = conditionValue ? 1 : 0
				whereClause += " \(extract) \(condition.conditionOperator.rawValue) \(boolValue)"
			} else {
				whereClause += " \(extract) \(condition.conditionOperator.rawValue) \(condition.value)"
			}
		}

		return whereClause
	}

	private func setValue(table: DBTable, key: String, objectValues: [String: any Sendable], addedDateTime: String, updatedDateTime: String, deleteDateTime: String, sourceDB: String, originalDB: String) async -> Bool {
		let openResults = await openDB()
		if case .failure = openResults {
			return false
		}

		if !(await createTable(table)) {
			return false
		}

		// The whole object is stored as a single JSON document in the `value` column.
		// Strip the key and the reserved date fields (those live in dedicated columns).
		var documentValues = objectValues
		documentValues.removeValue(forKey: "key")
		documentValues.removeValue(forKey: "addedDateTime")
		documentValues.removeValue(forKey: "updatedDateTime")
		documentValues.removeValue(forKey: "autoDeleteDateTime")

		guard let jsonString = jsonString(from: documentValues) else { return false }

		// Determine prior existence so the sync log can prune superseded entries, and so
		// the original addedDateTime is preserved on update.
		guard let existing = await sqlRows("select 1 from \(table) where key = '\(esc(key))'") else { return false }
		let tableHasKey = existing.isNotEmpty

		// A single-row UPSERT makes the write atomic without a sidecar to keep in sync.
		let sql = "insert into \(table) (key,addedDateTime,updatedDateTime,autoDeleteDateTime,value)"
			+ " values('\(esc(key))','\(addedDateTime)','\(updatedDateTime)',\(deleteDateTime),?)"
			+ " on conflict(key) do update set updatedDateTime='\(updatedDateTime)',autoDeleteDateTime=\(deleteDateTime),value=excluded.value"

		if !(await sqlExecute(sql, parameters: [jsonString])) {
			return false
		}

		if syncingEnabled && unsyncedTables.doesNotContain(table) {
			let now = AgileDB.stringValueForDate(Date())
			let logSQL = "insert into __synclog(timestamp, sourceDB, originalDB, tableName, activity, key) values('\(now)','\(sourceDB)','\(originalDB)','\(table)','U','\(esc(key))')"

			if await sqlExecute(logSQL) {
				let lastID = await lastInsertID()

				if tableHasKey {
					await sqlExecute("delete from __synclog where tableName = '\(table)' and key = '\(self.esc(key))' and rowid < \(lastID)")
				}
			}
		}

		return true
	}

	/// Serializes an object-value dictionary to a JSON string for storage in the `value` column.
	private func jsonString(from dict: [String: any Sendable]) -> String? {
		guard JSONSerialization.isValidJSONObject(dict),
			  let data = try? JSONSerialization.data(withJSONObject: dict),
			  let string = String(data: data, encoding: .utf8)
		else { return nil }

		return string
	}

	private func deleteForKey(table: DBTable, key: String, autoDelete: Bool, sourceDB: String, originalDB: String) async -> Bool {
		let openResults = await openDB()
		if case .failure = openResults { return false }

		if !tables.hasTable(table) { return false }

		let del1 = await sqlExecute("delete from \(table) where key = '\(esc(key))'")
		if !del1 { return false }

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

	private func dictValueFromTable(_ table: DBTable, for key: String, includeDates: Bool) async -> [String: any Sendable]? {
		assert(key != "", "key value must be provided")
		let openResults = await openDB()
		if case .failure = openResults { return nil }
		if !tables.hasTable(table) { return nil }

		var sql = "select value"
		if includeDates {
			sql += ",autoDeleteDateTime,addedDateTime,updatedDateTime"
		}
		sql += " from \(table) where key = '\(esc(key))'"

		guard let results = await sqlRows(sql), results.isNotEmpty else { return nil }
		let row = results[0]

		guard let jsonText = row.values[0] as? String,
			  let data = jsonText.data(using: .utf8),
			  var valueDict = (try? JSONSerialization.jsonObject(with: data)) as? [String: any Sendable]
		else { return nil }

		if includeDates {
			if let value = row.values[1] as? String { valueDict["autoDeleteDateTime"] = value as any Sendable }
			if let value = row.values[2] as? String { valueDict["addedDateTime"] = value as any Sendable }
			if let value = row.values[3] as? String { valueDict["updatedDateTime"] = value as any Sendable }
		}

		return valueDict
	}

	// MARK: - Internal Table methods
	static func reservedTable(_ table: String) -> Bool {
		return table.hasPrefix("__") || table.hasPrefix("sqlite_stat")
	}

	private func createTable(_ table: DBTable) async -> Bool {
		if tables.hasTable(table) { return true }

		let ct1 = await sqlExecute("create table \(table) (key text PRIMARY KEY, autoDeleteDateTime text, addedDateTime text, updatedDateTime text, value text)")
		let ct2 = await sqlExecute("create index idx_\(table)_autoDeleteDateTime on \(table)(autoDeleteDateTime)")
		if !ct1 || !ct2 { return false }

		tables.addTable(table)

		// Apply any indexes that were declared before the table physically existed.
		await createIndexesForTable(table)

		return true
	}

	/// Declares the indexes requested for a table. Each index field is materialized as a
	/// VIRTUAL generated column over `json_extract(value, '$.field')`, then indexed. This
	/// gives both index-backed lookups and real column names usable from direct SQL.
	private func createIndexesForTable(_ table: DBTable) async {
		if !tables.hasTable(table) { return }

		guard let tableIndexes = indexes[table.name] else { return }

		let existingColumns = Set(await columnNames(in: table))

		for index in tableIndexes {
			let fields = index.split(separator: ",").map({ $0.trimmingCharacters(in: .whitespaces) }).filter({ $0.isNotEmpty })
			if fields.isEmpty { continue }

			for field in fields where !existingColumns.contains(field) {
				_ = await sqlExecute("alter table \(table) add column \(field) GENERATED ALWAYS AS (json_extract(value, '$.\(field)')) VIRTUAL")
			}

			let indexName = "idx_\(table)_" + fields.joined(separator: "_")
			let columnsList = fields.joined(separator: ",")
			if let results = await sqlRows("select * from sqlite_master where tbl_name = '\(table)' and name = '\(indexName)'"), results.isEmpty {
				_ = await sqlExecute("CREATE INDEX \(indexName) on \(table)(\(columnsList))")
			}
		}
	}

	private func columnNames(in table: DBTable) async -> [String] {
		// table_xinfo (not table_info) reports VIRTUAL generated columns, so a previously
		// declared index column is detected and not re-added.
		guard let tableInfo = await sqlRows("pragma table_xinfo(\(table))") else { return [] }
		return tableInfo.compactMap({ $0.values[1] as? String })
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

	/// Executes a statement binding the given parameters (in order) to its `?` placeholders.
	@discardableResult
	private func sqlExecute(_ sql: String, parameters: [any Sendable]) async -> Bool {
		await withCheckedContinuation { continuation in
			_ = dbCore.sqlExecute(sql, parameters: parameters) { success in
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
	public func sqlSelect(_ sql: String, queue: DispatchQueue? = nil, completion: @escaping @Sendable (RowResults) -> Void) -> DBCommandToken? {
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
		// The semaphore establishes a happens-before ordering between the completion
		// writing the result and this method reading it, so the box access is safe.
		let removed = SendableBox(false)
		dbCore.removeExecutionBlock(commandReference) { result in
			removed.value = result
			semaphore.signal()
		}
		semaphore.wait()
		return removed.value
	}
}

/// A reference-type holder used to pass a value out of an `@Sendable` completion closure.
/// Marked `@unchecked Sendable` because callers must provide their own synchronization
/// (e.g. a semaphore) to establish a happens-before ordering around its access.
private final class SendableBox<T>: @unchecked Sendable {
	var value: T
	init(_ value: T) { self.value = value }
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
			var block: @Sendable () -> Void
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

		class func typeOfValue(_ value: any Sendable) -> ValueType {
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

		func openDBFile(_ dbFilePath: String, autoCloseTimeout: Int, completion: @escaping @Sendable (_ successful: BoolResults, _ openedFromOtherThread: Bool, _ fileExists: Bool) -> Void) {
			self.autoCloseTimeout = TimeInterval(exactly: autoCloseTimeout) ?? 0.0
			self.dbFilePath = dbFilePath
			if isDebugging {
				print(dbFilePath)
			}

			let block = { @Sendable [unowned self] in
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
			let block = { @Sendable [unowned self] in
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

		func lastID(_ completion: @escaping @Sendable (_ lastInsertionID: sqlite3_int64) -> Void) {
			let block = { @Sendable [unowned self] in
				completion(sqlite3_last_insert_rowid(self.sqliteDB))
			}

			addBlock(block)
		}

		func sqlExecute(_ sql: String, completion: @escaping @Sendable (_ success: Bool) -> Void) -> UInt {
			let block = { @Sendable [unowned self] in
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

		func sqlSelect(_ sql: String, completion: @escaping @Sendable (_ results: RowResults) -> Void) -> UInt {
			let block = { @Sendable [unowned self] in
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
								row.values.append(value as any Sendable)
							case SQLITE_INTEGER:
								row.values.append(Int(sqlite3_column_int64(dbps, index)) as any Sendable)
							case SQLITE_FLOAT:
								row.values.append(Double(sqlite3_column_double(dbps, index)) as any Sendable)
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

		func removeExecutionBlock(_ blockReference: UInt, completion: @escaping @Sendable (_ success: Bool) -> Void) {
			let block = { @Sendable in
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

		func sqlExecute(_ sql: String, parameters: [any Sendable], completion: @escaping @Sendable (_ success: Bool) -> Void) -> UInt {
			let block = { @Sendable [unowned self] in
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

				var index: Int32 = 1
				for parameter in parameters {
					status = self.bindValue(dbps!, index: index, value: parameter)
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

				completion(true)
				return
			}

			return addBlock(block)
		}

		private func bindValue(_ statement: OpaquePointer, index: Int32, value: any Sendable) -> Int32 {
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
		private func addBlock(_ block: @escaping @Sendable () -> Void) -> UInt {
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
						if let executionBlock = queuedBlocks.first {
							queuedBlocks.removeFirst()
							executionBlock.block()
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
