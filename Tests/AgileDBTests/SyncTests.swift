//
//  AgileDBSyncTests.swift
//  AgileDB
//
//  Created by Aaron Bratcher on 1/15/15.
//  Copyright (c) 2015 Aaron Bratcher. All rights reserved.
//

import Foundation
import Testing
@testable import AgileDB

func dbForTesting(isDebugging: Bool = false) -> AgileDB {
	let pathURL = URL(fileURLWithPath: pathForDB)
	print(pathURL)
	let db = AgileDB(fileLocation: pathURL, isDebugging: isDebugging)

	return db
}

var pathForDB: String {
	let fileName = UUID().uuidString
	let searchPaths = NSSearchPathForDirectoriesInDomains(.downloadsDirectory, .userDomainMask, true)
	let documentFolderPath = searchPaths[0]
	let dbFilePath = documentFolderPath + "/\(fileName).db"
	return dbFilePath
}

func removeDB(_ db: AgileDB) async {
	guard let path = await db.fileLocation else { return }

	let fileExists = FileManager.default.fileExists(atPath: path.path)
	if fileExists {
		try? FileManager.default.removeItem(atPath: path.path)
	}
}

@Suite("Sync Tests")
struct SyncTests {
	@Test("Enable syncing")
	func testEnableSyncing() async throws {
		let db = dbForTesting()
		#expect(await db.enableSyncing(), "Could not enable syncing")
		await removeDB(db)
	}

	@Test("Disable syncing")
	func testDisableSyncing() async throws {
		let db = dbForTesting()
		guard await db.enableSyncing() else { return }
		#expect(await db.disableSyncing(), "Could not disable syncing")
		await removeDB(db)
	}

	@Test("Create sync file")
	func testCreateSyncFile() async throws {
		let db = dbForTesting()

		_ = await db.disableSyncing()
		await db.dropAllTables()
		_ = await db.enableSyncing()

		await db.setValueInTable(DBTable(name: "table8"), for: "testKey1", to: "{\"numValue\":1,\"account\":\"ACCT1\",\"dateValue\":\"2014-8-19T18:23:42.434-05:00\",\"arrayValue\":[1,2,3,4,5]}", autoDeleteAfter: nil)
		await db.deleteFromTable(DBTable(name: "table8"), for: "testKey1")

		await db.setValueInTable(DBTable(name: "table9"), for: "testKey2", to: "{\"numValue\":2,\"account\":\"TEST1\",\"dateValue\":\"2014-9-19T18:23:42.434-05:00\",\"arrayValue\":[6,7,8,9,10]}", autoDeleteAfter: nil)

		await db.setValueInTable(DBTable(name: "table9"), for: "testKey1", to: "{\"numValue\":1,\"account\":\"ACCT1\",\"dateValue\":\"2014-8-19T18:23:42.434-05:00\"}", autoDeleteAfter: nil)

		await db.setValueInTable(DBTable(name: "table10"), for: "testKey3", to: "{\"numValue\":3,\"account\":\"TEST2\",\"dateValue\":\"2014-10-19T18:23:42.434-05:00\",\"arrayValue\":[11,12,13,14,15]}", autoDeleteAfter: nil)
		await db.setValueInTable(DBTable(name: "table10"), for: "testKey3", to: "{\"numValue\":3,\"dateValue\":\"2014-10-19T18:23:42.434-05:00\",\"arrayValue\":[11,12]}", autoDeleteAfter: nil)

		await db.setValueInTable(DBTable(name: "table11"), for: "testKey4", to: "{\"numValue\":4,\"account\":\"TEST3\",\"dateValue\":\"2014-11-19T18:23:42.434-05:00\",\"arrayValue\":[16,17,18,19,20]}", autoDeleteAfter: nil)
		await db.deleteFromTable(DBTable(name: "table11"), for: "testKey4")

		await db.setValueInTable(DBTable(name: "table12"), for: "testKey5", to: "{\"numValue\":5,\"account\":\"ACCT3\",\"dateValue\":\"2014-12-19T18:23:42.434-05:00\",\"arrayValue\":[21,22,23,24,25]}", autoDeleteAfter: nil)
		await db.dropTable("table9")

		let searchPaths = NSSearchPathForDirectoriesInDomains(.downloadsDirectory, .userDomainMask, true)
		let documentFolderPath = searchPaths[0]
		let logFilePath = documentFolderPath + "/testSyncLog.txt"
		print(logFilePath)
		let fileURL = URL(fileURLWithPath: logFilePath)

		let (complete, lastSequence) = await db.createSyncFileAtURL(fileURL, lastSequence: 0, targetDBInstanceKey: "TEST-DB-INSTANCE")

		#expect(complete, "sync file not completed")
		#expect(lastSequence == 10, "lastSequence is incorrect")

		// read in file and make sure it is valid JSON
		if let fileHandle = FileHandle(forReadingAtPath: logFilePath) {
			let dataValue = fileHandle.readDataToEndOfFile()
			if let _ = (try? JSONSerialization.jsonObject(with: dataValue, options: .mutableContainers)) as? [String: any Sendable] {
				// conversion successful
			} else {
				Issue.record("invalid sync file format")
			}
		} else {
			Issue.record("cannot open file")
		}

		await removeDB(db)
	}

	@Test("Process sync file")
	func testProcessSyncFile() async throws {
		let db = dbForTesting()

		#expect(await db.disableSyncing())
		#expect(await db.dropAllTables())
		#expect(await db.enableSyncing())

		let table11: DBTable = "table11"
		await db.setUnsyncedTables([table11])
		await db.close()
		await db.open()
		let unsyncedTables = await db.unsyncedTables
		#expect(unsyncedTables.count == 1)

		// will be deleted
		await db.setValueInTable(DBTable(name: "table8"), for: "testKey1", to: "{\"numValue\":10,\"account\":\"ACCT1\",\"dateValue\":\"2014-8-19T18:23:42.434-05:00\",\"arrayValue\":[1,2,3,4,5]}", autoDeleteAfter: nil)

		// these entries will be deleted because of a drop table
		await db.setValueInTable(DBTable(name: "table9"), for: "testKey2", to: "{\"numValue\":2,\"account\":\"TEST1\",\"dateValue\":\"2014-9-19T18:23:42.434-05:00\",\"arrayValue\":[6,7,8,9,10]}", autoDeleteAfter: nil)
		await db.setValueInTable(DBTable(name: "table9"), for: "testKey3", to: "{\"numValue\":2,\"account\":\"TEST1\",\"dateValue\":\"2014-9-19T18:23:42.434-05:00\",\"arrayValue\":[6,7,8,9,10]}", autoDeleteAfter: nil)
		await db.setValueInTable(DBTable(name: "table9"), for: "testKey4", to: "{\"numValue\":2,\"account\":\"TEST1\",\"dateValue\":\"2014-9-19T18:23:42.434-05:00\",\"arrayValue\":[6,7,8,9,10]}", autoDeleteAfter: nil)
		await db.setValueInTable(DBTable(name: "table9"), for: "testKey5", to: "{\"numValue\":2,\"account\":\"TEST1\",\"dateValue\":\"2014-9-19T18:23:42.434-05:00\",\"arrayValue\":[6,7,8,9,10]}", autoDeleteAfter: nil)

		// this value will be unchanged due to timeStamp
		await db.setValueInTable(DBTable(name: "table10"), for: "testKey3", to: "{\"numValue\":13,\"account\":\"TEST2\",\"dateValue\":\"2014-10-19T18:23:42.434-05:00\",\"arrayValue\":[11,12,13,14,15]}", autoDeleteAfter: nil)

		// this value will be updated
		await db.setValueInTable(DBTable(name: "table12"), for: "testKey5", to: "{\"numValue\":15,\"account\":\"ACCT3\",\"dateValue\":\"2014-12-19T18:23:42.434-05:00\",\"arrayValue\":[21,22,23,24,25]}", autoDeleteAfter: nil)

		let syncFileContents = """
  {
  "sourceDB": "58D200A048F9",
  "lastSequence": 1000,
  "logEntries": [
   {
  "timeStamp": "\(AgileDB.stringValueForDate(Date()))",
  "key": "testKey1",
  "activity": "D",
  "tableName": "table8"
   },
   {
   "timeStamp": "2010-01-15T16:22:55.262-05:00",
   "value": {
  "addedDateTime": "2015-01-15T16:22:55.246-05:00",
  "dateValue": "2014-10-19T18:23:42.434-05:00",
  "numValue": 3,
  "updatedDateTime": "2015-01-15T16:22:55.258-05:00",
  "arrayValue": [11,12]
   },
   "key": "testKey3",
   "activity": "U",
   "tableName": "table10"
   },
   {
  "timeStamp": "2015-01-15T16:22:55.276-05:00",
  "key": "testKey4",
  "activity": "D",
  "tableName": "table11"
   },
   {
   "timeStamp": "\(AgileDB.stringValueForDate(Date()))",
   "value": {
  "addedDateTime": "2015-01-15T16:22:55.277-05:00",
  "account": "ACCT3",
  "dateValue": "2014-12-19T18:23:42.434-05:00",
  "numValue": 5,
  "updatedDateTime": "2015-01-15T16:22:55.277-05:00",
  "arrayValue": [21,22,23,24,25]
   },
   "key": "testKey5",
   "activity": "U",
   "tableName": "table12"
   },
   {
  "tableName": "table9",
  "activity": "X",
  "timeStamp": "\(AgileDB.stringValueForDate(Date()))"
   }
  ]
  }
  """

		let searchPaths = NSSearchPathForDirectoriesInDomains(.downloadsDirectory, .userDomainMask, true)
		let documentFolderPath = searchPaths[0]
		let logFilePath = documentFolderPath + "/testSyncLog2.txt"

		FileManager.default.createFile(atPath: logFilePath, contents: nil, attributes: nil)
		if let fileHandle = FileHandle(forWritingAtPath: logFilePath) {
			fileHandle.write(syncFileContents.dataValue() as Data)
			fileHandle.closeFile()
			let fileURL = URL(fileURLWithPath: logFilePath)

			let (results, _, _) = await db.processSyncFileAtURL(fileURL, syncProgress: nil)
			#expect(results, "sync log not processed")

			// check for proper changes
			let table8HasKey = try await db.tableHasKey(table: DBTable(name: "table8"), key: "testKey1")
			#expect(!table8HasKey, "table8 still has entry")

			let table9HasKey2 = try await db.tableHasKey(table: DBTable(name: "table9"), key: "testKey2")
			#expect(!table9HasKey2, "drop table 9 failed")

			let table9HasKey3 = try await db.tableHasKey(table: DBTable(name: "table9"), key: "testKey3")
			#expect(!table9HasKey3, "drop table 9 failed")

			let table9HasKey4 = try await db.tableHasKey(table: DBTable(name: "table9"), key: "testKey4")
			#expect(!table9HasKey4, "drop table 9 failed")

			let table9HasKey5 = try await db.tableHasKey(table: DBTable(name: "table9"), key: "testKey5")
			#expect(!table9HasKey5, "drop table 9 failed")

			var jsonValue = try await db.valueFromTable(DBTable(name: "table10"), for: "testKey3")
			// compare dict values
			let dataValue = jsonValue.data(using: String.Encoding.utf8, allowLossyConversion: false)!
			let objectValues = (try? JSONSerialization.jsonObject(with: dataValue, options: .mutableContainers)) as? [String: any Sendable]
			let numValue = objectValues!["numValue"] as! Int

			#expect(numValue == 13, "number unexpectedly got changed")

			jsonValue = try await db.valueFromTable(DBTable(name: "table12"), for: "testKey5")
			// compare dict values
			let dataValue2 = jsonValue.data(using: String.Encoding.utf8, allowLossyConversion: false)!
			let objectValues2 = (try? JSONSerialization.jsonObject(with: dataValue2, options: .mutableContainers)) as? [String: any Sendable]
			let numValue2 = objectValues2!["numValue"] as! Int

			#expect(numValue2 == 5, "number was not changed")
		} else {
			Issue.record("unable to create log file")
		}

		await removeDB(db)
	}
}

extension String {
	func dataValue() -> Data {
		return data(using: .utf8, allowLossyConversion: false)!
	}
}
