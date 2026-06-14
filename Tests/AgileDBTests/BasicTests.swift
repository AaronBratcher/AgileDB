//
//  AgileDBTests.swift
//  AgileDBTests
//
//  Created by Aaron Bratcher on 1/8/15.
//  Copyright (c) 2015 Aaron Bratcher. All rights reserved.
//

import Foundation
import Testing
@testable import AgileDB

@Suite("Basic Database Tests")
struct BasicTests {
	@Test("Concurrent access does not corrupt DB")
	func testConcurrentAccess() async throws {
		let db = dbForTesting()
		let table: DBTable = "concurrency"
		let keys = (1...10).map { "key\($0)" }

		// Seed every key so that concurrent reads below always find a value.
		// Each key stores a stable value of {"val": key}, so a read is correct
		// whether it observes the seed or a concurrent re-write.
		for key in keys {
			_ = await db.setValueInTable(table, for: key, to: ["val": key as AnyObject])
		}

		// Hammer the DB with interleaved writes and reads in a single task group
		// so reads can land in the middle of writes. Each read returns the key it
		// read along with the JSON it got back.
		let readResults = await withTaskGroup(of: (key: String, json: String)?.self) { group in
			for key in keys {
				// Concurrent re-write of the key (value is unchanged).
				group.addTask {
					_ = await db.setValueInTable(table, for: key, to: ["val": key as AnyObject])
					return nil
				}
				// Concurrent read of the key.
				group.addTask {
					guard let json = try? await db.valueFromTable(table, for: key) else { return nil }
					return (key, json)
				}
			}

			var collected = [String: String]()
			for await result in group {
				if let result { collected[result.key] = result.json }
			}
			return collected
		}

		// Every key must have been readable during the concurrent access.
		#expect(readResults.count == keys.count)

		// Each stored value must match the key it was written for — proving the
		// concurrent writes did not cross-contaminate rows.
		for key in keys {
			_ = try #require(readResults[key])
			let dict = try await db.dictValueFromTable(table, for: key)
			#expect(dict["val"] as? String == key)
		}

		await removeDB(db)
	}

	@Test("Database auto-close closes and reopens after inactivity")
	func testAutoCloseTimer() async throws {
		let db = dbForTesting()
		await db.setAutoCloseTimeout(1) // 1 second for quick test
		_ = await db.open()
		let key = "autoclosekey"
		let table: DBTable = "autoCloseTable"
		let sample = "{\"num\":1}"
		_ = await db.setValueInTable(table, for: key, to: sample, autoDeleteAfter: nil)
		// Wait for auto-close
		try await Task.sleep(nanoseconds: 1_400_000_000)
		// The next operation should reopen the DB
		let val = try await db.valueFromTable(table, for: key)
		#expect(val.contains("num"))
		await removeDB(db)
	}

	@Test("URL open database")
	func testURLOpen() async throws {
		let path = pathForDB
		let location = URL(fileURLWithPath: path)

		let testDb = AgileDB()
		let didOpen = await testDb.open(location)

		#expect(didOpen)
		await testDb.close()

		let fileExists = FileManager.default.fileExists(atPath: path)
		if fileExists {
			try? FileManager.default.removeItem(atPath: path)
		}

		await removeDB(testDb)
	}

	@Test("Empty insert")
	func testEmptyInsert() async throws {
		let db = dbForTesting()

		let key = "emptykey"

		let table: DBTable = "table1"
		let successful = await db.setValueInTable(table, for: key, to: "{}", autoDeleteAfter: nil)
		#expect(successful, "setValueFailed")

		let jsonValue = try await db.valueFromTable(table, for: key)
		#expect(jsonValue.count > 0)

		await removeDB(db)
	}

	@Test("Simple insert")
	func testSimpleInsert() async throws {
		let db = dbForTesting()

		let table: DBTable = "table1"
		let key = "SIMPLEINSERTKEY"
		let sample = "{\"numValue\":1,\"dateValue\":\"2014-11-19T18:23:42.434-05:00\",\"link\":true}"
		let sampleData = sample.data(using: String.Encoding.utf8, allowLossyConversion: false)!
		let sampleDict = (try? JSONSerialization.jsonObject(with: sampleData, options: .mutableContainers)) as? [String: any Sendable]
		let successful = await db.setValueInTable(table, for: key, to: sample, autoDeleteAfter: nil)

		#expect(successful, "setValueFailed")

		let jsonValue = try await db.valueFromTable(table, for: key)

		// compare dict values
		let dataValue = jsonValue.data(using: String.Encoding.utf8, allowLossyConversion: false)!
		let objectValues = (try? JSONSerialization.jsonObject(with: dataValue, options: .mutableContainers)) as? [String: any Sendable]
		let equalDicts = objectValues?.count == sampleDict?.count
		let linked = objectValues!["link"] as! Bool

		#expect(linked, "Should be link of true")
		#expect(equalDicts, "Dictionaries don't match")

		await removeDB(db)
	}

	@Test("Array insert")
	func testArrayInsert() async throws {
		let db = dbForTesting()

		let table: DBTable = "table1"
		let key = "ARRAYINSERTKEY"
		let sample = "{\"numValue\":1,\"dateValue\":\"2014-11-19T18:23:42.434-05:00\",\"arrayValue\":[1,2,3,4,5],\"array2Value\":[\"1\",\"b\"]}"
		let sampleData = sample.data(using: String.Encoding.utf8, allowLossyConversion: false)!
		let sampleDict = (try? JSONSerialization.jsonObject(with: sampleData, options: .mutableContainers)) as? [String: any Sendable]
		let successful = await db.setValueInTable(table, for: key, to: sample, autoDeleteAfter: nil)

		#expect(successful, "setValueFailed")

		let jsonValue = try await db.valueFromTable(table, for: key)

		#expect(jsonValue.count > 0)

		// compare dict values
		let dataValue = jsonValue.data(using: String.Encoding.utf8, allowLossyConversion: false)!
		let objectValues = (try? JSONSerialization.jsonObject(with: dataValue, options: .mutableContainers)) as? [String: any Sendable]
		let equalDicts = objectValues?.count == sampleDict?.count
		#expect(equalDicts, "Dictionaries don't match")

		let array = objectValues!["arrayValue"] as! [Int]
		var properArray = array.filter({ $0 == 1 }).count == 1 && array.filter({ $0 == 2 }).count == 1 && array.filter({ $0 == 3 }).count == 1 && array.filter({ $0 == 4 }).count == 1 && array.filter({ $0 == 5 }).count == 1

		#expect(properArray, "improper Array")

		let array2 = objectValues!["array2Value"] as! [String]
		properArray = array2.filter({ $0 == "1" }).count == 1 && array2.filter({ $0 == "b" }).count == 1

		#expect(properArray, "improper Array2")

		await removeDB(db)
	}

	@Test("Change value")
	func testChange() async throws {
		let db = dbForTesting()

		let table: DBTable = "table1"
		let key = "AABBCC3"
		let firstSample = "{\"numValue\":1,\"dateValue\":\"2014-11-19T18:23:42.434-05:00\",\"arrayValue\":[1,2,3,4,5]}"
		var successful = await db.setValueInTable(table, for: key, to: firstSample, autoDeleteAfter: nil)

		#expect(successful, "setValueFailed")

		let sample = "{\"numValue\":2,\"arrayValue\":[6,7,8,9,10]}"
		successful = await db.setValueInTable(table, for: key, to: sample, autoDeleteAfter: nil)

		#expect(successful, "setValueFailed")

		let jsonValue = try await db.valueFromTable(table, for: key)

		#expect(jsonValue.count > 0)

		// compare dict values
		let dataValue = jsonValue.data(using: String.Encoding.utf8, allowLossyConversion: false)!
		let objectValues = (try? JSONSerialization.jsonObject(with: dataValue, options: .mutableContainers)) as? [String: any Sendable]
		let numValue = objectValues!["numValue"] as! Int

		#expect(numValue == 2, "number didn't change properly")

		let dateValue: (any Sendable)? = objectValues!["dateValue"]

		#expect(dateValue == nil, "date still exists")

		let array = objectValues!["arrayValue"] as! [Int]
		let properArray = array.filter({ $0 == 6 }).count == 1 && array.filter({ $0 == 7 }).count == 1 && array.filter({ $0 == 8 }).count == 1 && array.filter({ $0 == 9 }).count == 1 && array.filter({ $0 == 10 }).count == 1

		#expect(properArray, "improper Array")

		await removeDB(db)
	}

	@Test("Table has key")
	func testTableHasKey() async throws {
		let db = dbForTesting()

		let table: DBTable = "table0"
		let sample = "{\"numValue\":2,\"arrayValue\":[6,7,8,9,10]}"

		await db.setValueInTable(table, for: "testKey1", to: sample, autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey2", to: sample, autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey3", to: sample, autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey4", to: sample, autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey5", to: sample, autoDeleteAfter: nil)

		let hasKey = try await db.tableHasKey(table: table, key: "testKey4")
		#expect(hasKey, "invalid test result")
		
		await db.dropTable(table)
		let tableExists = await db.hasTable(table)
		#expect(!tableExists)

		await removeDB(db)
	}

	@Test("Table has all keys")
	func testTableHasAllKeys() async throws {
		let db = dbForTesting()

		let table: DBTable = "table0"
		let sample = "{\"numValue\":2,\"arrayValue\":[6,7,8,9,10]}"

		await db.setValueInTable(table, for: "testKey1", to: sample, autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey2", to: sample, autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey3", to: sample, autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey4", to: sample, autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey5", to: sample, autoDeleteAfter: nil)

		let hasKeys = try await db.tableHasAllKeys(table: table, keys: ["testKey1", "testKey2", "testKey3", "testKey4", "testKey5"])
		#expect(hasKeys, "invalid test result")

		await db.deleteFromTable(table, for: "testKey1")
		let hasKeysAfterDelete = try await db.tableHasAllKeys(table: table, keys: ["testKey1", "testKey2", "testKey3", "testKey4", "testKey5"])
		#expect(!hasKeysAfterDelete, "invalid test result")

		await removeDB(db)
	}

	@Test("Key fetch")
	func testKeyFetch() async throws {
		let db = dbForTesting()

		let table: DBTable = "table2"
		let sample = "{\"numValue\":2,\"arrayValue\":[6,7,8,9,10]}"

		await db.setValueInTable(table, for: "testKey1", to: sample, autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey2", to: sample, autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey3", to: sample, autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey4", to: sample, autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey5", to: sample, autoDeleteAfter: nil)

		let keys = try await db.keysInTable(table, sortOrder: nil)
		let properArray = keys.filter({ $0 == "testKey1" }).count == 1 && keys.filter({ $0 == "testKey2" }).count == 1 && keys.filter({ $0 == "testKey3" }).count == 1 && keys.filter({ $0 == "testKey4" }).count == 1 && keys.filter({ $0 == "testKey5" }).count == 1

		#expect(properArray, "improper keys")

		let keys2 = try await db.keysInTable(table, conditions: [])
		#expect(keys2.count == 5)

		await removeDB(db)
	}

	@Test("Ordered key fetch")
	func testOrderedKeyFetch() async throws {
		let db = dbForTesting()

		let table: DBTable = "table3"
		await db.setValueInTable(table, for: "testKey1", to: "{\"numValue\":2,\"value2\":1}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey2", to: "{\"numValue\":3,\"value2\":1}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey3", to: "{\"numValue\":2,\"value2\":3}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey4", to: "{\"numValue\":1,\"value2\":1}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey5", to: "{\"numValue\":2,\"value2\":2}", autoDeleteAfter: nil)

		let keys = try await db.keysInTable(table, sortOrder: "numValue,value2")
		let properArray = keys[0] == "testKey4" && keys[1] == "testKey1" && keys[2] == "testKey5" && keys[3] == "testKey3" && keys[4] == "testKey2"

		#expect(properArray, "improper keys")

		await removeDB(db)
	}

	@Test("Descending key fetch")
	func testDescendingKeyFetch() async throws {
		let db = dbForTesting()

		let table: DBTable = "table4"
		await db.setValueInTable(table, for: "testKey1", to: "{\"numValue\":2,\"value2\":1}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey2", to: "{\"numValue\":3,\"value2\":1}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey3", to: "{\"numValue\":2,\"value2\":3}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey4", to: "{\"numValue\":1,\"value2\":1}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey5", to: "{\"numValue\":2,\"value2\":2}", autoDeleteAfter: nil)

		let keys = try await db.keysInTable(table, sortOrder: "numValue desc,value2 desc")
		let properArray = keys[0] == "testKey2" && keys[1] == "testKey3" && keys[2] == "testKey5" && keys[3] == "testKey1" && keys[4] == "testKey4"

		#expect(properArray, "improper keys")

		await removeDB(db)
	}

	@Test("Deletion")
	func testDeletion() async throws {
		let db = dbForTesting()

		let table: DBTable = "table6"
		await db.setValueInTable(table, for: "testKey41", to: "{\"numValue\":1,\"account\":\"ACCT1\",\"dateValue\":\"2014-8-19T18:23:42.434-05:00\",\"arrayValue\":[1,2,3,4,5]}", autoDeleteAfter: nil)
		#expect(await db.deleteFromTable(table, for: "testKey41"), "deletion failed")
		let hasKey = try await db.tableHasKey(table: table, key: "testKey41")
		#expect(!hasKey, "key still exists")

		await removeDB(db)
	}

	@Test("Drop table")
	func testDropTable() async throws {
		let db = dbForTesting()

		let table: DBTable = "table7"
		await db.setValueInTable(table, for: "testKey1", to: "{\"numValue\":1,\"account\":\"ACCT1\",\"dateValue\":\"2014-8-19T18:23:42.434-05:00\",\"arrayValue\":[1,2,3,4,5]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey2", to: "{\"numValue\":2,\"account\":\"TEST1\",\"dateValue\":\"2014-9-19T18:23:42.434-05:00\",\"arrayValue\":[6,7,8,9,10]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey3", to: "{\"numValue\":3,\"account\":\"TEST2\",\"dateValue\":\"2014-10-19T18:23:42.434-05:00\",\"arrayValue\":[11,12,13,14,15]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey4", to: "{\"numValue\":4,\"account\":\"TEST3\",\"dateValue\":\"2014-11-19T18:23:42.434-05:00\",\"arrayValue\":[16,17,18,19,20]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey5", to: "{\"numValue\":5,\"account\":\"ACCT3\",\"dateValue\":\"2014-12-19T18:23:42.434-05:00\",\"arrayValue\":[21,22,23,24,25]}", autoDeleteAfter: nil)

		await db.dropTable("table7")
		await #expect(throws: DBError.tableNotFound) {
			try await db.keysInTable(table, sortOrder: nil)
		}

		await removeDB(db)
	}

	@Test("Drop all tables")
	func testDropAllTables() async throws {
		let db = dbForTesting()

		await db.setValueInTable(DBTable(name: "table8"), for: "testKey1", to: "{\"numValue\":1,\"account\":\"ACCT1\",\"dateValue\":\"2014-8-19T18:23:42.434-05:00\",\"arrayValue\":[1,2,3,4,5]}", autoDeleteAfter: nil)
		await db.setValueInTable(DBTable(name: "table9"), for: "testKey2", to: "{\"numValue\":2,\"account\":\"TEST1\",\"dateValue\":\"2014-9-19T18:23:42.434-05:00\",\"arrayValue\":[6,7,8,9,10]}", autoDeleteAfter: nil)
		await db.setValueInTable(DBTable(name: "table10"), for: "testKey3", to: "{\"numValue\":3,\"account\":\"TEST2\",\"dateValue\":\"2014-10-19T18:23:42.434-05:00\",\"arrayValue\":[11,12,13,14,15]}", autoDeleteAfter: nil)
		await db.setValueInTable(DBTable(name: "table11"), for: "testKey4", to: "{\"numValue\":4,\"account\":\"TEST3\",\"dateValue\":\"2014-11-19T18:23:42.434-05:00\",\"arrayValue\":[16,17,18,19,20]}", autoDeleteAfter: nil)
		await db.setValueInTable(DBTable(name: "table12"), for: "testKey5", to: "{\"numValue\":5,\"account\":\"ACCT3\",\"dateValue\":\"2014-12-19T18:23:42.434-05:00\",\"arrayValue\":[21,22,23,24,25]}", autoDeleteAfter: nil)

		await db.dropAllTables()

		await #expect(throws: DBError.tableNotFound) {
			try await db.keysInTable(DBTable(name: "table8"), sortOrder: nil)
		}

		await #expect(throws: DBError.tableNotFound) {
			try await db.keysInTable(DBTable(name: "table9"), sortOrder: nil)
		}

		await #expect(throws: DBError.tableNotFound) {
			try await db.keysInTable(DBTable(name: "table10"), sortOrder: nil)
		}

		await #expect(throws: DBError.tableNotFound) {
			try await db.keysInTable(DBTable(name: "table11"), sortOrder: nil)
		}

		await #expect(throws: DBError.tableNotFound) {
			try await db.keysInTable(DBTable(name: "table12"), sortOrder: nil)
		}

		await removeDB(db)
	}

	@Test("Auto delete")
	func testAutoDelete() async throws {
		let db = dbForTesting()

		let table: DBTable = "AutoDeleteTable1"
		let key = "SimpleDeleteKey"
		let sample = "{\"numValue\":1,\"dateValue\":\"2014-11-19T18:23:42.434-05:00\"}"
		let successful = await db.setValueInTable(table, for: key, to: sample, autoDeleteAfter: Date())

		#expect(successful, "setValueFailed")

		// Wait for 90 seconds for auto-delete to trigger
		try await Task.sleep(nanoseconds: 90_000_000_000)
		
		let keys = try await db.keysInTable(table, sortOrder: nil)
		let filteredKeys = keys.filter({ $0 == key })
		#expect(filteredKeys.count == 0, "keys were returned when table should be empty")

		await removeDB(db)
	}
}

