//
//  ConditionTests.swift
//  AgileDBTests
//
//  Created by Aaron Bratcher on 5/12/19.
//  Copyright © 2019 Aaron Bratcher. All rights reserved.
//

import Foundation
import Testing
@testable import AgileDB

@Suite("Condition Tests")
struct ConditionTests {
	@Test("Missing key condition")
	func missingKeyCondition() async throws {
		let db = dbForTesting()

		let table: DBTable = "table51"
		await db.setValueInTable(table, for: "testKey1", to: "{\"numValue\":1}", autoDeleteAfter: nil)

		// In the JSON document model there is no fixed schema, so a condition on a property
		// that no stored object contains simply matches nothing rather than erroring.
		let accountCondition = DBCondition(set: 0, objectKey: "account", conditionOperator: .equal, value: "ACCT1" as any Sendable)
		let missingKeys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [accountCondition]))
		#expect(missingKeys.count == 0, "Condition on an absent property should match nothing")

		let keyCondition = DBCondition(set: 0, objectKey: "key", conditionOperator: .equal, value: "ACCT1" as any Sendable)

		let keys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [keyCondition]))
		#expect(keys.count == 0, "Keys shouldn't exist")

		await removeDB(db)
	}

	@Test("Simple condition key fetch")
	func simpleConditionKeyFetch() async throws {
		let db = dbForTesting()

		let table: DBTable = "table5"
		await db.setValueInTable(table, for: "testKey1", to: "{\"numValue\":1,\"account\":\"ACCT's 1\",\"dateValue\":\"2014-8-19T18:23:42.434-05:00\",\"arrayValue\":[1,2,3,4,5]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey2", to: "{\"numValue\":2,\"account\":\"ACCT's 1\",\"dateValue\":\"2014-9-19T18:23:42.434-05:00\",\"arrayValue\":[6,7,8,9,10]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey3", to: "{\"numValue\":3,\"account\":\"ACCT2\",\"dateValue\":\"2014-10-19T18:23:42.434-05:00\",\"arrayValue\":[11,12,13,14,15]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey4", to: "{\"numValue\":4,\"account\":\"ACCT2\",\"dateValue\":\"2014-11-19T18:23:42.434-05:00\",\"arrayValue\":[16,17,18,19,20]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey5", to: "{\"numValue\":5,\"account\":\"ACCT3\",\"dateValue\":\"2014-12-19T18:23:42.434-05:00\",\"arrayValue\":[21,22,23,24,25]}", autoDeleteAfter: nil)

		let accountCondition = DBCondition(set: 0, objectKey: "account", conditionOperator: .equal, value: "ACCT's 1" as any Sendable)
		let numCondition = DBCondition(set: 0, objectKey: "numValue", conditionOperator: .greaterThan, value: 1 as any Sendable)

		let keys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [accountCondition, numCondition]))
		#expect(keys.count == 1 && keys[0] == "testKey2", "invalid key")

		await removeDB(db)
	}

	@Test("Contains condition")
	func containsCondition() async throws {
		let db = dbForTesting()

		let table: DBTable = "table6"
		await db.setIndexesForTable(table, to: ["account"])

		await db.setValueInTable(table, for: "testKey1", to: "{\"numValue\":1,\"account\":\"ACCT1\",\"dateValue\":\"2014-8-19T18:23:42.434-05:00\",\"arrayValue\":[1,2,3,4,5]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey2", to: "{\"numValue\":2,\"account\":\"TEST1\",\"dateValue\":\"2014-9-19T18:23:42.434-05:00\",\"arrayValue\":[6,7,8,9,10]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey3", to: "{\"numValue\":3,\"account\":\"TEST2\",\"dateValue\":\"2014-10-19T18:23:42.434-05:00\",\"arrayValue\":[11,12,13,14,15]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey4", to: "{\"numValue\":4,\"account\":\"TEST3\",\"dateValue\":\"2014-11-19T18:23:42.434-05:00\",\"arrayValue\":[16,17,18,19,20]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey5", to: "{\"numValue\":5,\"account\":\"ACCT3\",\"dateValue\":\"2014-12-19T18:23:42.434-05:00\",\"arrayValue\":[21,22,23,24,25]}", autoDeleteAfter: nil)

		let acctCondition = DBCondition(set: 0, objectKey: "account", conditionOperator: .contains, value: "ACCT" as any Sendable)
		let arrayCondition = DBCondition(set: 1, objectKey: "arrayValue", conditionOperator: .contains, value: 10 as any Sendable)

		let keys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [acctCondition, arrayCondition]))
		let success = keys.count == 3 && (keys.filter({ $0 == "testKey1" }).count == 1 && keys.filter({ $0 == "testKey5" }).count == 1 && keys.filter({ $0 == "testKey2" }).count == 1)
		#expect(success, "invalid keys")

		await removeDB(db)
	}

	@Test("Empty condition")
	func emptyCondition() async throws {
		let db = dbForTesting()

		let table: DBTable = "table61"
		await db.setValueInTable(table, for: "testKey1", to: "{\"numValue\":1,\"account\":\"ACCT1\",\"dateValue\":\"2014-8-19T18:23:42.434-05:00\",\"arrayValue\":[1,2,3,4,5]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey2", to: "{\"numValue\":2,\"account\":\"TEST1\",\"dateValue\":\"2014-9-19T18:23:42.434-05:00\",\"arrayValue\":[6,7,8,9,10]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey3", to: "{\"numValue\":3,\"account\":\"TEST2\",\"dateValue\":\"2014-10-19T18:23:42.434-05:00\",\"arrayValue\":[11,12,13,14,15]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey4", to: "{\"numValue\":4,\"account\":\"TEST3\",\"dateValue\":\"2014-11-19T18:23:42.434-05:00\",\"arrayValue\":[16,17,18,19,20]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey5", to: "{\"numValue\":5,\"account\":\"ACCT3\",\"dateValue\":\"2014-12-19T18:23:42.434-05:00\",\"arrayValue\":[21,22,23,24,25]}", autoDeleteAfter: nil)

		let conditionArray = [DBCondition]()

		let keys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: conditionArray))
		let success = keys.count == 5
		#expect(success, "invalid keys")

		await removeDB(db)
	}

	@Test("Validate objects")
	func validateObjects() async throws {
		let db = dbForTesting()

		let table: DBTable = "Transactions"
		let key = UUID().uuidString
		let dict = [
			"key": key
			, "accountKey": "Checking"
			, "locationKey": "Kroger"
			, "categoryKey": "Food"
		]

		let data = try JSONSerialization.data(withJSONObject: dict, options: .prettyPrinted)
		let json = String(data: data, encoding: .utf8)!
		await db.setValueInTable(table, for: key, to: json)

		var conditions: [DBCondition] = []
		conditions.append(DBCondition(set: 0, objectKey: "accountKey", conditionOperator: .equal, value: "Checking" as any Sendable))
		conditions.append(DBCondition(set: 0, objectKey: "locationKey", conditionOperator: .equal, value: "Kroger" as any Sendable))

		conditions.append(DBCondition(set: 1, objectKey: "accountKey", conditionOperator: .equal, value: "Checking" as any Sendable))
		conditions.append(DBCondition(set: 1, objectKey: "note", conditionOperator: .equal, value: "Kroger" as any Sendable))

		let keys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: conditions, validateObjects: true))
		#expect(keys.count == 1)

		await removeDB(db)
	}

	@Test("inList string condition")
	func inListStringCondition() async throws {
		let db = dbForTesting()

		let table: DBTable = "table70"
		await db.setValueInTable(table, for: "testKey1", to: "{\"account\":\"ACCT1\"}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey2", to: "{\"account\":\"ACCT2\"}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey3", to: "{\"account\":\"ACCT3\"}", autoDeleteAfter: nil)

		let condition = DBCondition(set: 0, objectKey: "account", conditionOperator: .inList, value: ["ACCT1", "ACCT3"] as any Sendable)
		let keys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [condition]))

		#expect(keys.count == 2)
		#expect(keys.contains("testKey1"))
		#expect(keys.contains("testKey3"))

		await removeDB(db)
	}

	@Test("inList int condition")
	func inListIntCondition() async throws {
		let db = dbForTesting()

		let table: DBTable = "table71"
		for value in 1...5 {
			await db.setValueInTable(table, for: "testKey\(value)", to: "{\"numValue\":\(value)}", autoDeleteAfter: nil)
		}

		let condition = DBCondition(set: 0, objectKey: "numValue", conditionOperator: .inList, value: [1, 3, 5] as any Sendable)
		let keys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [condition]))

		#expect(keys.count == 3)
		#expect(keys.contains("testKey1"))
		#expect(keys.contains("testKey3"))
		#expect(keys.contains("testKey5"))

		await removeDB(db)
	}

	@Test("inList double condition")
	func inListDoubleCondition() async throws {
		let db = dbForTesting()

		let table: DBTable = "table72"
		await db.setValueInTable(table, for: "testKey1", to: "{\"cost\":1.5}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey2", to: "{\"cost\":2.5}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey3", to: "{\"cost\":3.5}", autoDeleteAfter: nil)

		let condition = DBCondition(set: 0, objectKey: "cost", conditionOperator: .inList, value: [1.5, 3.5] as any Sendable)
		let keys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [condition]))

		#expect(keys.count == 2)
		#expect(keys.contains("testKey1"))
		#expect(keys.contains("testKey3"))

		await removeDB(db)
	}

	@Test("Contains string array condition")
	func containsStringArrayCondition() async throws {
		let db = dbForTesting()

		let table: DBTable = "table73"
		await db.setValueInTable(table, for: "testKey1", to: "{\"numValue\":1,\"tags\":[\"red\",\"green\",\"blue\"]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey2", to: "{\"numValue\":2,\"tags\":[\"green\",\"yellow\"]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey3", to: "{\"numValue\":3,\"tags\":[\"black\",\"white\"]}", autoDeleteAfter: nil)

		let condition = DBCondition(set: 0, objectKey: "tags", conditionOperator: .contains, value: "green" as any Sendable)
		let keys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [condition]))

		#expect(keys.count == 2)
		#expect(keys.contains("testKey1"))
		#expect(keys.contains("testKey2"))

		await removeDB(db)
	}

	@Test("Contains double array condition")
	func containsDoubleArrayCondition() async throws {
		let db = dbForTesting()

		let table: DBTable = "table74"
		await db.setValueInTable(table, for: "testKey1", to: "{\"numValue\":1,\"prices\":[1.5,2.5,3.5]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey2", to: "{\"numValue\":2,\"prices\":[2.5,4.5]}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey3", to: "{\"numValue\":3,\"prices\":[5.5,6.5]}", autoDeleteAfter: nil)

		let condition = DBCondition(set: 0, objectKey: "prices", conditionOperator: .contains, value: 2.5 as any Sendable)
		let keys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [condition]))

		#expect(keys.count == 2)
		#expect(keys.contains("testKey1"))
		#expect(keys.contains("testKey2"))

		await removeDB(db)
	}

	@Test("Date condition")
	func dateCondition() async throws {
		let db = dbForTesting()

		let table: DBTable = "table75"
		let earlyDate = Date(timeIntervalSince1970: 1_000_000)
		let middleDate = Date(timeIntervalSince1970: 2_000_000)
		let lateDate = Date(timeIntervalSince1970: 3_000_000)

		await db.setValueInTable(table, for: "testKey1", to: "{\"dateValue\":\"\(AgileDB.stringValueForDate(earlyDate))\"}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey2", to: "{\"dateValue\":\"\(AgileDB.stringValueForDate(lateDate))\"}", autoDeleteAfter: nil)

		let lessThanCondition = DBCondition(set: 0, objectKey: "dateValue", conditionOperator: .lessThan, value: middleDate as any Sendable)
		let earlierKeys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [lessThanCondition]))
		#expect(earlierKeys.count == 1)
		#expect(earlierKeys.contains("testKey1"))

		let greaterThanCondition = DBCondition(set: 0, objectKey: "dateValue", conditionOperator: .greaterThan, value: middleDate as any Sendable)
		let laterKeys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [greaterThanCondition]))
		#expect(laterKeys.count == 1)
		#expect(laterKeys.contains("testKey2"))

		await removeDB(db)
	}

	@Test("Bool condition")
	func boolCondition() async throws {
		let db = dbForTesting()

		let table: DBTable = "table76"
		await db.setValueInTable(table, for: "testKey1", to: "{\"numValue\":1,\"flag\":true}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey2", to: "{\"numValue\":2,\"flag\":false}", autoDeleteAfter: nil)
		await db.setValueInTable(table, for: "testKey3", to: "{\"numValue\":3,\"flag\":true}", autoDeleteAfter: nil)

		let trueCondition = DBCondition(set: 0, objectKey: "flag", conditionOperator: .equal, value: true as any Sendable)
		let trueKeys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [trueCondition]))
		#expect(trueKeys.count == 2)
		#expect(trueKeys.contains("testKey1"))
		#expect(trueKeys.contains("testKey3"))

		let falseCondition = DBCondition(set: 0, objectKey: "flag", conditionOperator: .equal, value: false as any Sendable)
		let falseKeys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [falseCondition]))
		#expect(falseKeys.count == 1)
		#expect(falseKeys.contains("testKey2"))

		await removeDB(db)
	}

	@Test("Comparison operators")
	func comparisonOperators() async throws {
		let db = dbForTesting()

		let table: DBTable = "table77"
		for value in 1...5 {
			await db.setValueInTable(table, for: "testKey\(value)", to: "{\"numValue\":\(value)}", autoDeleteAfter: nil)
		}

		let notEqual = DBCondition(set: 0, objectKey: "numValue", conditionOperator: .notEqual, value: 3 as any Sendable)
		let notEqualKeys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [notEqual]))
		#expect(notEqualKeys.count == 4)
		#expect(!notEqualKeys.contains("testKey3"))

		let lessThan = DBCondition(set: 0, objectKey: "numValue", conditionOperator: .lessThan, value: 3 as any Sendable)
		let lessThanKeys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [lessThan]))
		#expect(lessThanKeys.count == 2)
		#expect(lessThanKeys.contains("testKey1"))
		#expect(lessThanKeys.contains("testKey2"))

		let lessThanOrEqual = DBCondition(set: 0, objectKey: "numValue", conditionOperator: .lessThanOrEqual, value: 3 as any Sendable)
		let lessThanOrEqualKeys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [lessThanOrEqual]))
		#expect(lessThanOrEqualKeys.count == 3)

		let greaterThanOrEqual = DBCondition(set: 0, objectKey: "numValue", conditionOperator: .greaterThanOrEqual, value: 4 as any Sendable)
		let greaterThanOrEqualKeys = try #require(await db.keysInTable(table, sortOrder: nil, conditions: [greaterThanOrEqual]))
		#expect(greaterThanOrEqualKeys.count == 2)
		#expect(greaterThanOrEqualKeys.contains("testKey4"))
		#expect(greaterThanOrEqualKeys.contains("testKey5"))

		await removeDB(db)
	}
}
