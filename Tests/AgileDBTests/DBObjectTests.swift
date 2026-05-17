//
//  DBObjectTests.swift
//  AgileDBTests
//
//  Created by Aaron Bratcher on 4/10/20.
//  Copyright © 2020 Aaron Bratcher. All rights reserved.
//

import Foundation
import Testing
@testable import AgileDB

struct ShippingAddress: Codable {
	let streetNumber: Int
	let streetName: String
	let city: String
	let state: String
	let zip: String
	let active: Bool
	let residents: [String]
}

extension ShippingAddress: Equatable {
	static func == (lhs: Self, rhs: Self) -> Bool {
		return lhs.streetNumber == rhs.streetNumber
		&& lhs.streetName == rhs.streetName
		&& lhs.city == rhs.city
		&& lhs.state == rhs.state
		&& lhs.zip == rhs.zip
		&& lhs.active == rhs.active
		&& lhs.residents == rhs.residents
	}
}

typealias Author =  [String: String]
struct AddressHolder: DBObject {
	static let table: DBTable = "AddressHolder"

	var key = UUID().uuidString
	var address: ShippingAddress
	var addresses: [ShippingAddress]?
	var authorName: Author = ["first": "Aaron", "middle": "L", "last": "Bratcher"]
	var authors: [Author] = [
		["first": "Aaron", "middle": "L", "last": "Bratcher"],
		["first": "Aaron2", "middle": "L2", "last": "Bratcher2"]
	]

	init(using address: ShippingAddress, addresses: [ShippingAddress]?) {
		self.address = address
		self.addresses = addresses
	}
}

struct Transaction: DBObject {
	static let table: DBTable = "MoneyTransaction"

	var key = UUID().uuidString
	var date: Date
	var accountKey: String
	var notes: [String]?
	var amount: Int
	var purchaseOrders: [Int]?
	var purchaseDates: [Date]?
	var isNew = true
}

enum TransactionValue {
	static let key = "TKey"
	static let accountKey = "accountKey"
	static let notes = ["Note1", "Note2", "Note3"]
	static let amount = 100
	static let purchaseOrders = [1, 2, 3, 4, 5]
	static let isNew = true
}

struct EncodingTransaction: DBObject {
	static let table: DBTable = "EncodingTransaction"

	var key = UUID().uuidString
	var accountKey = "XK-12345"
	var amount = 100
	var amounts = [200, 300, 400]
	var cost = 100.5
	var costs = [200.1, 300.2, 400.3]
	var users = ["user1", "user2", "user3"]
	var isNew = true
	var today = Date()
	var dates = [Date(), Date(), Date()]
	var location = Location()
	var locations = [Location(), Location(), Location()]
}

struct Location: DBObject {
	static let table: DBTable = "Locations"

	var key = UUID().uuidString
	var name = "Kroger"
	var manager = Person()
}

struct Person: DBObject {
	static let table: DBTable = "People"

	var key = UUID().uuidString
	var firstName = "Store"
	var lastName = "Manaager"
}

@Suite("Database Object Tests")
struct DBObjectTests {
	@Test("Save object to database")
	func testSaveObject() async throws {
		let db = dbForTesting()

		let date: Date = Date()
		let purchaseDates = [date, date, date]

		let transaction = Transaction(key: TransactionValue.key, date: date, accountKey: TransactionValue.accountKey, notes: TransactionValue.notes, amount: TransactionValue.amount, purchaseOrders: TransactionValue.purchaseOrders, purchaseDates: purchaseDates, isNew: TransactionValue.isNew)
		await transaction.save(to: db)

		let testTransaction = try #require(await Transaction.init(db: db, key: TransactionValue.key))

		let dateCompare = Calendar(identifier: .gregorian).compare(date, to: testTransaction.date, toGranularity: .second)
		let dateCompare0 = Calendar(identifier: .gregorian).compare(date, to: testTransaction.purchaseDates![0], toGranularity: .second)
		let dateCompare1 = Calendar(identifier: .gregorian).compare(date, to: testTransaction.purchaseDates![1], toGranularity: .second)
		let dateCompare2 = Calendar(identifier: .gregorian).compare(date, to: testTransaction.purchaseDates![2], toGranularity: .second)

		#expect(testTransaction.key == TransactionValue.key)
		#expect(testTransaction.accountKey == TransactionValue.accountKey)
		#expect(testTransaction.notes == TransactionValue.notes)
		#expect(testTransaction.amount == TransactionValue.amount)
		#expect(testTransaction.purchaseOrders == TransactionValue.purchaseOrders)
		#expect(dateCompare == ComparisonResult.orderedSame)
		#expect(dateCompare0 == ComparisonResult.orderedSame)
		#expect(dateCompare1 == ComparisonResult.orderedSame)
		#expect(dateCompare2 == ComparisonResult.orderedSame)
		#expect(testTransaction.isNew == TransactionValue.isNew)

		await removeDB(db)
	}

	@Test("Complex object with nested structures")
	func testComplexObject() async throws {
		let db = dbForTesting()

		let address = ShippingAddress(streetNumber: 123, streetName: "Main St", city: "Chicago", state: "IL", zip: "60614", active: false, residents: ["Man", "Woman", "Child"])
		let addresses = [address, address]

		var addressHolder = AddressHolder(using: address, addresses: nil)
		await addressHolder.save(to: db)

		let newHolder = try await AddressHolder.load(from: db, for: addressHolder.key)
		#expect(newHolder.address == address)
		#expect(newHolder.authorName == addressHolder.authorName)
		#expect(newHolder.authors == addressHolder.authors)

		addressHolder = AddressHolder(using: address, addresses: addresses)
		await addressHolder.save(to: db)

		let newHolder2 = try await AddressHolder.load(from: db, for: addressHolder.key)
		#expect(newHolder2.address == address)
		#expect(newHolder2.addresses == addresses)
		#expect(newHolder2.authorName == addressHolder.authorName)
		#expect(newHolder.authors == addressHolder.authors)

		await removeDB(db)
	}

	@Test("Save nil value")
	func testSaveNilValue() async throws {
		let db = dbForTesting()

		let date = Date()
		let transaction = Transaction(key: TransactionValue.key, date: date, accountKey: TransactionValue.accountKey, amount: TransactionValue.amount, isNew: TransactionValue.isNew)
		await transaction.save(to: db)

		let testTransaction = try #require(await Transaction.init(db: db, key: TransactionValue.key))

		#expect(testTransaction.notes == nil)

		await removeDB(db)
	}

	@Test("Load async object")
	func testAsyncObject() async throws {
		let db = dbForTesting()

		let transaction = Transaction(key: TransactionValue.key, date: Date(), accountKey: TransactionValue.accountKey, notes: TransactionValue.notes, amount: TransactionValue.amount, isNew: TransactionValue.isNew)
		await transaction.save(to: db)

		let transaction2 = try await Transaction.load(from: db, for: TransactionValue.key)
		#expect(transaction2.key == TransactionValue.key)
		#expect(transaction2.accountKey == TransactionValue.accountKey)

		await removeDB(db)
	}

	@Test("Delete object from database")
	func testDelete() async throws {
		let db = dbForTesting()

		let transaction = Transaction(key: TransactionValue.key, date: Date(), accountKey: TransactionValue.accountKey, notes: TransactionValue.notes, amount: TransactionValue.amount, isNew: TransactionValue.isNew)
		await transaction.save(to: db)
		await transaction.delete(from: db)
		let hasKey = try await db.tableHasKey(table: Transaction.table, key: TransactionValue.key)

		#expect(!hasKey)

		await removeDB(db)
	}

	@Test("Load object from database")
	func testLoadFromDB() async throws {
		let db = dbForTesting()

		let transaction = Transaction(key: TransactionValue.key, date: Date(), accountKey: TransactionValue.accountKey, notes: TransactionValue.notes, amount: TransactionValue.amount, isNew: TransactionValue.isNew)
		await transaction.save(to: db)

		let object = try await Transaction.load(from: db, for: TransactionValue.key)
		#expect(object.accountKey == TransactionValue.accountKey)
		#expect(object.amount == TransactionValue.amount)

		await removeDB(db)
	}

	@Test("Nested object save")
	func testNestedSave() async throws {
		let db = dbForTesting()

		let transaction = EncodingTransaction()
		await transaction.save(to: db)

		let loadedTransaction = await EncodingTransaction(db: db, key: transaction.key)
		let encodingTransaction = try #require(loadedTransaction)
		#expect(transaction.amount == encodingTransaction.amount)
		#expect(encodingTransaction.locations.count == 3)
		#expect(encodingTransaction.locations[0].manager.firstName == "Store")

		await removeDB(db)
	}
}
