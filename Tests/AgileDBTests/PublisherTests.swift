//
//  PublisherTests.swift
//  AgileDBTests
//
//  Created by Aaron Bratcher  on 5/3/20.
//  Copyright © 2020 Aaron Bratcher. All rights reserved.
//

import Foundation
import Testing
import Combine
@testable import AgileDB

@Suite("Publisher Tests")
struct PublisherTests {
	@Test("Create publisher")
	func testCreatePublisher() async throws {
		let db = dbForTesting()

		await addObjectsToDB(db)

		var receivedResults: [DBResults<Transaction>] = []
		let publisher: DBResultsPublisher<Transaction> = await db.publisher()

		let cancellable = publisher.sink(receiveCompletion: { _ in }) { results in
			receivedResults.append(results)
		}

		// Wait for initial publisher update
		try await Task.sleep(nanoseconds: 500_000_000)

		// Add a new transaction
		var transaction = Transaction(date: Date(), accountKey: "A1", amount: 100)
		transaction.key = "K10"
		await transaction.save(to: db)

		// Wait for publisher update
		try await Task.sleep(nanoseconds: 1_500_000_000)

		// Verify we received the expected updates
		#expect(receivedResults.count >= 2, "Should have received at least 2 updates")
		#expect(receivedResults[0].count == 9, "First update should have 9 items")
		#expect(receivedResults[1].count == 10, "Second update should have 10 items")

		cancellable.cancel()

		await removeDB(db)
	}

	@Test("Updated publishers")
	func testUpdatedPublishers() async throws {
		let db = dbForTesting()

		await addObjectsToDB(db)

		let account1Condition = DBCondition(set: 0, objectKey: "accountKey", conditionOperator: .equal, value: "A1" as AnyObject)
		let account2Condition = DBCondition(set: 0, objectKey: "accountKey", conditionOperator: .equal, value: "A2" as AnyObject)

		var results1: [DBResults<Transaction>] = []
		var results2: [DBResults<Transaction>] = []

		let publisher1: DBResultsPublisher<Transaction> = await db.publisher(conditions: [account1Condition])
		let subscription1 = publisher1.sink(receiveCompletion: { _ in }) { results in
			results1.append(results)
		}

		let publisher2: DBResultsPublisher<Transaction> = await db.publisher(conditions: [account2Condition])
		let subscription2 = publisher2.sink(receiveCompletion: { _ in }) { results in
			results2.append(results)
		}

		// Wait for initial updates
		try await Task.sleep(nanoseconds: 500_000_000)

		// Add transactions
		var transaction = Transaction(date: Date(), accountKey: "A1", amount: 100)
		transaction.key = "K10"
		await transaction.save(to: db)

		transaction = Transaction(date: Date(), accountKey: "A2", amount: 100)
		transaction.key = "K11"
		await transaction.save(to: db)

		// Wait for updates
		try await Task.sleep(nanoseconds: 1_500_000_000)

		// Delete a transaction
		await db.deleteFromTable(Transaction.table, for: "K10")

		// Wait for final updates
		try await Task.sleep(nanoseconds: 1_500_000_000)

		// Verify publisher 1 received expected updates
		#expect(results1.count >= 3, "Publisher 1 should have received at least 3 updates")
		#expect(results1[0].count == 5, "Publisher 1 first update should have 5 items (initial)")
		#expect(results1[1].count == 6, "Publisher 1 second update should have 6 items (after adding K10)")
		#expect(results1[2].count == 5, "Publisher 1 third update should have 5 items (after deleting K10)")

		// Verify publisher 2 received expected updates
		#expect(results2.count >= 2, "Publisher 2 should have received at least 2 updates")
		#expect(results2[0].count == 4, "Publisher 2 first update should have 4 items (initial)")
		#expect(results2[1].count == 5, "Publisher 2 second update should have 5 items (after adding K11)")

		subscription1.cancel()
		subscription2.cancel()

		await removeDB(db)
	}

	func addObjectsToDB(_ db: AgileDB) async {
		var transaction = Transaction(date: Date(), accountKey: "A1", amount: 100)
		transaction.key = "K1"
		await transaction.save(to: db)

		transaction = Transaction(date: Date(), accountKey: "A1", amount: 200)
		transaction.key = "K2"
		await transaction.save(to: db)

		transaction = Transaction(date: Date(), accountKey: "A1", amount: 300)
		transaction.key = "K3"
		await transaction.save(to: db)

		transaction = Transaction(date: Date(), accountKey: "A1", amount: 400)
		transaction.key = "K4"
		await transaction.save(to: db)

		transaction = Transaction(date: Date(), accountKey: "A1", amount: 500)
		transaction.key = "K5"
		await transaction.save(to: db)

		transaction = Transaction(date: Date(), accountKey: "A2", amount: 600)
		transaction.key = "K6"
		await transaction.save(to: db)

		transaction = Transaction(date: Date(), accountKey: "A2", amount: 700)
		transaction.key = "K7"
		await transaction.save(to: db)

		transaction = Transaction(date: Date(), accountKey: "A2", amount: 800)
		transaction.key = "K8"
		await transaction.save(to: db)

		transaction = Transaction(date: Date(), accountKey: "A2", amount: 900)
		transaction.key = "K9"
		await transaction.save(to: db)
	}

//	@Test("Publisher receives updates for changed values")
//	func testPublisherReceivesUpdates() async throws {
//		let db = dbForTesting()
//		struct TestObj: DBObject {
//			static let table: DBTable = "PubTable"
//			var key = UUID().uuidString
//			var value = 0
//		}
//		let initial = TestObj(value: 1)
//		await initial.save(to: db)
//
//		let publisher = await db.publisher(sortOrder: nil, conditions: nil, validateObjects: false) as DBResultsPublisher<TestObj>
//		var received = [TestObj]()
//		var receivedError: Error? = nil
//		let cancellable = publisher.sink(receiveCompletion: { completion in
//			if case let .failure(error) = completion {
//				receivedError = error
//			}
//		}, receiveValue: { objs in
//			received = Array(_immutableCocoaArray: objs)
//		})
//		await Task.yield()
//		// Initial state
//		#expect(received.first?.value == 1)
//
//		// Update value, publisher should get new value
//		var updated = initial
//		updated.value = 99
//		await updated.save(to: db)
//
//		// Wait briefly for publisher to receive update
//		try await Task.sleep(nanoseconds: 500_000_000)
//		#expect(received.first?.value == 99)
//
//		cancellable.cancel()
//		#expect(receivedError == nil, "No error should have been received by the publisher")
//		await removeDB(db)
//	}
}
