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

		// Poll the most recent emission's count until it reaches the expected value (or
		// times out). This tolerates the publisher's intermediate emissions, avoids
		// depending on a single fixed delay, and never indexes into a possibly-empty array.
		func waitForLatestCount(_ expected: Int) async -> Int? {
			for _ in 0 ..< 50 {
				try? await Task.sleep(nanoseconds: 100_000_000)
				if let count = receivedResults.last?.count, count == expected { return count }
			}
			return receivedResults.last?.count
		}

		// The initial emission can be coalesced away by the publisher's dropFirst() if the
		// asynchronous fetch resolves before the subscription attaches, so assert on a
		// change made *after* subscribing: reaching 10 proves the 9 pre-existing rows were
		// counted plus the one just added.
		var transaction = Transaction(date: Date(), accountKey: "A1", amount: 100)
		transaction.key = "K10"
		await transaction.save(to: db)

		#expect(await waitForLatestCount(10) == 10, "Update should reflect 10 items (9 existing + 1 added)")

		cancellable.cancel()

		await removeDB(db)
	}

	@Test("Updated publishers")
	func testUpdatedPublishers() async throws {
		let db = dbForTesting()

		await addObjectsToDB(db)

		let account1Condition = DBCondition(set: 0, objectKey: "accountKey", conditionOperator: .equal, value: "A1" as any Sendable)
		let account2Condition = DBCondition(set: 0, objectKey: "accountKey", conditionOperator: .equal, value: "A2" as any Sendable)

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

		// Poll a result set's latest count until it matches (or times out), rather than
		// indexing into arrays that may not yet hold the expected emissions.
		func waitForCount(_ latest: () -> Int?, _ expected: Int) async -> Int? {
			for _ in 0 ..< 50 {
				try? await Task.sleep(nanoseconds: 100_000_000)
				if let count = latest(), count == expected { return count }
			}
			return latest()
		}

		// The initial emission can be coalesced away by the publisher's dropFirst(), so
		// assert only on changes made after subscribing. The post-change counts inherently
		// validate the initial state (A1 reaching 6 proves it started at 5) and confirm
		// that each change is routed to the matching conditional publisher.

		// Add an A1 transaction -> publisher 1 reflects 6 items (5 existing + 1).
		var transaction = Transaction(date: Date(), accountKey: "A1", amount: 100)
		transaction.key = "K10"
		await transaction.save(to: db)
		#expect(await waitForCount({ results1.last?.count }, 6) == 6, "Publisher 1 should have 6 items after adding an A1 transaction")

		// Add an A2 transaction -> publisher 2 reflects 5 items (4 existing + 1).
		transaction = Transaction(date: Date(), accountKey: "A2", amount: 100)
		transaction.key = "K11"
		await transaction.save(to: db)
		#expect(await waitForCount({ results2.last?.count }, 5) == 5, "Publisher 2 should have 5 items after adding an A2 transaction")

		// Delete the A1 transaction -> publisher 1 returns to 5 items.
		await db.deleteFromTable(Transaction.table, for: "K10")
		#expect(await waitForCount({ results1.last?.count }, 5) == 5, "Publisher 1 should have 5 items after deleting the A1 transaction")

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

	@Test("Publisher receives updates for changed values")
	func testPublisherReceivesUpdates() async throws {
		let db = dbForTesting()
		struct TestObj: DBObject {
			static let table: DBTable = "PubTable"
			var key = UUID().uuidString
			var value = 0
		}
		let publisher = await db.publisher(sortOrder: nil, conditions: nil, validateObjects: false) as DBResultsPublisher<TestObj>

		// DBResults is a key cursor whose objects must be loaded asynchronously, so
		// capture each emitted result set and materialize values via object(at:).
		var receivedResults = [DBResults<TestObj>]()
		var receivedError: Error? = nil
		let cancellable = publisher.sink(receiveCompletion: { completion in
			if case let .failure(error) = completion {
				receivedError = error
			}
		}, receiveValue: { results in
			receivedResults.append(results)
		})

		// Load the first object of the most recently emitted result set.
		func latestValue() async -> Int? {
			guard let last = receivedResults.last else { return nil }
			return await last.object(at: 0)?.value
		}

		// Poll the latest emission until its object holds `expected` (or time out).
		// Polling avoids depending on a single fixed delay and tolerates the extra
		// intermediate emissions the publisher produces around subscription time.
		func waitForValue(_ expected: Int) async -> Int? {
			var latest: Int?
			for _ in 0 ..< 30 {
				try? await Task.sleep(nanoseconds: 100_000_000)
				latest = await latestValue()
				if latest == expected { return latest }
			}
			return latest
		}

		// Save values *after* subscribing. The initial subscription emission can be
		// dropped by the publisher's dropFirst(), but every change made once the
		// subscription is established is reliably delivered.
		var object = TestObj(value: 1)
		await object.save(to: db)
		#expect(await waitForValue(1) == 1)

		// Update the value; the publisher should emit a result reflecting it.
		object.value = 99
		await object.save(to: db)
		#expect(await waitForValue(99) == 99)

		cancellable.cancel()
		#expect(receivedError == nil, "No error should have been received by the publisher")
		await removeDB(db)
	}
}
