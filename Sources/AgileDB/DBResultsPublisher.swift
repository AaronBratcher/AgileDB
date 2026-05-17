//
//  DBResultsPublisher.swift
//  AgileDB
//
//  Created by Aaron Bratcher on 4/30/20.
//  Copyright © 2020 Aaron Bratcher. All rights reserved.
//

import Foundation
import Combine

protocol UpdatablePublisher {
	var id: UUID { get }
	var table: DBTable { get }
	var conditions: [DBCondition]? { get }
	var validateObjects: Bool { get }

	func updateSubject()
	func clearResults(in table: DBTable)
}

public class DBResultsPublisher<T: DBObject>: UpdatablePublisher, Identifiable, @unchecked Sendable {
	public typealias Output = DBResults<T>
	public typealias Failure = DBError

	public let id = UUID()
	let table: DBTable
	let conditions: [DBCondition]?
	let validateObjects: Bool

	private let subject: CurrentValueSubject<DBResults<T>, Failure>
	private let subscriptionLock = NSLock()
	private var subscriptions = 0
	private let db: AgileDB
	private let sortOrder: String?

	init(db: AgileDB, table: DBTable, sortOrder: String? = nil, conditions: [DBCondition]? = nil, validateObjects: Bool = false) {
		self.db = db
		self.table = table
		self.sortOrder = sortOrder
		self.conditions = conditions
		self.validateObjects = validateObjects
		subject = CurrentValueSubject(DBResults(db: db, keys: []))
	}

	deinit {
		stopPublisher()
	}

	func stopPublisher() {
		let db = self.db
		let publisherID = self.id
		Task { await db.removePublisherWithID(publisherID) }
	}
}

extension DBResultsPublisher: Publisher {
	public func receive<S>(subscriber: S)
	where S: Subscriber, DBResultsPublisher.Failure == S.Failure, DBResultsPublisher.Output == S.Input {
		var start = false

		subscriptionLock.lock()
		subscriptions += 1
		start = subscriptions == 1
		subscriptionLock.unlock()

		if start {
			updateSubject()
		}
		DBResultsSubscription(fetchPublisher: self, subscriber: AnySubscriber(subscriber))
	}

	func updateSubject() {
		// Capture only Sendable values to cross isolation boundaries safely
		let db = self.db
		let sortOrder = self.sortOrder
		let conditions = self.conditions
		let validateObjects = self.validateObjects
		let subject = self.subject
		let table = self.table
		Task {
			do {
				let keys = try await db.keysInTable(table, sortOrder: sortOrder, conditions: conditions, validateObjects: validateObjects)
				let result = DBResults<T>(db: db, keys: keys)
				subject.send(result)
			}
			catch {}
		}
	}

	func clearResults(in table: DBTable) {
		if T.table != table { return }
		let result = DBResults<T>(db: self.db, keys: [])
		self.subject.send(result)
	}

	private func dropSubscription() {
		subscriptionLock.lock()
		subscriptions -= 1
		let stop = subscriptions == 0
		subscriptionLock.unlock()

		if stop {
			stopPublisher()
		}
	}

	private class DBResultsSubscription: Subscription {
		private var publisher: DBResultsPublisher?
		private var cancellable: AnyCancellable?

		@discardableResult
		init(fetchPublisher: DBResultsPublisher, subscriber: AnySubscriber<Output, Failure>) {
			self.publisher = fetchPublisher

			subscriber.receive(subscription: self)

			cancellable = fetchPublisher.subject.sink(receiveCompletion: { completion in
				subscriber.receive(completion: completion)
			}, receiveValue: { value in
				_ = subscriber.receive(value)
			})
		}

		func request(_ demand: Subscribers.Demand) { }

		func cancel() {
			cancellable?.cancel()
			cancellable = nil
			publisher?.dropSubscription()
			publisher = nil
		}
	}
}
