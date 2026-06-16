//
//  DBCursor.swift
//  AgileDB
//
//  Created by Aaron Bratcher on 4/10/20.
//  Copyright © 2020 Aaron Bratcher. All rights reserved.
//

import Foundation

public class DBResults<T: DBObject>: Identifiable {
	public typealias CustomClassValue = T
	public typealias CustomClassIndex = Array<String>.Index

	public let id = UUID()
	public let keys: [String]
	private let db: AgileDB

	public var count: Int { keys.count }
	public var isEmpty: Bool { keys.isEmpty }

	public init(db: AgileDB = AgileDB.shared, keys: [String] = []) {
		self.db = db
		self.keys = keys
	}

	/// Asynchronously loads the object at the given index. Returns nil if the index is out
	/// of bounds or the object can no longer be loaded from the database.
	public func object(at index: CustomClassIndex) async -> T? {
		guard index >= 0 && index < keys.count else { return nil }
		return await T(db: db, key: keys[index])
	}
}

extension DBResults: AsyncSequence {
	public typealias Element = T

	/// Loads each object on demand. Only keys are stored, so loading is asynchronous —
	/// iterate with `for await object in results`.
	public struct AsyncIterator: AsyncIteratorProtocol {
		private let results: DBResults
		private var index = 0

		init(results: DBResults) {
			self.results = results
		}

		public mutating func next() async -> T? {
			// Keys that can no longer be loaded are skipped; `nil` is returned only
			// once every key has been visited, signalling the end of the sequence.
			while index < results.keys.count {
				let current = index
				index += 1
				if let object = await results.object(at: current) {
					return object
				}
			}

			return nil
		}
	}

	public func makeAsyncIterator() -> AsyncIterator {
		AsyncIterator(results: self)
	}
}
