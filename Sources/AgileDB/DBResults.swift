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

	/// Asynchronously loads the object at the given index.
	public func object(at index: CustomClassIndex) async -> T? {
		guard index >= 0 && index < keys.count else { return nil }
		return await T(db: db, key: keys[index])
	}
}

extension DBResults: RandomAccessCollection {
	public var startIndex: CustomClassIndex { return keys.startIndex }
	public var endIndex: CustomClassIndex { return keys.endIndex }

	/// Returns nil — use `object(at:) async` to load objects asynchronously.
	public subscript(index: CustomClassIndex) -> CustomClassValue? {
		return nil
	}
}
