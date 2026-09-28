//
//  DBPredicate.swift
//  AgileDB
//
//  Created by Aaron Bratcher on 7/4/26.
//

/**
Holds the `DBCondition`s produced by expanding a `#Predicate<T> { ... }` macro. Pass it to
`@Query`'s `filter:` parameter, or read `conditions` directly for use with `keysInTable`,
`countKeysInTable`, or a `publisher`.
*/
public struct DBPredicate<T: DBObject>: Sendable {
	public let conditions: [DBCondition]

	public init(conditions: [DBCondition]) {
		self.conditions = conditions
	}
}

public extension String {
	/**
	True when both strings have the same letters and digits, ignoring case, diacritics,
	punctuation and whitespace ("Sams Club".almostEquals("Sam's Club") is true).

	Inside `#Predicate` this becomes an `.almostEqual` condition evaluated by SQLite:
	```swift
	#Predicate<Payee> { $0.name.almostEquals("Sams Club") }
	```
	*/
	func almostEquals(_ other: String) -> Bool {
		return AgileDB.alphanumericKey(self) == AgileDB.alphanumericKey(other)
	}
}
