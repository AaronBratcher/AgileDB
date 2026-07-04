//
//  Query.swift
//  AgileDB
//
//  Created by Aaron Bratcher on 7/4/26.
//

#if canImport(SwiftUI)
import SwiftUI
import Combine

private struct AgileDBEnvironmentKey: EnvironmentKey {
	static let defaultValue: AgileDB = .shared
}

extension EnvironmentValues {
	/// The `AgileDB` instance `@Query` reads from. Set it with `.environment(\.modelContext, myDB)`
	/// somewhere above any view using `@Query`; defaults to `AgileDB.shared` if never set.
	public var modelContext: AgileDB {
		get { self[AgileDBEnvironmentKey.self] }
		set { self[AgileDBEnvironmentKey.self] = newValue }
	}
}

/**
Fetches `DBObject`s and keeps the result up to date, for use as a SwiftUI property wrapper —
modeled on SwiftData's `@Query`. The database is read from `@Environment(\.modelContext)`
rather than passed in directly; set it once with `.environment(\.modelContext, myDB)` above
any view using `@Query`.

```swift
ContentView()
    .environment(\.modelContext, AgileDB.shared)

struct ContentView: View {
    @Query(filter: #Predicate<Account> { $0.balance > 0 }, sort: "name")
    var accounts: [Account]

    var body: some View {
        List(accounts) { account in
            Text(account.name)
        }
    }
}
```

Results start empty and are loaded asynchronously; the view refreshes automatically both
once the initial load completes and whenever the underlying table changes, the same way
`publisher()` already does.
*/
@available(iOS 13.0, macOS 10.15, tvOS 13.0, watchOS 6.0, *)
@propertyWrapper
public struct Query<T: DBObject>: DynamicProperty, @unchecked Sendable {
	@Environment(\.modelContext) private var db: AgileDB
	@StateObject private var box = QueryBox<T>()

	private let conditions: [DBCondition]?
	private let sortOrder: String?
	private let validateObjects: Bool

	@MainActor
	public init(filter: DBPredicate<T>? = nil, sort sortOrder: String? = nil, validateObjects: Bool = false) {
		self.conditions = filter?.conditions
		self.sortOrder = sortOrder
		self.validateObjects = validateObjects
	}

	@MainActor
	public var wrappedValue: [T] {
		box.objects
	}

	public var projectedValue: Query<T> {
		self
	}

	// DynamicProperty.update() is a nonisolated protocol requirement, but SwiftUI only ever
	// calls it from the main thread; assumeIsolated bridges synchronously into the
	// MainActor-isolated box and @Environment value on that guarantee.
	public nonisolated func update() {
		MainActor.assumeIsolated {
			box.configure(db: db, conditions: conditions, sortOrder: sortOrder, validateObjects: validateObjects)
		}
	}
}

@available(iOS 13.0, macOS 10.15, tvOS 13.0, watchOS 6.0, *)
@MainActor
private final class QueryBox<T: DBObject>: ObservableObject, @unchecked Sendable {
	@Published fileprivate var objects: [T] = []
	private var cancellable: AnyCancellable?

	/// `update()` is called before every body evaluation, so this only does real work once —
	/// on the first call, once the environment's `db` is actually available.
	func configure(db: AgileDB, conditions: [DBCondition]?, sortOrder: String?, validateObjects: Bool) {
		guard cancellable == nil else { return }

		// A placeholder subscription so a second `configure` call within the same run loop
		// turn (before the Task below has run) doesn't start a duplicate fetch.
		cancellable = AnyCancellable {}

		Task { [weak self] in
			let publisher: DBResultsPublisher<T> = await db.publisher(sortOrder: sortOrder, conditions: conditions, validateObjects: validateObjects)
			guard let self else { return }

			self.cancellable = publisher.sink(
				receiveCompletion: { _ in },
				receiveValue: { [weak self] results in
					guard let self else { return }
					Task { @MainActor in
						var loaded: [T] = []
						for await object in results {
							loaded.append(object)
						}
						self.objects = loaded
					}
				}
			)
		}
	}
}
#endif
