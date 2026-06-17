# AgileDB Architecture

AgileDB is a key/value document store built on SQLite. Each entry is stored under a
string key in a named table, with the object body serialized as a JSON document in a
`value` column. Queries are expressed against the JSON document using SQLite's
`json_extract`/`json_each`, and selected properties can be promoted to indexed virtual
columns. The framework is fully `async`/`await` based and concurrency-safe under Swift 6.

This document summarizes the **public-facing API surface** of the framework and its
three principal supporting types: `DBObject`, `DBResults`, and `DBResultsPublisher`.

## Component overview

| Component | Role |
| --- | --- |
| `AgileDB` | The database actor. Owns the SQLite file, performs all reads/writes, manages tables, indexes, syncing, and publishers. |
| `DBObject` | Protocol your model types adopt to get type-safe `save`/`load`/`delete` and Codable-based (de)serialization, including nested objects. |
| `DBResults` | An `AsyncSequence` of `DBObject`s, backed by a list of keys that are loaded lazily on demand. |
| `DBResultsPublisher` | A Combine `Publisher` that emits `DBResults` and re-emits whenever the underlying query results change. |
| Public models | `DBTable`, `DBCondition`, `DBConditionOperator`, `DBRow`, `DBError`, `DBCommandToken`, and the `Result` typealiases in `PublicModels.swift`. |

## Storage model

Every table is created with the same physical schema:

```
create table <table> (
    key text PRIMARY KEY,
    autoDeleteDateTime text,
    addedDateTime text,
    updatedDateTime text,
    value text            -- the object body as a JSON document
)
```

- The `key` and the three date columns are real columns; **all other object properties
  live inside the `value` JSON document**.
- Writes are a single-row UPSERT (`on conflict(key) do update …`), making each save atomic.
- Conditions and sort orders are translated to `json_extract(value, '$.field')`
  expressions. The `contains` operator resolves at query time via `json_type`: array
  membership (`json_each`) for JSON arrays, substring `like` for JSON text.
- Declaring indexes (`setIndexesForTable`) adds a `VIRTUAL` generated column over
  `json_extract(value, '$.field')` and indexes it, giving both index-backed lookups and
  real column names usable from direct SQL.
- Reserved internal tables are prefixed with `__` (e.g. `__settings`, `__synclog`,
  `__unsyncedTables`) or `sqlite_stat`.

## Concurrency model

- `AgileDB` is an `actor`; its mutable state is isolated and accessed through `await`.
- All SQLite work is funneled to a single private `SQLiteCore` thread that serializes
  execution blocks on its own queue. The actor bridges to it with
  `withCheckedContinuation`. `SQLiteCore` is `nonisolated` within the actor because it
  manages its own concurrency.
- The file auto-closes after `autoCloseTimeout` seconds of inactivity and re-opens on the
  next operation. A value of `0` disables auto-close.

---

## `AgileDB` — public API

### Lifecycle & configuration

| Member | Description |
| --- | --- |
| `static let shared: AgileDB` | Shared singleton instance. |
| `init(fileLocation: URL? = nil, isDebugging: Bool = false)` | Create an instance, optionally with a custom file location. |
| `private(set) var dbFilePath: String?` | Path of the database file; `nil` until opened. |
| `var autoCloseTimeout: Int` | Seconds of inactivity before the file auto-closes. Default `2`; `0` = never. |
| `func setAutoCloseTimeout(_ timeout: Int)` | Setter usable across the actor boundary. |
| `var printSQL: Bool` | When `true`, prints generated SQL. |
| `static var dateFormatter: DateFormatter` | ISO-8601-style formatter used for all stored dates. |
| `func open(_ location: URL? = nil) async -> Bool` | Open the database file. `@discardableResult`. |
| `func close()` | Close the database. |

### Keys & existence

| Member | Description |
| --- | --- |
| `func tableHasKey(table:key:) async throws -> Bool` | Whether a table contains a key. |
| `func tableHasAllKeys(table:keys:) async throws -> Bool` | Whether a table contains all given keys. |
| `func tableHasKey(table:key:queue:completion:) -> DBCommandToken?` | Closure-based variant; returns a cancellation token. |
| `func tableHasAllKeys(table:keys:queue:completion:) -> DBCommandToken?` | Closure-based variant. |
| `func keysInTable(_:sortOrder:conditions:validateObjects:) async throws -> [String]` | Keys matching optional sort order and conditions. |
| `func keysInTable(_:sortOrder:conditions:validateObjects:queue:completion:) -> DBCommandToken?` | Closure-based variant. |
| `func hasTable(_:) async -> Bool` | Whether the table exists. |

### Indexing

| Member | Description |
| --- | --- |
| `func setIndexesForTable(_:to:) async -> BoolResults` | Declare which object properties to index. Index columns are materialized as virtual generated columns. |

### Set / get / delete values

| Member | Description |
| --- | --- |
| `func setValueInTable(_:for:to value: String,autoDeleteAfter:) async -> Bool` | Store a JSON string under a key, with optional expiration. |
| `func setValueInTable(_:for:to objectValues: [String: any Sendable],autoDeleteAfter:) async -> Bool` | Store a dictionary value under a key. |
| `func valueFromTable(_:for:) async throws -> String` | Stored value as a JSON string. |
| `func dictValueFromTable(_:for:) async throws -> [String: any Sendable]` | Stored value as a dictionary. |
| `func deleteFromTable(_:for:) async -> Bool` | Delete an entry by key. |
| `func dropTable(_:) async -> Bool` | Drop a table and its data. |
| `func dropAllTables() async -> Bool` | Drop all (non-reserved) tables. |

Setting a value with an `autoDeleteAfter` date causes a background timer to delete the
entry once that time passes. Successful writes/deletes notify any matching publishers.

### Direct SQL

| Member | Description |
| --- | --- |
| `func sqlSelect(_ sql: String) async throws -> [DBRow]` | Run a raw `select` and return rows. |
| `func sqlSelect(_:queue:completion:) -> DBCommandToken?` | Closure-based variant. |

### Reactive queries

| Member | Description |
| --- | --- |
| `func publisher<T>(sortOrder:conditions:validateObjects:) -> DBResultsPublisher<T>` | Create a Combine publisher of `DBResults<T>` for `T.table`. |

### Syncing

AgileDB supports multi-instance synchronization through exported/imported sync files. Each
database has a generated `instanceKey`, and changes are journaled in `__synclog`.

| Member | Description |
| --- | --- |
| `var isSyncingEnabled: Bool?` | Current syncing status; `nil` if not yet opened. |
| `func enableSyncing() async -> Bool` | Enable syncing (creates the sync log). |
| `func disableSyncing() async -> Bool` | Disable syncing. |
| `var unsyncedTables: [DBTable]` | Read-only list of tables excluded from sync. |
| `func setUnsyncedTables(_:) async -> Bool` | Set tables to exclude from sync. |
| `func createSyncFileAtURL(_:lastSequence:targetDBInstanceKey:) async -> (Bool, Int)` | Produce a sync file for another instance; returns success and the new last sequence. |
| `func processSyncFileAtURL(_:syncProgress:) async -> (Bool, String, Int)` | Apply a sync file; returns success, source instance key, and last sequence. |
| `typealias syncProgressUpdate = (Double) -> Void` | Progress callback type for sync processing. |

### Utilities

| Member | Description |
| --- | --- |
| `var instanceKey: String?` | This instance's unique key; `nil` if not opened. |
| `nonisolated func esc(_:) -> String` | Escape single quotes for inline SQL. |
| `static func stringValueForDate(_:) -> String` | Format a `Date` for storage. |
| `static func dateValueForString(_:) -> Date?` | Parse a stored date string. |

---

## `DBObject` — public protocol

`DBObject` is the type-safe modeling layer. Conform a `Codable, Sendable` type to it to
gain database persistence keyed by `key`.

```swift
public protocol DBObject: Codable, Sendable {
    static var table: DBTable { get }
    var key: String { get set }
    var codingKeys: [CodingKey] { get }   // default: [] (encode all properties)
}
```

### Protocol extension members

| Member | Description |
| --- | --- |
| `var codingKeys: [CodingKey]` | Defaults to `[]`, meaning all properties are encoded. |
| `init?(db: AgileDB, key: String) async` | Load an instance by key; `nil` if missing or undecodable. |
| `static func load(from: AgileDB, for key: String) async throws -> Self` | Load an instance by key; throws `DBError` on failure. |
| `func save(to: AgileDB, autoDeleteAfter: Date? = nil, saveNestedObjects: Bool = true) async -> Bool` | Persist the object (and, by default, nested `DBObject`s and arrays of them). |
| `func delete(from: AgileDB) async -> Bool` | Delete the object (does not delete nested objects). |
| `var jsonValue: String?` | Full JSON encoding of the object (dates use `AgileDB.dateFormatter`). |
| `var dictValue: [String: any Sendable]?` | Dictionary used for storage; **nested `DBObject`s are referenced by key only, not embedded**. |
| `static func loadObjectFromDB(_:for:queue:completion:) -> DBCommandToken?` | **Deprecated** — use `await load` instead. |

### Nested-object handling

Nested `DBObject`s are stored independently and referenced **only by key**. On load, the
custom `DBObjectDecoder`/`DBObjectEncoder` resolve nested references through a retry loop:
a decode pass that encounters a not-yet-loaded nested object records the miss and aborts;
the missing dictionaries are then fetched asynchronously from the database and cached, and
the decode is retried until it succeeds. This lets a synchronous `Codable` decode pull in
asynchronously-loaded nested objects.

---

## `DBResults` — public class

`DBResults<T: DBObject>` is a lazily-loaded, key-backed `AsyncSequence` of objects. It
stores only keys; each object is loaded from the database on demand.

```swift
public class DBResults<T: DBObject>: Identifiable, AsyncSequence {
    public typealias Element = T

    public let id: UUID
    public let keys: [String]

    public var count: Int { keys.count }
    public var isEmpty: Bool { keys.isEmpty }

    public init(db: AgileDB = .shared, keys: [String] = [])

    public func object(at index: Array<String>.Index) async -> T?
    public func makeAsyncIterator() -> AsyncIterator
}
```

| Member | Description |
| --- | --- |
| `id` | Stable identity (for SwiftUI / `Identifiable`). |
| `keys` | The ordered key list backing the results. |
| `count` / `isEmpty` | Number of keys / whether empty. |
| `object(at:) async -> T?` | Load the object at an index; `nil` if out of bounds or no longer loadable. |
| `AsyncSequence` conformance | Iterate with `for await object in results`. Keys that can no longer be loaded are skipped during iteration. |

Because loading is asynchronous and on-demand, `DBResults` holds a small footprint
regardless of result size — objects are materialized only as you iterate or index into them.

---

## `DBResultsPublisher` — public class

`DBResultsPublisher<T: DBObject>` is a Combine `Publisher` that emits `DBResults<T>` and
re-emits whenever the underlying query results change (after relevant saves, deletes, table
drops, or sync imports). Obtain one from `AgileDB.publisher(...)`.

```swift
public class DBResultsPublisher<T: DBObject>: Publisher, Identifiable, @unchecked Sendable {
    public typealias Output = DBResults<T>
    public typealias Failure = DBError

    public let id: UUID

    public func receive<S>(subscriber: S) where S: Subscriber,
        S.Failure == DBError, S.Input == DBResults<T>
}
```

Behavior:

- On the **first** subscription it kicks off an asynchronous fetch via
  `AgileDB.keysInTable(...)` using the publisher's stored `sortOrder`/`conditions`/
  `validateObjects`, then emits the resulting `DBResults<T>`.
- The internal `CurrentValueSubject`'s initial empty seed is dropped so subscribers receive
  the first *real* fetch result, not the placeholder.
- The owning `AgileDB` actor tracks live publishers and calls `updateSubject()` on those
  whose query includes a changed key, re-running the query and emitting fresh results.
- When the last subscription is cancelled (or on `deinit`), the publisher deregisters
  itself from the database.

This makes it straightforward to drive SwiftUI views: subscribe to a publisher and the view
updates automatically as matching data changes.

---

## Supporting public models (`PublicModels.swift`)

| Type | Description |
| --- | --- |
| `struct DBTable` | Identifies a table. `Equatable`, `Sendable`, `ExpressibleByStringLiteral`, `CustomStringConvertible`. Rejects empty/reserved names. |
| `struct DBCondition` | A query condition: `set` (OR-group page), `objectKey`, `conditionOperator`, and `value`. Conditions in the same `set` are AND'd; different sets are OR'd. |
| `enum DBConditionOperator: String` | `equal` `=`, `notEqual` `<>`, `lessThan` `<`, `greaterThan` `>`, `lessThanOrEqual` `<=`, `greaterThanOrEqual` `>=`, `contains` `...`, `inList` `()`. |
| `struct DBRow` | A raw query row: `values: [(any Sendable)?]`. |
| `struct DBCommandToken` | Returned by closure-based async methods; `cancel() -> Bool` removes the command before it executes. |
| `enum DBError` | `cannotWriteToFile`, `diskError`, `damagedFile`, `cannotOpenFile`, `tableNotFound`, `cannotParseData`, `other(Int)`. `RawRepresentable` by `Int`. |
| `typealias BoolResults` | `Result<Bool, DBError>` |
| `typealias KeyResults` | `Result<[String], DBError>` |
| `typealias RowResults` | `Result<[DBRow], DBError>` |
| `typealias JsonResults` | `Result<String, DBError>` |
| `typealias DictResults` | `Result<[String: any Sendable], DBError>` |

### Query condition semantics

`DBCondition.set` groups conditions into OR'd "pages":

- Conditions with the **same** `set` value are combined with **AND**.
- Different `set` values are combined with **OR**.

For example, `(a AND b) OR (c)` is expressed as two conditions with `set: 0` for `a`/`b`
and one condition with `set: 1` for `c`.
