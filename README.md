# AgileDB
- A low latency thread-safe key/value SQLite database wrapper written in Swift that requires no SQL knowledge to use.
- No need to keep track of columns used in the database; it's automatic. 
- Completely thread safe. AgileDB is implemented as a Swift `actor` and is fully Swift 6 concurrency compliant.
- Built around Async/Await
- Use the publisher method to work with Combine and SwiftUI

## Installation  ##
include AgileDB as a dependency in your Package.swift file:
```swift
dependencies: [
   .package(url: "https://github.com/AaronBratcher/AgileDB", from: "8.6.0")
]
```

## Getting Started ##
- The easiest way to use AgileDB is to create a class or struct that complies to the DBObject Protocol. These entities will automatically be `Codable`. Encoded values are saved to the database. (See below for supported value types)
- Alternately, you can use low level methods that work from a JSON dictionary. Supported types in the JSON node are String, Int, Double, Bool, [String], [Int], [Double].
## DBObject Protocol ##
- DBObjects can have the following types saved and read to the database: DBObject, Int, Double, String, Date, Bool, Dictionary, Codable Struct [DBObject], [Int], [Double], [String], [Date], [Dictionary], [Codable Struct]. All properties may be optional. For saved DBObject properties, the key is stored so the referenced objects can be edited and saved independently
- Bool properties read from the database will be interpreted as follows: An integer 0 = false and any other number is true. For string values "1", "yes", "YES", "true", and "TRUE" evaluate to true.

### Protocol Definition ###
```swift
public protocol DBObject: Codable, Sendable {
    static var table: DBTable { get }
    static var currentSchemaVersion: Int { get }
    var key: String { get set }
    var codingKeys: [CodingKey] { get }

    static func convertToCurrentSchema(_ dictValue: [String: any Sendable], from schemaVersion: Int) -> [String: any Sendable]
}
```
- `codingKeys` has a default implementation that returns an empty array, encoding all of the object's properties. Provide your own implementation to limit which properties are encoded.
- `currentSchemaVersion` defaults to `1`. `convertToCurrentSchema` has a default implementation that returns `dictValue` unchanged. See **Schema Versioning** below.

### Protocol methods ###
```swift
/**
 Instantiate object and populate with values from the database, recursively if necessary. If instantiation fails, nil is returned.

 - parameter db: Database object holding the data.
 - parameter key: Key of the data entry.
*/
public init?(db: AgileDB, key: String) async

/**
 Save the object's encoded values to the database.

 - parameter db: Database object to hold the data.
 - parameter expiration: Optional Date specifying when the data is to be automatically deleted. Default value is nil specifying no automatic deletion.
 - parameter saveNestedObjects: Save nested DBObjects and arrays of DBObjects. Default value is true.

 - returns: Discardable Bool value of a successful save.
*/
@discardableResult
public func save(to db: AgileDB, autoDeleteAfter expiration: Date? = nil, saveNestedObjects: Bool = true) async -> Bool

/**
 Remove the object from the database.

 - parameter db: Database object that holds the data.
 - parameter cascadeDelete: Also delete referenced objects that would otherwise be left orphaned, but only if nothing else still references them. Default value is false.

 - returns: DBDeleteResult indicating whether the delete failed outright, completed in full, or completed but left some still-referenced objects in place.
*/
@discardableResult
public func delete(from db: AgileDB, cascadeDelete: Bool = false) async -> DBDeleteResult

/**
 Asynchronously instantiate object and populate with values from the database, recursively if necessary.

 - parameter db: Database object to hold the data.
 - parameter key: Key of the data entry.

 - returns: DBObject
 - throws: DBError
*/
public static func load(from db: AgileDB, for key: String) async throws -> Self
```

### Sample Struct ###
```swift
import AgileDB

enum Table {
    static let categories: DBTable = "Categories"
    static let accounts: DBTable = "Accounts"
    static let people: DBTable = "People"
}

struct Category: DBObject {
    static var table: DBTable { return Table.categories }
    var key = UUID().uuidString
    var accountKey = ""
    var name = ""
    var inSummary = true
}

// save to database
await category.save(to: db)

// save to database, automatically delete after designated date
await category.save(to: db, autoDeleteAfter: deletionDate)

// instantiate and pull from DB asynchronously
guard let category = await Category(db: db, key: categoryKey) else { return }

// delete from DB
await category.delete(from: db)

```

### Supported Property Types ###
Most `Codable` property types just work. A few are given special storage so they round-trip cleanly through the JSON document each row is stored as:

| Type | Stored as |
| --- | --- |
| `Bool`, `Int`, `Int8`/`16`/`32`, `Double`, `String` | Native value |
| `Date`, `[Date]` | String(s), formatted consistently regardless of locale |
| `Data`, `[Data]` | Base64 string(s) |
| A `DBObject` property, or `[DBObject]` | Reference(s) by key - see "Object References & Cascade Delete" below |

```swift
struct Receipt: DBObject {
    var key = UUID().uuidString
    var note = ""
    var photo: Data?
    var thumbnails: [Data] = []
}
```

Anything else Codable falls back to a generic JSON encoding, stored as a string.

### Schema Versioning ###
Every save stamps the stored dictionary with the type's `currentSchemaVersion` (default 1). When loading data saved under an older version, `convertToCurrentSchema` is called automatically with the raw stored dictionary and the version it was saved with, and its return value is what actually gets decoded — including for nested `DBObject`s loaded as part of a parent.

```swift
struct Account: DBObject {
    static var table: DBTable { Table.accounts }
    static var currentSchemaVersion: Int { 2 }

    var key = UUID().uuidString
    var name = ""
    var isActive = true // added in schema version 2

    static func convertToCurrentSchema(_ dictValue: [String: any Sendable], from schemaVersion: Int) -> [String: any Sendable] {
        var dictValue = dictValue
        if schemaVersion < 2 {
            dictValue["isActive"] = true // rows saved before version 2 default to active
        }
        return dictValue
    }
}
```

### Object References & Cascade Delete ###
Every `save` records which other `DBObject`s this object's nested `DBObject`/`[DBObject]` properties currently point to, and updates a reverse-lookup ("who references me") on each of those objects. This bookkeeping is local to the database instance — it's not part of the JSON `value` document, not synced between instances, and invisible to your model's `Codable` conformance.

`delete(from:cascadeDelete:)` uses that bookkeeping: with `cascadeDelete: true`, deleting an object also deletes any object it referenced that has no other referrers, walking recursively through the reference graph (a shared reference cycle is deleted as a whole once nothing outside the cycle still points into it). An object that's still referenced by something else is left in place instead.

```swift
struct Client: DBObject {
    static var table: DBTable { Table.clients }
    var key = UUID().uuidString
    var name = ""
}

struct Invoice: DBObject {
    static var table: DBTable { Table.invoices }
    var key = UUID().uuidString
    var client: Client
}

// invoice1 and invoice2 both reference the same client
let result = await invoice1.delete(from: db, cascadeDelete: true)
switch result {
case .completed:
    break // invoice1 deleted; client also deleted if invoice1 was its only referrer
case .partial(let retained):
    break // invoice1 deleted, but `retained` (e.g. [DBReference(table: Table.clients, key: client.key)]) is still referenced by invoice2, so it was left alone
case .failed:
    break // invoice1's own row could not be deleted
}
```
This walk isn't atomic: if the app is interrupted partway through a cascade, some now-unreferenced objects may be left behind rather than deleted.

### All macros are currently in beta ###

## Model Macro ##
`@Model` must be attached to a `class`, not a `struct` — it also makes the type `Observable`, and `Observable` (Swift's `Observation` framework) only works on reference types. It generates the `DBObject` boilerplate shown above (`DBObject` conformance, the `key` property if not already declared, `static var table`, `init(from:)`, and `encode(to:)`), plus the `Observable` machinery — an `ObservationRegistrar` and `@ObservationTracked` on eligible stored properties — so instances participate in SwiftUI observation like any other `@Observable` class. Mark any properties that shouldn't be persisted with `@Transient`; everything else is included.

```swift
@Model(table: Table.categories)
final class Category: @unchecked Sendable {
    var accountKey: String = ""
    var name: String = ""
    var inSummary: Bool = true
    @Transient var isNew: Bool = true // not saved to the database
}
```
generates the same boilerplate as the hand-written `Category` above (plus `Observable` conformance), while excluding `isNew` from what's persisted.

**Every property that will be persisted must have an explicit type annotation** (`var name: String = ""`, not `var name = ""`). The macro generates `init(from:)`/`encode(to:)` itself rather than relying on the compiler's `Codable` synthesis — `@ObservationTracked` turns stored properties into computed ones, which disables that synthesis — and macros expand before type-checking, so there's no way to infer a property's type from its initializer expression the way the compiler can. A property without an explicit type annotation is a compile-time error.

`@Transient` properties don't need to be Optional, but they are skipped entirely in the generated `init(from:)`/`encode(to:)`, so a non-optional `@Transient` property must have a default value (`= true`, `= 0`, `= ""`, etc.) to keep it after decoding — without one, the type won't compile as `Decodable`.

The `table` argument is optional. If omitted, the table name is derived from the type's own name:
```swift
@Model
final class Widget: @unchecked Sendable {
    var name: String = ""
}
// Widget.table == DBTable(name: "Widget")
```
Since table names are effectively part of the on-disk schema, prefer passing `table:` explicitly for any model where the type name might later change, or where the table already exists under a different name.

## Predicate Macro ##
`#Predicate<T> { ... }` builds a `DBPredicate<T>` — a typed wrapper around `[DBCondition]` — from a single-expression closure, the same way SwiftData's `#Predicate` builds a `Predicate<T>`.

```swift
let predicate = #Predicate<Account> { $0.type == .checking && $0.balance > 0 }
// predicate.conditions is a [DBCondition] usable with keysInTable, countKeysInTable, or publisher
```

Supported inside the closure:
- Comparisons `==`, `!=`, `<`, `>`, `<=`, `>=` between a property path (`$0.property` or `$0.nested.property`) and a value, in either order
- `$0.property.contains(value)` for array/string properties (`.contains`)
- `array.contains($0.property)` for membership checks (`.inList`)
- `$0.property.almostEquals(value)` (or `value.almostEquals($0.property)`) for fuzzy string equality (`.almostEqual`) — see **Condition Operators** below
- `&&` and `||` combining any number of the above, including mixed nesting — each `&&`-joined group becomes one condition `set` (ANDed), and `||` starts a new set (ORed), matching `DBCondition`'s own set-based semantics

```swift
// (type == .checking || type == .savings) && balance > 0
let predicate = #Predicate<Account> {
    ($0.type == .checking || $0.type == .savings) && $0.balance > 0
}
```

`#Predicate` only understands this specific set of forms — it doesn't evaluate the closure or resolve types, since it runs entirely at compile time over the closure's syntax. Anything else (`!`, `??`, multi-statement closures, arbitrary function calls) produces a compile-time error rather than unexpected runtime behavior.

## Query Property Wrapper ##
`@Query` fetches `DBObject`s and keeps the result current — modeled directly on SwiftData's `@Query`, right down to reading its database from `@Environment(\.modelContext).`

```swift
@main
struct MyApp: App {
    var body: some Scene {
        WindowGroup {
            ContentView()
                .environment(\.modelContext, AgileDB.shared)
        }
    }
}

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

- Set the database once with `.environment(\.modelContext, myDB)` on a parent view; every `@Query` beneath it reads from that value. If it's never set, `@Query` falls back to `AgileDB.shared`.
- `filter` takes a `DBPredicate<T>` built with `#Predicate`; omit it to fetch everything in the table.
- `sort` is the same comma-delimited property-name string used elsewhere in AgileDB (e.g. `"name"` or `"date desc"`), rather than SwiftData's `SortDescriptor`.
- Results start empty and load asynchronously, then refresh automatically both once the initial load completes and whenever the underlying table changes — it's built directly on `publisher()`, so it shares that same live-update behavior.

`@Query` is a `DynamicProperty` backed by `@StateObject` and `@Environment`, so — exactly like SwiftData's `@Query` — it only resolves its database and establishes its live-updating identity once it's a stored property of a `View` that SwiftUI is actually rendering; it isn't meaningfully usable or testable by constructing it directly outside of one.

## DBResults Class
- Works with DBObject elements
- Instantiate the class with a reference to the database and the keys
- Only keys are stored to minimize memory usage
- Objects are loaded asynchronously on demand, either with the `object(at:)` method or by iterating the results as an `AsyncSequence` with `for await`
- The database publisher returns an instance of this class

### Usage ###
```swift
let keys = try await db.keysInTable(Category.table)

let categories = DBResults<Category>(db: db, keys: keys)

// Iterate as an AsyncSequence (objects load on demand):
for await category in categories {
    // use category object
}

// Or load a specific object by index:
guard let first = await categories.object(at: 0) else { return }
```

## Publisher ##
- Use the publisher with Combine subscribers and SwiftUI
- Sends DBResults as needed to reflect finished queries and updated results
- Uses completion to send possible errors

### Usage ###
```swift
/**
Returns a  Publisher for generic DBResults. Uses the table of the DBObject for results.

- parameter sortOrder: Optional string that gives a comma delimited list of properties to sort by.
- parameter conditions: Optional array of DBConditions that specify what conditions must be met.

- returns: DBResultssPublisher
*/

let publisher: DBResultsPublisher<Transaction> = db.publisher()
let _ = publisher.sink(receiveCompletion: { _ in }) { ( results) in
// assign to AnyCancellable property
}
```

## Auto-Close & Reopen ##
- The database file automatically closes after `autoCloseTimeout` seconds of inactivity (default `2`; `0` disables it) and reopens transparently on the next operation.
- If reopening fails right after an auto-close, AgileDB retries a few times with a short, increasing delay before giving up — this covers transient situations such as the host machine having just woken from sleep, where the volume can take a moment to become available again.
- If every retry fails, `onDatabaseUnavailable` is called so your app can let the user know, instead of the operation silently stalling:
```swift
db.onDatabaseUnavailable = {
    // called from a background thread; hop to the main actor before touching UI state
    print("AgileDB couldn't reopen its database file")
}
```
- AgileDB never posts a `Notification` for this itself — `onDatabaseUnavailable` is a plain closure so the choice of how to surface it (a `NotificationCenter` post, a delegate call, direct UI state, etc.) is left to your app.

## Low level methods ##

### Keys ###

See if a given table holds a given key.
```swift
let table: DBTable = "categories"
do {
    let hasKey = try await AgileDB.shared.tableHasKey(table: table, key: "category1")
    if hasKey {
        // table has key
    } else {
        // table didn't have key
    }
} catch {
    // handle error
}
```

See if a given table holds all given keys.
```swift
let table: DBTable = "categories"
do {
    let hasKeys = try await AgileDB.shared.tableHasAllKeys(table: table, keys: ["category1","category2","category3"])
    if hasKeys {
        // table has all keys
    } else {
        // table didn't have all keys
    }
} catch {
    // handle error
}
```

Return an array of keys in a given table. Optionally specify sort order based on a value at the root level
```swift
let table: DBTable = "categories"
do {
    let tableKeys = try await AgileDB.shared.keysInTable(table, sortOrder: "name, date desc")
    // process keys
} catch {
    // handle error
}
```

Return an array of keys from the given table sorted in the way specified matching the given conditions.
```swift
/**
All conditions in the same set are ANDed together. Separate sets are ORed against each other.  (set:0 AND set:0 AND set:0) OR (set:1 AND set:1 AND set:1) OR (set:2)

Unsorted Example:

let accountCondition = DBCondition(set:0,objectKey:"account",conditionOperator:.equal, value:"ACCT1")
do {
	let keys = try await AgileDB.shared.keysInTable("table1", sortOrder: nil, conditions: [accountCondition])
	// use keys
} catch {
	// handle error
}

- parameter table: The DBTable to return keys from.
- parameter sortOrder: Optional string that gives a comma delimited list of properties to sort by.
- parameter conditions: Optional array of DBConditions that specify what conditions must be met.

- returns: [String] Returns an array of keys from the table.
- throws: DBError when the database could not be opened or another error occurred.

public func keysInTable(_ table: DBTable, sortOrder: String? = nil, conditions: [DBCondition]? = nil) async throws -> [String]
*/


let table: DBTable = "accounts"
let accountCondition = DBCondition(set:0,objectKey:"account", conditionOperator:.equal, value:"ACCT1")
do {
    let keys = try await AgileDB.shared.keysInTable(table, sortOrder: nil, conditions: [accountCondition])
    // process keys
} catch {
    // handle error
}
```

Return the count of keys in a given table instead of loading the keys themselves. Accepts the same `conditions` parameter as `keysInTable.`
```swift
let table: DBTable = "accounts"
do {
    let count = try await AgileDB.shared.countKeysInTable(table)
    // use count
} catch {
    // handle error
}
```

Count keys matching conditions.
```swift
let table: DBTable = "accounts"
let accountCondition = DBCondition(set:0,objectKey:"account", conditionOperator:.equal, value:"ACCT1")
do {
    let count = try await AgileDB.shared.countKeysInTable(table, conditions: [accountCondition])
    // use count
} catch {
    // handle error
}
```

A completion closure-based version is also available. Since AgileDB is an actor, the call itself still requires `await`, but the count is delivered to the completion closure rather than being awaited directly; a `DBCommandToken` is returned so the command can be cancelled before it executes.
```swift
let table: DBTable = "accounts"
let token = await AgileDB.shared.countKeysInTable(table) { results in
    switch results {
    case .success(let count):
        // use count
    case .failure(let error):
        // handle error
    }
}
```



### Condition Operators ###

| Operator | Raw value | Matches when the property… |
| --- | --- | --- |
| `.equal` | `=` | equals the value |
| `.notEqual` | `<>` | does not equal the value |
| `.lessThan` / `.greaterThan` | `<` / `>` | compares less / greater |
| `.lessThanOrEqual` / `.greaterThanOrEqual` | `<=` / `>=` | compares less-or-equal / greater-or-equal |
| `.contains` | `...` | is an array holding the value, or text containing the value as a substring |
| `.inList` | `()` | equals one of the values in the given array |
| `.almostEqual` | `~=` | has the same letters and digits as the value, ignoring case, diacritics, punctuation and whitespace |

`.almostEqual` is for fuzzy matching of user-typed text: searching for "Sams Club" finds "Sam's Club", "SAMS-CLUB" and "sams club", but not "Sam's Club Gas" (it is equality, not substring). Both sides are reduced to lowercase alphanumerics by `AgileDB.alphanumericKey(_:)`; the same comparison is available in Swift as `String.almostEquals(_:)`.
```swift
let payeeCondition = DBCondition(set: 0, objectKey: "name", conditionOperator: .almostEqual, value: "Sams Club")
let keys = try await AgileDB.shared.keysInTable("payees", conditions: [payeeCondition])

// or, with the macro
let predicate = #Predicate<Payee> { $0.name.almostEquals(searchText) }
```
`.almostEqual` is evaluated by a SQL function over every row's value, so it can't use an index declared with `setIndexesForTable`.

### Values ###
Data can be set or retrieved manually as shown here or your class/struct can conform to the DBObject protocol, documented above, and use the built-in init and save methods for greater ease and flexibility.

Set value in table
```swift
let table: DBTable = "Transactions"
let key = UUID().uuidString
let dict = [
    "key": key
    , "accountKey": "Checking"
    , "locationKey" :"Kroger"
    , "categoryKey": "Food"
]

let data = try! JSONSerialization.data(withJSONObject: dict, options: .prettyPrinted)
let json = String(data: data, encoding: .utf8)!
// the key object in the json value is ignored in the setValue method
if await AgileDB.shared.setValueInTable(table, for: key, to: json) {
    // success
} else {
    // handle error
}
```

Retrieve value for a given key
```swift
let table: DBTable = "categories"
do {
    let jsonValue = try await AgileDB.shared.valueFromTable(table, for: "category1")
    // process value
} catch {
    // handle error
}

do {
    let dictValue = try await AgileDB.shared.dictValueFromTable(table, for: "category1")
    // process dictionary value
} catch {
    // handle error
}
```

Delete the value for a given key
```swift
let table: DBTable = "categories"
if await AgileDB.shared.deleteFromTable(table, for: "category1") {
    // value was deleted
} else {
    // handle error
}
```

## Retrieving Data Asynchronously ##
AgileDB is an `actor`, so data is retrieved asynchronously using `await`. Methods that can fail are marked `throws`.

```swift
let db = AgileDB.shared
let table: DBTable = "categories"

do {
   let value = try await db.valueFromTable(table, for: key)
	
} catch {
}
```

*Asynchronous methods available*
- tableHasKey
- tableHasAllKeys
- keysInTable
- countKeysInTable
- valueFromTable
- dictValueFromTable
- sqlSelect
- load in the DBObject protocol

## SQL Queries ##
AgileDB lets you run standard SQL `select` statements for more complex queries — joins, aggregates, grouping, and so on. `sqlSelect` returns an array of rows (each row an array of values), or throws on error.

Each object is stored as a single JSON document in its table's `value` column, alongside the `key`, `addedDateTime`, `updatedDateTime`, and `autoDeleteDateTime` columns. Individual object properties are therefore **not** their own columns. To reference a property as if it were a column, use SQLite's `json_extract(value, '$.propertyName')`:

```swift
let db = AgileDB.shared
// "account" is a property inside each row's JSON document, not a physical column,
// so it must be addressed with json_extract.
let sql = "select key from accounts where json_extract(value, '$.account') = 'ACCT1'"
do {
    let results = try await db.sqlSelect(sql)
    // process results
} catch {
    // handle error
}
```

`key`, `addedDateTime`, `updatedDateTime`, and `autoDeleteDateTime` are real columns and may be referenced directly by name.

### Joining related objects ###
A nested `DBObject` property stores the referenced object's `key`, and a `[DBObject]` property stores an array of keys. Join them back to their tables using `json_extract` (for a single reference) and `json_each` (to expand an array of keys):

```swift
// Invoice -> Client (single reference): the invoice's "client" property holds the client's key.
let clientJoin = """
select json_extract(c.value, '$.name')
from Invoice i
join InvoiceClient c on json_extract(i.value, '$.client') = c.key
where json_extract(i.value, '$.number') = 1001
"""

// Invoice -> line items (array of keys): expand the array with json_each, then join by key.
let itemTotals = """
select json_extract(c.value, '$.name'),
       sum(json_extract(item.value, '$.quantity') * json_extract(item.value, '$.price'))
from Invoice i
join InvoiceClient c on json_extract(i.value, '$.client') = c.key,
     json_each(i.value, '$.lineItems') je
join InvoiceLineItem item on je.value = item.key
group by i.key
"""
```

### Indexing properties as named columns ###
To query a property by name directly (and to index it for performance), declare it with `setIndexesForTable(_:to:)`. Each indexed property becomes an indexed, generated column derived from the JSON document, so it can be used by name in SQL:

```swift
await db.setIndexesForTable("accounts", to: ["account"])
// "account" is now a real, indexed column and can be referenced directly.
let results = try await db.sqlSelect("select key from accounts where account = 'ACCT1'")
```

## Syncing ##
AgileDB can sync with other instances of itself by enabling syncing before processing any data and then sharing a sync log.

```swift
/**
Enables syncing. Once enabled, a log is created for all current values in the tables.

- returns: Bool If syncing was successfully enabled.
*/
public func enableSyncing() async -> Bool


/**
Disables syncing.

- returns: Bool If syncing was successfully disabled.
*/
public func disableSyncing() async -> Bool
    

/**
Read-only array of unsynced tables. Any tables not in this array will be synced.
*/
private(set) public var unsyncedTables: [DBTable]

/**
Sets the tables that are not to be synced.

- parameter tables: Array of tables that are not to be synced.

- returns: Bool If list was set successfully.
*/
public func setUnsyncedTables(_ tables: [DBTable]) async -> Bool


/**
Creates a sync file that can be used on another AgileDB instance to sync data. This is a synchronous call.

- parameter filePath: The full path, including the file itself, to be used for the log file.
- parameter lastSequence: The last sequence used for the given target  Initial sequence is 0.
- parameter targetDBInstanceKey: The dbInstanceKey of the target database. Use the dbInstanceKey method to get the DB's instanceKey.

- returns: (Bool,Int) If the file was successfully created and the lastSequence that should be used in subsequent calls to this instance for the given targetDBInstanceKey.
*/
public func createSyncFileAtURL(_ localURL: URL!, lastSequence: Int, targetDBInstanceKey: String) async -> (Bool, Int)


/**
Processes a sync file created by another instance of AgileDB This is a synchronous call.

- parameter filePath: The path to the sync file.
- parameter syncProgress: Optional function that will be called periodically giving the percent complete.

- returns: (Bool,String,Int)  If the sync file was successfully processed,the instanceKey of the submiting DB, and the lastSequence that should be used in subsequent calls to the createSyncFile method of the instance that was used to create this file. If the database couldn't be opened or syncing hasn't been enabled, then the instanceKey will be empty and the lastSequence will be equal to zero.
*/
public typealias syncProgressUpdate = (_ percentComplete: Double) -> Void
public func processSyncFileAtURL(_ localURL: URL!, syncProgress: syncProgressUpdate?) async -> (Bool, String, Int)
```    
    
# Revision History
### 8.6 ###
- New `.almostEqual` (`~=`) condition operator for fuzzy string matching on letters and digits only, ignoring case, diacritics, punctuation and whitespace ("Sams Club" finds "Sam's Club"). Available in `#Predicate` as `$0.property.almostEquals(value)`, and in Swift as `String.almostEquals(_:)` / `AgileDB.alphanumericKey(_:)`.

### 8.5 ###
- DBObjects now support Data and [Data] types.

### 8.4 ###
- Reopening the database file after an auto-close now retries a few times with a short, increasing delay instead of crashing (`fatalError`) on the very first failure — a failure right after a system wake is usually transient and clears within a second or two.
- New `onDatabaseUnavailable: (@Sendable () -> Void)?` property, called if reopening still fails after those retries. See **Auto-Close & Reopen** above.

### 8.3 ###
- `@Model` is now class-only; attaching it to a `struct` is a compile-time error. The macro also conforms the type to `Observable` and adds `@ObservationTracked` to eligible stored properties, so instances participate in SwiftUI observation.
- `@Model` now generates `init(from:)`/`encode(to:)` itself instead of relying on the compiler's `Codable` synthesis (which `@ObservationTracked`'s computed properties disable). **Every property that will be persisted must have an explicit type annotation** as a result — macros can't infer a type from an initializer expression the way the compiler can.

### 8.2 ###
- DBObject now tracks references to other DBObjects made via nested `DBObject`/`[DBObject]` properties, updated on every `save`. This bookkeeping is local to the database instance and not synced.
- `delete(from:cascadeDelete:)` gained a `cascadeDelete` parameter: when true, referenced objects with no other referrers are deleted along with the object, recursively. Returns a new `DBDeleteResult` (`.failed`, `.completed`, or `.partial(retained:)`) in place of the previous `Bool`.

### 8.1 ###
- DBObject gained schema versioning: `currentSchemaVersion` (default `1`) and `convertToCurrentSchema(_:from:)` (default: returns the dictionary unchanged). When loading data saved under an older version, `convertToCurrentSchema` runs automatically, for both top-level and nested objects.

### 8.0 ###
- New `@Query` property wrapper and `#Predicate<T> { ... }` macro, modeled on SwiftData's, for fetching and filtering `DBObject`s in SwiftUI; set the database once via `.environment(\.modelContext, myDB)`.
- New `@Model` macro generates `DBObject` conformance, `key`, `table`, and `codingKeys` for a class or struct; pair with `@Transient` on individual properties to exclude them from persistence.
- Removed the `validateObjects` parameter from `keysInTable`, `countKeysInTable`, and `publisher`. Since 7.0's move to JSON-document storage there was no table schema left to validate condition sets against, so the parameter had been silently ignored; it's now gone from the API entirely.

### 7.0 ###
- New method: `countKeysInTable`, with async/await and completion closure variants, returns the count of keys in a table matching the given conditions without loading the keys themselves.
- Objects are now stored as a single JSON document in each table's `value` column (rather than one physical column per property plus a side table for arrays). Direct SQL `select` statements must reference properties with `json_extract(value, '$.property')`; use `json_each` to join across array-of-key relationships, or declare indexes with `setIndexesForTable(_:to:)` to expose properties as named, indexed columns.
- AgileDB is now implemented as a Swift `actor` and is fully Swift 6 language-mode compliant.
- Asynchronous (async/await) methods are now the primary API. Most low-level methods are `async`, and those that can fail are `async throws`.
- Dictionary values now use `any Sendable` rather than `AnyObject`.
- DBObject now conforms to `Sendable` and exposes a `codingKeys` property (defaults to encoding all properties).
- DBObject `save` gained a `saveNestedObjects` parameter (default true).
- DBResults loads objects on demand and conforms to `AsyncSequence`; iterate it with `for await` or load a single object with the asynchronous `object(at:)` method.
- `unsyncedTables` and `setUnsyncedTables(_:)` now use `[DBTable]`.
- CocoaPods support removed; install via Swift Package Manager.
- Minimum platforms raised (iOS 18, macOS 12, tvOS 18, watchOS 9); built with the Swift 6.2 toolchain.

### 6.5 ###
- All DBObject methods have async/await counterparts
- Developed and tested with Xcode 14.2

### 6.4 ###
- Updated DBObject encoding to support Dictonary, Codable Struct, [Dictionary], and [Codable Struct]
- Converted project to proper Swift Package

### 6.3 ###
- Minimum swift version updated to 5.5
- Added async/await support

### 6.2 ###
- New method: tableHasAllKeys and it's asynchronous equivilent

### 6.1 ###
- DBObjects can recursively save and load DBObject and [DBObject] properties (Technically the key is stored so the referenced objects can be edited and saved independently)

### 6.0 ###
- New publisher method for use in SwiftUI and Combine. The publisher returns the new DBResults object. All active publishers will send new results if published DBResults has added, deleted, or updated keys.
- New DBResults object that's subscripted. Only the keys are stored for better memory use.
- DBObject structures can now store [String], [Int], [Double], and [Date] types. (Nested objects not supported.)

### 5.1 ###
- Developed and tested with Xcode 10.2 using Swift 5
- Introduction of DBObject protocol. See below.
- debugMode property renamed to isDebugging.
- `key` object in value provided is now ignored instead of giving error.
- New parameter `validateObjects` in the keysInTable method will ignore condition sets that refer to objects not in the table.

### 5.0 ###
- Developed and tested with Xcode 10.1
- Several methods deprecated with a renamed version available for clarity at the point of use.
- Data can be retrieved asynchronously.
- The class property `sharedInstance` has been renamed to `shared`.
- Methods are no longer class-level, they must be accessed through an instance of the db. A simple way to update to this is to simply append .shared to the class name in any existing code.
