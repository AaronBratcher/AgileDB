# AgileDB
[![CocoaPods](https://img.shields.io/cocoapods/v/AgileDB.svg)](https://cocoapods.org/)

- A SQLite database wrapper written in Swift that requires no SQL knowledge to use.
- No need to keep track of columns used in the database; it's automatic.
- Completely thread safe since it uses its own Thread subclass.
- Works with Async/Await
- Use the publish method to work with Combine and SwiftUI

## Installation Options ##
- Swift Package Manager (Recommended)
- Cocoapods `pod AgileDB`
- Include all .swift source files in your project

## Getting Started ##
- The easiest way to use AgileDB is to create a class or struct that adheres to the DBObject Protocol. These entities will automatically be `Codable`. Encoded values are saved to the database. (See below for supported value types)
- Alternately, you can use low level methods that work from a JSON dictionary. Supported types in the JSON node are String, Int, Double, Bool, [String], [Int], [Double].
- For any method that returns an optional, that value is nil if an error occured and could not return a proper value.

## DBObject Protocol ##
- DBbjects can have the following types saved and read to the DB: DBObject, Int, Double, String, Date, Bool, Dictionary, Codable Struct [DBObject], [Int], [Double], [String], [Date], [Dictionary], [Codable Struct] (Dictionary and Struct and array alternatives saved as JSON text in DB column). All properties may be optional. For DBObject properties, the key is stored so the referenced objects can be edited and saved independently
- Bool properties read from the database will be interpreted as follows: An integer 0 = false and any other number is true. For string values "1", "yes", "YES", "true", and "TRUE" evaluate to true.

### Protocol Definition ###
```swift
public protocol DBObject: Codable {
    static var table: DBTable { get }
    var key: String { get set }
}
```

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

 - returns: Discardable Bool value of a successful save.
*/
@discardableResult
public func save(to db: AgileDB, autoDeleteAfter expiration: Date? = nil) async -> Bool

/**
 Remove the object from the database

 - parameter db: Database object that holds the data. This does not delete nested objects.
 - returns: Discardable Bool value of a successful deletion.
*/
public func delete(from db: AgileDB) async -> Bool

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

enum Table: String {
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
category.save(to: db)

// save to database, automatically delete after designated date
category.save(to: db, autoDeleteAfter: deletionDate)

// instantiate synchronously
guard let category = Category(db: db, key: categoryKey) else { return }

// instantiate asynchronously
do {
    let category = try await Category.load(from: db, for: categoryKey)
    // use category
} catch {
}
```

## DBResults Class
- Works with DBObject elements
- Instantiate the class with a reference to the database and the keys
- Only keys are stored to minimize memory usage
- The database publisher returns an instance of this class

### Usage ###
```swift
guard let keys = db.keysInTable(Category.table) else { return }

let categories = DBResults<Category>(db: db, keys: keys)
    
for category in categories {
    // use category object
}
    
for index in 0..<categories.count {
    let category = categories[index]
    // use category object
}
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
- parameter validateObjects: Optional bool that condition sets will be validated against the table. Any set that refers to json objects that do not exist in the table will be ignored. Default value is false.

- returns: DBResultssPublisher
*/

let publisher: DBResultsPublisher<Transaction> = db.publisher()
let _ = publisher.sink(receiveCompletion: { _ in }) { ( results) in
// assign to AnyCancellable property
}
```

## Low level methods ##

### Keys ###

See if a given table holds a given key.
```swift
let table: DBTable = "categories"
if let hasKey = AgileDB.shared.tableHasKey(table:table, key:"category1") {
    // process here
    if hasKey {
        // table has key
    } else {
        // table didn't have key
    }
} else {
    // handle error
}
```

See if a given table holds all given keys.
```swift
let table: DBTable = "categories"
if let hasKeys = AgileDB.shared.tableHasAllKeys(table:table, keys:["category1","category2","category3"]) {
    // process here
    if hasKeys {
        // table has all keys
    } else {
        // table didn't have key
    }
} else {
    // handle error
}
```

Return an array of keys in a given table. Optionally specify sort order based on a value at the root level
```swift
let table: DBTable = "categories"
if let tableKeys = AgileDB.shared.keysInTable(table, sortOrder:"name, date desc") }
    // process keys
} else {
    // handle error
}
```

Return an array of keys from the given table sorted in the way specified matching the given conditions.
```swift
/**
All conditions in the same set are ANDed together. Separate sets are ORed against each other.  (set:0 AND set:0 AND set:0) OR (set:1 AND set:1 AND set:1) OR (set:2)

Unsorted Example:

let accountCondition = DBCondition(set:0,objectKey:"account",conditionOperator:.equal, value:"ACCT1")
if let keys = AgileDB.keysInTable("table1", sortOrder:nil, conditions:accountCondition) {
	// use keys
} else {
	// handle error
}

- parameter table: The DBTable to return keys from.
- parameter sortOrder: Optional string that gives a comma delimited list of properties to sort by.
- parameter conditions: Optional array of DBConditions that specify what conditions must be met.
- parameter validateObjects: Optional bool that condition sets will be validated against the table. Any set that refers to json objects that do not exist in the table will be ignored. Default value is false.

- returns: [String]? Returns an array of keys from the table. Is nil when database could not be opened or other error occured.

public func keysInTable(_ table: DBTable, sortOrder: String? = nil, conditions: [DBCondition]? = nil, validateObjects: Bool = false) -> [String]?
*/


let table: DBTable = "accounts"
let accountCondition = DBCondition(set:0,objectKey:"account", conditionOperator:.equal, value:"ACCT1")
if let keys = AgileDB.shared.keysInTable(table, sortOrder: nil, conditions: [accountCondition]) {
    // process keys
} else {
    // handle error
}
```



### Values ###
Data can be set or retrieved manually as shown here or your class/struct can adhere to the DBObject protocol, documented above, and use the built-in init and save methods for greater ease and flexibility.

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
if AgileDB.shared.setValueInTable(table, for: key, to: json)
    // success
} else {
    // handle error
}
```

Retrieve value for a given key
```swift
let table: DBTable = "categories"
if let jsonValue = AgileDB.shared.valueFromTable(table, for:"category1") {
    // process value
} else {
    // handle error
}

if let dictValue = AgileDB.shared.dictValueFromTable(table, for:"category1") {
    // process dictionary value
} else {
    // handle error
}
```

Delete the value for a given key
```swift
let table: DBTable = "categories"
if AgileDB.shared.deleteFromTable(table, for:"category1") {
    // value was deleted
} else {
    // handle error
}
```

## Retrieving Data Asynchronously ##
With version 6.3, AgileDB allows data to be retrieved asynchronously using `await`.

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
- valueFromTable
- dictValueFromTable
- sqlSelect
- load in the DBObject protocol

## SQL Queries ##
AgileDB allows you to do standard SQL selects for more complex queries. Because the values given are actually broken into separate columns in the tables, a standard SQL statement can be passed in and an array of rows (arrays of values) will be optionally returned.

```
let db = AgileDB.shared
let sql = "select name from accounts a inner join categories c on c.accountKey = a.key order by a.name"
if let results = db.sqlSelect(sql) {
    // process results
} else {
    // handle error
}
```

## Syncing ##
AgileDB can sync with other instances of itself by enabling syncing before processing any data and then sharing a sync log.

```swift
/**
Enables syncing. Once enabled, a log is created for all current values in the tables.

- returns: Bool If syncing was successfully enabled.
*/
public func enableSyncing() -> Bool


/**
Disables syncing.

- returns: Bool If syncing was successfully disabled.
*/
public func disableSyncing() -> Bool
    

/**
Read-only array of unsynced tables. Any tables not in this array will be synced.
*/
var unsyncedTables: [String]

/**
Sets the tables that are not to be synced.

- parameter tables: Array of tables that are not to be synced.

- returns: Bool If list was set successfully.
*/
public func setUnsyncedTables(_ tables: [String]) -> Bool


/**
Creates a sync file that can be used on another AgileDB instance to sync data. This is a synchronous call.

- parameter filePath: The full path, including the file itself, to be used for the log file.
- parameter lastSequence: The last sequence used for the given target  Initial sequence is 0.
- parameter targetDBInstanceKey: The dbInstanceKey of the target database. Use the dbInstanceKey method to get the DB's instanceKey.

- returns: (Bool,Int) If the file was successfully created and the lastSequence that should be used in subsequent calls to this instance for the given targetDBInstanceKey.
*/
public func createSyncFileAtURL(_ localURL: URL!, lastSequence: Int, targetDBInstanceKey: String) -> (Bool, Int)


/**
Processes a sync file created by another instance of AgileDB This is a synchronous call.

- parameter filePath: The path to the sync file.
- parameter syncProgress: Optional function that will be called periodically giving the percent complete.

- returns: (Bool,String,Int)  If the sync file was successfully processed,the instanceKey of the submiting DB, and the lastSequence that should be used in subsequent calls to the createSyncFile method of the instance that was used to create this file. If the database couldn't be opened or syncing hasn't been enabled, then the instanceKey will be empty and the lastSequence will be equal to zero.
*/
public typealias syncProgressUpdate = (_ percentComplete: Double) -> Void
public func processSyncFileAtURL(_ localURL: URL!, syncProgress: syncProgressUpdate?) -> (Bool, String, Int)
```    
    
# Revision History
### 6.5 ###
- All DBObject methods have async/await counterparts 

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
