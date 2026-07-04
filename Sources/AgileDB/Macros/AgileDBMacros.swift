//
//  AgileDBMacros.swift
//  AgileDB
//
//  Created by Aaron Bratcher on 7/4/26.
//

/**
Conforms the attached class or struct to `DBObject` and generates its boilerplate:

- `key`, if the type doesn't already declare one (defaults to `UUID().uuidString`)
- `static var table`, from the `table:` argument, or derived from the type name if omitted
- `codingKeys`, listing every stored property not marked `@Transient`

```swift
@Model(table: Table.categories)
public struct MoneyCategory: Sendable {
    public var key = UUID().uuidString
    public var name = "Unspecified"
    @Transient public var isNew = true
}
```

- parameter table: Optional expression evaluating to a `DBTable`. Defaults to `DBTable(name:)` using the type's own name.
*/
@attached(extension, conformances: DBObject)
@attached(member, names: named(key), named(table), named(codingKeys), arbitrary)
public macro Model(table: DBTable? = nil) = #externalMacro(module: "AgileDBMacrosPlugin", type: "ModelMacro")

/**
Excludes the attached property from persistence when applied within a type using `@Model`.
*/
@attached(peer)
public macro Transient() = #externalMacro(module: "AgileDBMacrosPlugin", type: "TransientMacro")

/**
Builds a `DBPredicate<T>` from a single-expression closure, for use with `@Query`'s `filter:`
parameter or directly with `keysInTable`/`countKeysInTable`/`publisher`.

```swift
let predicate = #Predicate<Account> { $0.type == .checking && $0.balance > 0 }
```

Supported inside the closure:
- Comparisons `==`, `!=`, `<`, `>`, `<=`, `>=` between a property path (`$0.property` or
  `$0.nested.property`) and a value expression, in either order
- `$0.property.contains(value)` for array/string properties
- `array.contains($0.property)` for membership checks
- `&&` and `||` combining any number of the above, including mixed nesting (expanded to the
  set-based AND/OR form `DBCondition` uses)

Each `&&`-joined group of comparisons becomes one condition `set` (ANDed); `||` starts a new
set (ORed against the others), matching `DBCondition`'s own semantics.
*/
@freestanding(expression)
public macro Predicate<T: DBObject>(_ body: (T) -> Bool) -> DBPredicate<T> = #externalMacro(module: "AgileDBMacrosPlugin", type: "PredicateMacro")
