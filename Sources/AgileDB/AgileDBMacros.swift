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
