import SwiftSyntax
import SwiftSyntaxMacros
import SwiftSyntaxMacrosTestSupport
import XCTest
@testable import AgileDBMacrosPlugin

final class ModelMacroTests: XCTestCase {
	let macros: [String: Macro.Type] = [
		"Model": ModelMacro.self,
		"Transient": TransientMacro.self,
	]

	func testExplicitTableArgument() {
		assertMacroExpansion(
			"""
			@Model(table: Table.categories)
			final class MoneyCategory {
				var name: String = "Unspecified"
			}
			""",
			expandedSource: """
			final class MoneyCategory {
				@ObservationTracked
				var name: String = "Unspecified"

			    var key: String = UUID().uuidString

			    static var table: DBTable {
			        Table.categories
			    }

			    init() {
			    }

			    @ObservationIgnored private let _$observationRegistrar = Observation.ObservationRegistrar()

			    internal nonisolated func access<Member>(keyPath: KeyPath<MoneyCategory, Member>) {
			        _$observationRegistrar.access(self, keyPath: keyPath)
			    }

			    internal nonisolated func withMutation<Member, MutationResult>(keyPath: KeyPath<MoneyCategory, Member>, _ mutation: () throws -> MutationResult) rethrows -> MutationResult {
			        try _$observationRegistrar.withMutation(of: self, keyPath: keyPath, mutation)
			    }

			    private nonisolated func shouldNotifyObservers<Member>(_ lhs: Member, _ rhs: Member) -> Bool {
			        true
			    }

			    private nonisolated func shouldNotifyObservers<Member: Equatable>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs != rhs
			    }

			    private nonisolated func shouldNotifyObservers<Member: AnyObject>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs !== rhs
			    }

			    private nonisolated func shouldNotifyObservers<Member: Equatable & AnyObject>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs != rhs
			    }

			    private enum CodingKeys: String, CodingKey, CaseIterable {
			        case key, name
			    }

			    init(from decoder: any Decoder) throws {
			        let container = try decoder.container(keyedBy: CodingKeys.self)
			        self.key = try container.decode(String.self, forKey: .key)
			        self.name = try container.decode(String.self, forKey: .name)
			    }

			    func encode(to encoder: any Encoder) throws {
			        var container = encoder.container(keyedBy: CodingKeys.self)
			        try container.encode(key, forKey: .key)
			        try container.encode(name, forKey: .name)
			    }
			}

			extension MoneyCategory: DBObject, Observable {
			}
			""",
			macros: macros
		)
	}

	func testTableDerivedFromTypeNameWhenOmitted() {
		assertMacroExpansion(
			"""
			@Model
			final class Widget {
				var name: String = "Widget"
			}
			""",
			expandedSource: """
			final class Widget {
				@ObservationTracked
				var name: String = "Widget"

			    var key: String = UUID().uuidString

			    static var table: DBTable {
			        DBTable(name: "Widget")
			    }

			    init() {
			    }

			    @ObservationIgnored private let _$observationRegistrar = Observation.ObservationRegistrar()

			    internal nonisolated func access<Member>(keyPath: KeyPath<Widget, Member>) {
			        _$observationRegistrar.access(self, keyPath: keyPath)
			    }

			    internal nonisolated func withMutation<Member, MutationResult>(keyPath: KeyPath<Widget, Member>, _ mutation: () throws -> MutationResult) rethrows -> MutationResult {
			        try _$observationRegistrar.withMutation(of: self, keyPath: keyPath, mutation)
			    }

			    private nonisolated func shouldNotifyObservers<Member>(_ lhs: Member, _ rhs: Member) -> Bool {
			        true
			    }

			    private nonisolated func shouldNotifyObservers<Member: Equatable>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs != rhs
			    }

			    private nonisolated func shouldNotifyObservers<Member: AnyObject>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs !== rhs
			    }

			    private nonisolated func shouldNotifyObservers<Member: Equatable & AnyObject>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs != rhs
			    }

			    private enum CodingKeys: String, CodingKey, CaseIterable {
			        case key, name
			    }

			    init(from decoder: any Decoder) throws {
			        let container = try decoder.container(keyedBy: CodingKeys.self)
			        self.key = try container.decode(String.self, forKey: .key)
			        self.name = try container.decode(String.self, forKey: .name)
			    }

			    func encode(to encoder: any Encoder) throws {
			        var container = encoder.container(keyedBy: CodingKeys.self)
			        try container.encode(key, forKey: .key)
			        try container.encode(name, forKey: .name)
			    }
			}

			extension Widget: DBObject, Observable {
			}
			""",
			macros: macros
		)
	}

	func testExistingKeyPropertyIsLeftUntouched() {
		assertMacroExpansion(
			"""
			@Model(table: Table.widgets)
			final class Widget {
				var key: String = "custom-default"
				var name: String = "Widget"
			}
			""",
			expandedSource: """
			final class Widget {
				@ObservationTracked
				var key: String = "custom-default"
				@ObservationTracked
				var name: String = "Widget"

			    static var table: DBTable {
			        Table.widgets
			    }

			    init() {
			    }

			    @ObservationIgnored private let _$observationRegistrar = Observation.ObservationRegistrar()

			    internal nonisolated func access<Member>(keyPath: KeyPath<Widget, Member>) {
			        _$observationRegistrar.access(self, keyPath: keyPath)
			    }

			    internal nonisolated func withMutation<Member, MutationResult>(keyPath: KeyPath<Widget, Member>, _ mutation: () throws -> MutationResult) rethrows -> MutationResult {
			        try _$observationRegistrar.withMutation(of: self, keyPath: keyPath, mutation)
			    }

			    private nonisolated func shouldNotifyObservers<Member>(_ lhs: Member, _ rhs: Member) -> Bool {
			        true
			    }

			    private nonisolated func shouldNotifyObservers<Member: Equatable>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs != rhs
			    }

			    private nonisolated func shouldNotifyObservers<Member: AnyObject>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs !== rhs
			    }

			    private nonisolated func shouldNotifyObservers<Member: Equatable & AnyObject>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs != rhs
			    }

			    private enum CodingKeys: String, CodingKey, CaseIterable {
			        case key, name
			    }

			    init(from decoder: any Decoder) throws {
			        let container = try decoder.container(keyedBy: CodingKeys.self)
			        self.key = try container.decode(String.self, forKey: .key)
			        self.name = try container.decode(String.self, forKey: .name)
			    }

			    func encode(to encoder: any Encoder) throws {
			        var container = encoder.container(keyedBy: CodingKeys.self)
			        try container.encode(key, forKey: .key)
			        try container.encode(name, forKey: .name)
			    }
			}

			extension Widget: DBObject, Observable {
			}
			""",
			macros: macros
		)
	}

	func testIgnoredPropertyIsExcludedFromCodingKeys() {
		assertMacroExpansion(
			"""
			@Model(table: Table.widgets)
			final class Widget {
				var name: String = "Widget"
				@Transient var isNew: Bool = true
			}
			""",
			expandedSource: """
			final class Widget {
				@ObservationTracked
				var name: String = "Widget"
				var isNew: Bool = true

			    var key: String = UUID().uuidString

			    static var table: DBTable {
			        Table.widgets
			    }

			    init() {
			    }

			    @ObservationIgnored private let _$observationRegistrar = Observation.ObservationRegistrar()

			    internal nonisolated func access<Member>(keyPath: KeyPath<Widget, Member>) {
			        _$observationRegistrar.access(self, keyPath: keyPath)
			    }

			    internal nonisolated func withMutation<Member, MutationResult>(keyPath: KeyPath<Widget, Member>, _ mutation: () throws -> MutationResult) rethrows -> MutationResult {
			        try _$observationRegistrar.withMutation(of: self, keyPath: keyPath, mutation)
			    }

			    private nonisolated func shouldNotifyObservers<Member>(_ lhs: Member, _ rhs: Member) -> Bool {
			        true
			    }

			    private nonisolated func shouldNotifyObservers<Member: Equatable>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs != rhs
			    }

			    private nonisolated func shouldNotifyObservers<Member: AnyObject>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs !== rhs
			    }

			    private nonisolated func shouldNotifyObservers<Member: Equatable & AnyObject>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs != rhs
			    }

			    private enum CodingKeys: String, CodingKey, CaseIterable {
			        case key, name
			    }

			    init(from decoder: any Decoder) throws {
			        let container = try decoder.container(keyedBy: CodingKeys.self)
			        self.key = try container.decode(String.self, forKey: .key)
			        self.name = try container.decode(String.self, forKey: .name)
			    }

			    func encode(to encoder: any Encoder) throws {
			        var container = encoder.container(keyedBy: CodingKeys.self)
			        try container.encode(key, forKey: .key)
			        try container.encode(name, forKey: .name)
			    }
			}

			extension Widget: DBObject, Observable {
			}
			""",
			macros: macros
		)
	}

	func testOptionalPropertyUsesDecodeIfPresent() {
		assertMacroExpansion(
			"""
			@Model(table: Table.widgets)
			final class Widget {
				var name: String = "Widget"
				var note: String?
			}
			""",
			expandedSource: """
			final class Widget {
				@ObservationTracked
				var name: String = "Widget"
				@ObservationTracked
				var note: String?

			    var key: String = UUID().uuidString

			    static var table: DBTable {
			        Table.widgets
			    }

			    init() {
			    }

			    @ObservationIgnored private let _$observationRegistrar = Observation.ObservationRegistrar()

			    internal nonisolated func access<Member>(keyPath: KeyPath<Widget, Member>) {
			        _$observationRegistrar.access(self, keyPath: keyPath)
			    }

			    internal nonisolated func withMutation<Member, MutationResult>(keyPath: KeyPath<Widget, Member>, _ mutation: () throws -> MutationResult) rethrows -> MutationResult {
			        try _$observationRegistrar.withMutation(of: self, keyPath: keyPath, mutation)
			    }

			    private nonisolated func shouldNotifyObservers<Member>(_ lhs: Member, _ rhs: Member) -> Bool {
			        true
			    }

			    private nonisolated func shouldNotifyObservers<Member: Equatable>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs != rhs
			    }

			    private nonisolated func shouldNotifyObservers<Member: AnyObject>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs !== rhs
			    }

			    private nonisolated func shouldNotifyObservers<Member: Equatable & AnyObject>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs != rhs
			    }

			    private enum CodingKeys: String, CodingKey, CaseIterable {
			        case key, name, note
			    }

			    init(from decoder: any Decoder) throws {
			        let container = try decoder.container(keyedBy: CodingKeys.self)
			        self.key = try container.decode(String.self, forKey: .key)
			        self.name = try container.decode(String.self, forKey: .name)
			        self.note = try container.decodeIfPresent(String.self, forKey: .note)
			    }

			    func encode(to encoder: any Encoder) throws {
			        var container = encoder.container(keyedBy: CodingKeys.self)
			        try container.encode(key, forKey: .key)
			        try container.encode(name, forKey: .name)
			        try container.encodeIfPresent(note, forKey: .note)
			    }
			}

			extension Widget: DBObject, Observable {
			}
			""",
			macros: macros
		)
	}

	func testStructIsRejected() {
		assertMacroExpansion(
			"""
			@Model
			struct Widget {
				var name: String = "Widget"
			}
			""",
			expandedSource: """
			struct Widget {
				var name: String = "Widget"
			}
			""",
			diagnostics: [
				DiagnosticSpec(message: "@Model can only be attached to a class declaration (structs can't conform to Observable)", line: 1, column: 1)
			],
			macros: macros
		)
	}

	func testMissingTypeAnnotationIsDiagnosed() {
		assertMacroExpansion(
			"""
			@Model(table: Table.widgets)
			final class Widget {
				var name = "Widget"
			}
			""",
			expandedSource: """
			final class Widget {
				@ObservationTracked
				var name = "Widget"

			    var key: String = UUID().uuidString

			    static var table: DBTable {
			        Table.widgets
			    }

			    init() {
			    }

			    @ObservationIgnored private let _$observationRegistrar = Observation.ObservationRegistrar()

			    internal nonisolated func access<Member>(keyPath: KeyPath<Widget, Member>) {
			        _$observationRegistrar.access(self, keyPath: keyPath)
			    }

			    internal nonisolated func withMutation<Member, MutationResult>(keyPath: KeyPath<Widget, Member>, _ mutation: () throws -> MutationResult) rethrows -> MutationResult {
			        try _$observationRegistrar.withMutation(of: self, keyPath: keyPath, mutation)
			    }

			    private nonisolated func shouldNotifyObservers<Member>(_ lhs: Member, _ rhs: Member) -> Bool {
			        true
			    }

			    private nonisolated func shouldNotifyObservers<Member: Equatable>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs != rhs
			    }

			    private nonisolated func shouldNotifyObservers<Member: AnyObject>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs !== rhs
			    }

			    private nonisolated func shouldNotifyObservers<Member: Equatable & AnyObject>(_ lhs: Member, _ rhs: Member) -> Bool {
			        lhs != rhs
			    }

			    private enum CodingKeys: String, CodingKey, CaseIterable {
			        case key
			    }

			    init(from decoder: any Decoder) throws {
			        let container = try decoder.container(keyedBy: CodingKeys.self)
			        self.key = try container.decode(String.self, forKey: .key)
			    }

			    func encode(to encoder: any Encoder) throws {
			        var container = encoder.container(keyedBy: CodingKeys.self)
			        try container.encode(key, forKey: .key)
			    }
			}

			extension Widget: DBObject, Observable {
			}
			""",
			diagnostics: [
				DiagnosticSpec(message: "Property 'name' needs an explicit type annotation for @Model to generate Codable conformance (type inference from initializers isn't available to macros)", line: 3, column: 6)
			],
			macros: macros
		)
	}
}
