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
			struct MoneyCategory {
				var name = "Unspecified"
			}
			""",
			expandedSource: """
			struct MoneyCategory {
				var name = "Unspecified"

			    var key = UUID().uuidString

			    static var table: DBTable {
			        Table.categories
			    }
			}

			extension MoneyCategory: DBObject {
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
				var name = "Widget"
			}
			""",
			expandedSource: """
			final class Widget {
				var name = "Widget"

			    var key = UUID().uuidString

			    static var table: DBTable {
			        DBTable(name: "Widget")
			    }
			}

			extension Widget: DBObject {
			}
			""",
			macros: macros
		)
	}

	func testExistingKeyPropertyIsLeftUntouched() {
		assertMacroExpansion(
			"""
			@Model(table: Table.widgets)
			struct Widget {
				var key = "custom-default"
				var name = "Widget"
			}
			""",
			expandedSource: """
			struct Widget {
				var key = "custom-default"
				var name = "Widget"

			    static var table: DBTable {
			        Table.widgets
			    }
			}

			extension Widget: DBObject {
			}
			""",
			macros: macros
		)
	}

	func testIgnoredPropertyIsExcludedFromCodingKeys() {
		assertMacroExpansion(
			"""
			@Model(table: Table.widgets)
			struct Widget {
				var name = "Widget"
				@Transient var isNew = true
			}
			""",
			expandedSource: """
			struct Widget {
				var name = "Widget"
				var isNew = true

			    var key = UUID().uuidString

			    static var table: DBTable {
			        Table.widgets
			    }

			    private enum CodingKeys: String, CodingKey, CaseIterable {
			        case key, name
			    }

			    var codingKeys: [CodingKey] {
			        CodingKeys.allCases
			    }
			}

			extension Widget: DBObject {
			}
			""",
			macros: macros
		)
	}
}
