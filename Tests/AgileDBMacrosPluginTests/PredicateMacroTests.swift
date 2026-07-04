import SwiftSyntax
import SwiftSyntaxMacros
import SwiftSyntaxMacrosTestSupport
import XCTest
@testable import AgileDBMacrosPlugin

final class PredicateMacroTests: XCTestCase {
	let macros: [String: Macro.Type] = [
		"Predicate": PredicateMacro.self,
	]

	func testSingleComparison() {
		assertMacroExpansion(
			"""
			#Predicate<Account> { $0.name == "Checking" }
			""",
			expandedSource: """
			DBPredicate<Account>(conditions: [
			    DBCondition(set: 0, objectKey: "name", conditionOperator: .equal, value: "Checking" as any Sendable)
			    ])
			""",
			macros: macros
		)
	}

	func testValueOnLeftSideSwapsOperator() {
		assertMacroExpansion(
			"""
			#Predicate<Account> { 100 < $0.balance }
			""",
			expandedSource: """
			DBPredicate<Account>(conditions: [
			    DBCondition(set: 0, objectKey: "balance", conditionOperator: .greaterThan, value: 100 as any Sendable)
			    ])
			""",
			macros: macros
		)
	}

	func testAndCombinesIntoSameSet() {
		assertMacroExpansion(
			"""
			#Predicate<Account> { $0.type == .checking && $0.balance > 0 }
			""",
			expandedSource: """
			DBPredicate<Account>(conditions: [
			    DBCondition(set: 0, objectKey: "type", conditionOperator: .equal, value: .checking as any Sendable),
			    DBCondition(set: 0, objectKey: "balance", conditionOperator: .greaterThan, value: 0 as any Sendable)
			    ])
			""",
			macros: macros
		)
	}

	func testOrCombinesIntoDifferentSets() {
		assertMacroExpansion(
			"""
			#Predicate<Account> { $0.type == .checking || $0.type == .savings }
			""",
			expandedSource: """
			DBPredicate<Account>(conditions: [
			    DBCondition(set: 0, objectKey: "type", conditionOperator: .equal, value: .checking as any Sendable),
			    DBCondition(set: 1, objectKey: "type", conditionOperator: .equal, value: .savings as any Sendable)
			    ])
			""",
			macros: macros
		)
	}

	func testMixedAndOrExpandsToDisjunctiveNormalForm() {
		assertMacroExpansion(
			"""
			#Predicate<Account> { ($0.type == .checking || $0.type == .savings) && $0.balance > 0 }
			""",
			expandedSource: """
			DBPredicate<Account>(conditions: [
			    DBCondition(set: 0, objectKey: "type", conditionOperator: .equal, value: .checking as any Sendable),
			    DBCondition(set: 0, objectKey: "balance", conditionOperator: .greaterThan, value: 0 as any Sendable),
			    DBCondition(set: 1, objectKey: "type", conditionOperator: .equal, value: .savings as any Sendable),
			    DBCondition(set: 1, objectKey: "balance", conditionOperator: .greaterThan, value: 0 as any Sendable)
			    ])
			""",
			macros: macros
		)
	}

	func testContainsOnProperty() {
		assertMacroExpansion(
			"""
			#Predicate<Account> { $0.name.contains("Check") }
			""",
			expandedSource: """
			DBPredicate<Account>(conditions: [
			    DBCondition(set: 0, objectKey: "name", conditionOperator: .contains, value: "Check" as any Sendable)
			    ])
			""",
			macros: macros
		)
	}

	func testArrayContainsPropertyProducesInList() {
		assertMacroExpansion(
			"""
			#Predicate<Account> { ["ACCT1", "ACCT3"].contains($0.key) }
			""",
			expandedSource: """
			DBPredicate<Account>(conditions: [
			    DBCondition(set: 0, objectKey: "key", conditionOperator: .inList, value: ["ACCT1", "ACCT3"] as any Sendable)
			    ])
			""",
			macros: macros
		)
	}

	func testNestedPropertyPath() {
		assertMacroExpansion(
			"""
			#Predicate<BasicTransaction> { $0.account.name == "Checking" }
			""",
			expandedSource: """
			DBPredicate<BasicTransaction>(conditions: [
			    DBCondition(set: 0, objectKey: "account.name", conditionOperator: .equal, value: "Checking" as any Sendable)
			    ])
			""",
			macros: macros
		)
	}

	func testNamedClosureParameter() {
		assertMacroExpansion(
			"""
			#Predicate<Account> { account in account.balance >= 100 }
			""",
			expandedSource: """
			DBPredicate<Account>(conditions: [
			    DBCondition(set: 0, objectKey: "balance", conditionOperator: .greaterThanOrEqual, value: 100 as any Sendable)
			    ])
			""",
			macros: macros
		)
	}

	func testExplicitReturnStatement() {
		assertMacroExpansion(
			"""
			#Predicate<Account> { return $0.balance != 0 }
			""",
			expandedSource: """
			DBPredicate<Account>(conditions: [
			    DBCondition(set: 0, objectKey: "balance", conditionOperator: .notEqual, value: 0 as any Sendable)
			    ])
			""",
			macros: macros
		)
	}
}
