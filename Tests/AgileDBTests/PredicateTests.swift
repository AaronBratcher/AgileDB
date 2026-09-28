//
//  PredicateTests.swift
//  AgileDBTests
//
//  Created by Aaron Bratcher on 7/4/26.
//

import Foundation
import Testing
@testable import AgileDB

@Model(table: "PredicateAccount")
final class PredicateAccount: @unchecked Sendable {
	var name: String = ""
	var type: String = "checking"
	var balance: Int = 0
	var tags: [String] = []
}

@Suite("Predicate macro integration")
struct PredicateTests {
	@Test("Single comparison filters via keysInTable")
	func testSingleComparisonFilters() async throws {
		let db = dbForTesting()

		let checking = PredicateAccount()
		checking.name = "Checking"
		checking.type = "checking"
		await checking.save(to: db)

		let savings = PredicateAccount()
		savings.name = "Savings"
		savings.type = "savings"
		await savings.save(to: db)

		let predicate = #Predicate<PredicateAccount> { $0.type == "checking" }
		let keys = try await db.keysInTable(PredicateAccount.table, conditions: predicate.conditions)

		#expect(keys == [checking.key])

		await removeDB(db)
	}

	@Test("AND combines conditions into the same set")
	func testAndCombinesConditions() async throws {
		let db = dbForTesting()

		let match = PredicateAccount()
		match.type = "checking"
		match.balance = 500
		await match.save(to: db)

		let wrongType = PredicateAccount()
		wrongType.type = "savings"
		wrongType.balance = 500
		await wrongType.save(to: db)

		let wrongBalance = PredicateAccount()
		wrongBalance.type = "checking"
		wrongBalance.balance = 10
		await wrongBalance.save(to: db)

		let predicate = #Predicate<PredicateAccount> { $0.type == "checking" && $0.balance > 100 }
		let keys = try await db.keysInTable(PredicateAccount.table, conditions: predicate.conditions)

		#expect(keys == [match.key])

		await removeDB(db)
	}

	@Test("OR combines conditions into separate sets")
	func testOrCombinesConditions() async throws {
		let db = dbForTesting()

		let checking = PredicateAccount()
		checking.type = "checking"
		checking.balance = 10
		await checking.save(to: db)

		let bigSavings = PredicateAccount()
		bigSavings.type = "savings"
		bigSavings.balance = 5000
		await bigSavings.save(to: db)

		let smallSavings = PredicateAccount()
		smallSavings.type = "savings"
		smallSavings.balance = 10
		await smallSavings.save(to: db)

		let predicate = #Predicate<PredicateAccount> { $0.type == "checking" || $0.balance > 400 }
		let keys = try await db.keysInTable(PredicateAccount.table, conditions: predicate.conditions)

		#expect(Set(keys) == Set([checking.key, bigSavings.key]))

		await removeDB(db)
	}

	@Test("inList matches membership in an array literal")
	func testInListMatchesMembership() async throws {
		let db = dbForTesting()

		let checking = PredicateAccount()
		checking.type = "checking"
		await checking.save(to: db)

		let savings = PredicateAccount()
		savings.type = "savings"
		await savings.save(to: db)

		let creditCard = PredicateAccount()
		creditCard.type = "creditCard"
		await creditCard.save(to: db)

		let predicate = #Predicate<PredicateAccount> { ["checking", "savings"].contains($0.type) }
		let keys = try await db.keysInTable(PredicateAccount.table, conditions: predicate.conditions)

		#expect(Set(keys) == Set([checking.key, savings.key]))

		await removeDB(db)
	}

	@Test("contains matches array element")
	func testContainsMatchesArrayElement() async throws {
		let db = dbForTesting()

		let tagged = PredicateAccount()
		tagged.tags = ["primary", "shared"]
		await tagged.save(to: db)

		let untagged = PredicateAccount()
		untagged.tags = ["shared"]
		await untagged.save(to: db)

		let predicate = #Predicate<PredicateAccount> { $0.tags.contains("primary") }
		let keys = try await db.keysInTable(PredicateAccount.table, conditions: predicate.conditions)

		#expect(keys == [tagged.key])

		await removeDB(db)
	}

	@Test("almostEquals matches ignoring punctuation and case")
	func testAlmostEqualsMatchesFuzzily() async throws {
		let db = dbForTesting()

		let apostrophe = PredicateAccount()
		apostrophe.name = "Sam's Club"
		await apostrophe.save(to: db)

		let shouted = PredicateAccount()
		shouted.name = "SAMS CLUB"
		await shouted.save(to: db)

		let other = PredicateAccount()
		other.name = "Costco"
		await other.save(to: db)

		let searchText = "Sams Club"
		let predicate = #Predicate<PredicateAccount> { $0.name.almostEquals(searchText) }
		let keys = try await db.keysInTable(PredicateAccount.table, conditions: predicate.conditions)

		#expect(Set(keys) == Set([apostrophe.key, shouted.key]))

		await removeDB(db)
	}
}
