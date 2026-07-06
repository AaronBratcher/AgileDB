//
//  ReferenceTests.swift
//  AgileDBTests
//

import Foundation
import Testing
@testable import AgileDB

struct RefChild: DBObject {
	static let table: DBTable = "RefChild"

	var key = UUID().uuidString
	var name = "Child"
}

struct RefParent: DBObject {
	static let table: DBTable = "RefParent"

	var key = UUID().uuidString
	var name = "Parent"
	var child: RefChild
}

struct CycleA: DBObject {
	static let table: DBTable = "CycleA"

	var key = UUID().uuidString
	var partner: CycleB? = nil
}

// A class, not a struct, because a value type can't have a stored property that
// recursively contains itself (even indirectly through CycleA's `partner`).
final class CycleB: DBObject, @unchecked Sendable {
	static let table: DBTable = "CycleB"

	var key = UUID().uuidString
	var partner: CycleA? = nil

	init(partner: CycleA? = nil) {
		self.partner = partner
	}
}

@Suite("Object Reference Tracking")
struct ReferenceTests {
	@Test("Saving records internal and external references")
	func testSaveRecordsReferences() async throws {
		let db = dbForTesting()

		let child = RefChild()
		let parent = RefParent(child: child)
		await parent.save(to: db)

		let internalReferences = await db.internalReferences(for: RefParent.table, key: parent.key)
		#expect(internalReferences == [DBReference(table: RefChild.table, key: child.key)])

		let externalReferences = await db.externalReferences(for: RefChild.table, key: child.key)
		#expect(externalReferences == [DBReference(table: RefParent.table, key: parent.key)])

		await removeDB(db)
	}

	@Test("Re-saving with a different reference updates backlinks")
	func testSaveUpdatesBacklinksOnChange() async throws {
		let db = dbForTesting()

		let childA = RefChild(name: "A")
		let childB = RefChild(name: "B")
		var parent = RefParent(child: childA)
		await parent.save(to: db)

		parent.child = childB
		await parent.save(to: db)

		let childAExternalReferences = await db.externalReferences(for: RefChild.table, key: childA.key)
		#expect(childAExternalReferences == [])

		let childBExternalReferences = await db.externalReferences(for: RefChild.table, key: childB.key)
		#expect(childBExternalReferences == [DBReference(table: RefParent.table, key: parent.key)])

		await removeDB(db)
	}

	@Test("cascadeDelete removes an orphaned nested object")
	func testCascadeDeleteRemovesOrphan() async throws {
		let db = dbForTesting()

		let child = RefChild()
		let parent = RefParent(child: child)
		await parent.save(to: db)

		let result = await parent.delete(from: db, cascadeDelete: true)
		#expect(result == .completed)

		let parentExists = try await db.tableHasKey(table: RefParent.table, key: parent.key)
		let childExists = try await db.tableHasKey(table: RefChild.table, key: child.key)
		#expect(!parentExists)
		#expect(!childExists)

		await removeDB(db)
	}

	@Test("cascadeDelete retains an object still referenced elsewhere")
	func testCascadeDeleteRetainsSharedReference() async throws {
		let db = dbForTesting()

		let sharedChild = RefChild()
		let parent1 = RefParent(child: sharedChild)
		let parent2 = RefParent(child: sharedChild)
		await parent1.save(to: db)
		await parent2.save(to: db)

		let firstResult = await parent1.delete(from: db, cascadeDelete: true)
		#expect(firstResult == .partial(retained: [DBReference(table: RefChild.table, key: sharedChild.key)]))

		let childExistsAfterFirstDelete = try await db.tableHasKey(table: RefChild.table, key: sharedChild.key)
		#expect(childExistsAfterFirstDelete)

		let secondResult = await parent2.delete(from: db, cascadeDelete: true)
		#expect(secondResult == .completed)

		let childExistsAfterSecondDelete = try await db.tableHasKey(table: RefChild.table, key: sharedChild.key)
		#expect(!childExistsAfterSecondDelete)

		await removeDB(db)
	}

	@Test("delete without cascadeDelete leaves referenced objects in place")
	func testNonCascadingDeleteLeavesReferencesAlone() async throws {
		let db = dbForTesting()

		let child = RefChild()
		let parent = RefParent(child: child)
		await parent.save(to: db)

		let result = await parent.delete(from: db)
		#expect(result == .completed)

		let childExists = try await db.tableHasKey(table: RefChild.table, key: child.key)
		#expect(childExists)

		await removeDB(db)
	}

	@Test("cascadeDelete on a reference cycle terminates and removes both objects")
	func testCascadeDeleteHandlesCycle() async throws {
		let db = dbForTesting()

		var a = CycleA()
		let b = CycleB(partner: a)
		a.partner = b

		// b.partner holds a value snapshot of `a` from before `a.partner` was set above, so
		// saving `b` directly here would re-persist that stale, partner-less copy of `a`.
		// `a.save` alone is sufficient: it recursively saves the current `b`, which already
		// carries the reference back to `a`.
		await a.save(to: db)

		let result = await a.delete(from: db, cascadeDelete: true)
		#expect(result == .completed)

		let aExists = try await db.tableHasKey(table: CycleA.table, key: a.key)
		let bExists = try await db.tableHasKey(table: CycleB.table, key: b.key)
		#expect(!aExists)
		#expect(!bExists)

		await removeDB(db)
	}
}
