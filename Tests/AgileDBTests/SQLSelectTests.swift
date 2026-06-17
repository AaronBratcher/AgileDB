//
//  SQLSelectTests.swift
//  AgileDBTests
//
//  Exercises direct `sqlSelect` queries against the JSON document storage using a
//  classic invoice construct: an Invoice references a single Client (a nested DBObject)
//  and an array of line items (nested DBObjects). Because each object is stored as a
//  JSON document in its table's `value` column, relationships are expressed in SQL with
//  json_extract (for a referenced key) and json_each (to expand an array of keys), then
//  joined back to the referenced tables by `key`.
//

import Foundation
import Testing
@testable import AgileDB

struct InvoiceClient: DBObject {
	static let table: DBTable = "InvoiceClient"

	var key = UUID().uuidString
	var name: String
	var city: String
}

struct InvoiceLineItem: DBObject {
	static let table: DBTable = "InvoiceLineItem"

	var key = UUID().uuidString
	var name: String
	var quantity: Int
	var price: Double
}

struct Invoice: DBObject {
	static let table: DBTable = "Invoice"

	var key = UUID().uuidString
	var number: Int
	var paid: Bool
	var client: InvoiceClient        // nested DBObject -> stored as the client's key
	var lineItems: [InvoiceLineItem] // nested DBObjects -> stored as an array of keys
}

@Suite("SQL Select Tests")
struct SQLSelectTests {
	/// Saves two clients and two invoices (one with two line items, one with a single
	/// line item). Saving an Invoice cascades the nested client and line items into their
	/// own tables, leaving the invoice document holding only their keys.
	func populate(_ db: AgileDB) async {
		let acme = InvoiceClient(name: "Acme", city: "Springfield")
		let globex = InvoiceClient(name: "Globex", city: "Portland")

		let invoice1 = Invoice(number: 1001, paid: true, client: acme, lineItems: [
			InvoiceLineItem(name: "Widget", quantity: 2, price: 9.99),
			InvoiceLineItem(name: "Gadget", quantity: 1, price: 19.99),
		])

		let invoice2 = Invoice(number: 1002, paid: false, client: globex, lineItems: [
			InvoiceLineItem(name: "Gizmo", quantity: 5, price: 4.5),
		])

		await invoice1.save(to: db)
		await invoice2.save(to: db)
	}

	/// json_extract of a JSON number can come back as either an integer or a real, so read
	/// numeric aggregates tolerantly.
	private func doubleValue(_ value: (any Sendable)?) -> Double? {
		if let doubleValue = value as? Double { return doubleValue }
		if let intValue = value as? Int { return Double(intValue) }
		return nil
	}

	@Test("Select simple scalar values")
	func selectSimpleValues() async throws {
		let db = dbForTesting()
		await populate(db)

		// Pull a simple scalar out of each document, ordered.
		let numberRows = try await db.sqlSelect("select json_extract(value, '$.number') from \(Invoice.table) order by json_extract(value, '$.number')")
		#expect(numberRows.compactMap({ $0.values[0] as? Int }) == [1001, 1002])

		// Filter on a simple boolean value (stored as a JSON bool, surfaced as 1/0).
		let paidRows = try await db.sqlSelect("select key from \(Invoice.table) where json_extract(value, '$.paid') = 1")
		#expect(paidRows.count == 1)

		await removeDB(db)
	}

	@Test("Join invoice to its client")
	func joinInvoiceToClient() async throws {
		let db = dbForTesting()
		await populate(db)

		// The invoice document stores the client's key; join back to the client table.
		let sql = """
		select json_extract(i.value, '$.number'), json_extract(c.value, '$.name')
		from \(Invoice.table) i
		join \(InvoiceClient.table) c on json_extract(i.value, '$.client') = c.key
		where json_extract(c.value, '$.city') = 'Portland'
		"""

		let rows = try await db.sqlSelect(sql)
		#expect(rows.count == 1)
		#expect(rows[0].values[0] as? Int == 1002)
		#expect(rows[0].values[1] as? String == "Globex")

		await removeDB(db)
	}

	@Test("Join invoice to its line items")
	func joinInvoiceToLineItems() async throws {
		let db = dbForTesting()
		await populate(db)

		// Expand the invoice's array of line-item keys with json_each, then join each key
		// back to the line-item table.
		let sql = """
		select json_extract(item.value, '$.name'), json_extract(item.value, '$.quantity')
		from \(Invoice.table) i, json_each(i.value, '$.lineItems') je
		join \(InvoiceLineItem.table) item on je.value = item.key
		where json_extract(i.value, '$.number') = 1001
		order by json_extract(item.value, '$.name')
		"""

		let rows = try await db.sqlSelect(sql)
		#expect(rows.compactMap({ $0.values[0] as? String }) == ["Gadget", "Widget"])
		#expect(rows.compactMap({ $0.values[1] as? Int }) == [1, 2])

		await removeDB(db)
	}

	@Test("Aggregate invoice totals across client and line items")
	func invoiceTotalsByClient() async throws {
		let db = dbForTesting()
		await populate(db)

		// Three-way join: invoice -> client (single reference) and invoice -> line items
		// (array), summing quantity * price per invoice.
		let sql = """
		select json_extract(c.value, '$.name'),
		       sum(json_extract(item.value, '$.quantity') * json_extract(item.value, '$.price'))
		from \(Invoice.table) i
		join \(InvoiceClient.table) c on json_extract(i.value, '$.client') = c.key,
		     json_each(i.value, '$.lineItems') je
		join \(InvoiceLineItem.table) item on je.value = item.key
		group by i.key
		order by json_extract(i.value, '$.number')
		"""

		let rows = try await db.sqlSelect(sql)
		#expect(rows.count == 2)

		// Invoice 1001 (Acme): 2 * 9.99 + 1 * 19.99 = 39.97
		#expect(rows[0].values[0] as? String == "Acme")
		#expect(abs((doubleValue(rows[0].values[1]) ?? 0) - 39.97) < 0.0001)

		// Invoice 1002 (Globex): 5 * 4.5 = 22.5
		#expect(rows[1].values[0] as? String == "Globex")
		#expect(abs((doubleValue(rows[1].values[1]) ?? 0) - 22.5) < 0.0001)

		await removeDB(db)
	}

	@Test("Filter invoices by a line-item property across the join")
	func filterInvoicesByLineItem() async throws {
		let db = dbForTesting()
		await populate(db)

		// Find the distinct invoices that contain a line item priced over 15, joining the
		// invoice's line-item keys to the line-item table.
		let sql = """
		select distinct json_extract(i.value, '$.number')
		from \(Invoice.table) i, json_each(i.value, '$.lineItems') je
		join \(InvoiceLineItem.table) item on je.value = item.key
		where json_extract(item.value, '$.price') > 15
		"""

		let rows = try await db.sqlSelect(sql)
		#expect(rows.compactMap({ $0.values[0] as? Int }) == [1001])

		await removeDB(db)
	}
}
