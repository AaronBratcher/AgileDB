import Testing
import Foundation
@testable import AgileDB

@Suite("DBResults")
struct DBResultsTests {
	@Test
	func testDBResults() async throws {
		let db = dbForTesting()

		let date = Date()
		let keys = ["1", "2", "3"]

		var transaction = Transaction(key: keys[0], date: date, accountKey: "A1", notes: TransactionValue.notes, amount: TransactionValue.amount, purchaseOrders: TransactionValue.purchaseOrders, isNew: TransactionValue.isNew)
		await transaction.save(to: db)

		transaction = Transaction(key: keys[1], date: date, accountKey: "A2", notes: TransactionValue.notes, amount: TransactionValue.amount, purchaseOrders: TransactionValue.purchaseOrders, isNew: TransactionValue.isNew)
		await transaction.save(to: db)

		transaction = Transaction(key: keys[2], date: date, accountKey: "A3", notes: TransactionValue.notes, amount: TransactionValue.amount, purchaseOrders: TransactionValue.purchaseOrders, isNew: TransactionValue.isNew)
		await transaction.save(to: db)

		let transactions = DBResults<Transaction>(db: db, keys: keys)
		#expect(transactions.count == 3)

		let t1 = try await #require(transactions.object(at: 0))
		let t2 = try await #require(transactions.object(at: 1))
		let t3 = try await #require(transactions.object(at: 2))

		#expect(t1.accountKey == "A1")
		#expect(t2.accountKey == "A2")
		#expect(t3.accountKey == "A3")

		await removeDB(db)
	}
}
