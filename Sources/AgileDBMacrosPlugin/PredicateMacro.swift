//
//  PredicateMacro.swift
//  AgileDBMacrosPlugin
//

import SwiftSyntax
import SwiftSyntaxMacros
import SwiftDiagnostics
import SwiftOperators

enum PredicateDiagnostic: DiagnosticMessage {
	case missingGenericType
	case missingClosure
	case unsupportedClosureSignature
	case unsupportedBody
	case unsupportedExpression(String)

	var message: String {
		switch self {
		case .missingGenericType:
			return "#Predicate requires an explicit generic argument, e.g. #Predicate<Account> { ... }"
		case .missingClosure:
			return "#Predicate requires a trailing closure, e.g. #Predicate<Account> { $0.name == \"x\" }"
		case .unsupportedClosureSignature:
			return "#Predicate closures must take a single parameter"
		case .unsupportedBody:
			return "#Predicate closures must contain a single boolean expression"
		case .unsupportedExpression(let detail):
			return "#Predicate does not support this expression: \(detail)"
		}
	}

	var diagnosticID: MessageID {
		MessageID(domain: "AgileDBMacrosPlugin", id: "Predicate.\(self)")
	}

	var severity: DiagnosticSeverity { .error }
}

struct PredicateExpansionError: Error {
	let diagnostic: PredicateDiagnostic
}

/// Builds a `DBPredicate<T>` from a single-expression closure. See the `#Predicate` macro
/// declaration in the AgileDB target for supported syntax.
public struct PredicateMacro: ExpressionMacro {
	public static func expansion(
		of node: some FreestandingMacroExpansionSyntax,
		in context: some MacroExpansionContext
	) throws -> ExprSyntax {
		guard let typeArgument = node.genericArgumentClause?.arguments.first?.argument else {
			throw PredicateExpansionError(diagnostic: .missingGenericType)
		}
		let typeName = typeArgument.trimmedDescription

		guard let closure = node.trailingClosure else {
			throw PredicateExpansionError(diagnostic: .missingClosure)
		}

		let paramName = try closureParameterName(closure)
		let bodyExpr = try singleExpression(from: closure)

		let folded = try OperatorTable.standardOperators.foldAll(bodyExpr)
		guard let foldedExpr = folded.as(ExprSyntax.self) else {
			throw PredicateExpansionError(diagnostic: .unsupportedExpression("could not resolve operator precedence"))
		}

		let tree = try parseBoolExpr(foldedExpr, paramName: paramName)
		let groups = dnf(tree)

		var conditionExprs: [String] = []
		for (setIndex, group) in groups.enumerated() {
			for leaf in group {
				conditionExprs.append(
					"DBCondition(set: \(setIndex), objectKey: \"\(leaf.objectKey)\", conditionOperator: \(leaf.conditionOperator), value: \(leaf.valueExpr) as any Sendable)"
				)
			}
		}

		let conditionsList = conditionExprs.joined(separator: ",\n    ")
		let result: ExprSyntax = """
		DBPredicate<\(raw: typeName)>(conditions: [
		    \(raw: conditionsList)
		])
		"""

		return result
	}
}

// MARK: - Closure inspection

private func closureParameterName(_ closure: ClosureExprSyntax) throws -> String {
	guard let signature = closure.signature, let parameterClause = signature.parameterClause else {
		return "$0"
	}

	switch parameterClause {
	case .simpleInput(let shorthandParameters):
		guard let first = shorthandParameters.first, shorthandParameters.count == 1 else {
			throw PredicateExpansionError(diagnostic: .unsupportedClosureSignature)
		}
		return first.name.text
	case .parameterClause(let typedParameters):
		guard let first = typedParameters.parameters.first, typedParameters.parameters.count == 1 else {
			throw PredicateExpansionError(diagnostic: .unsupportedClosureSignature)
		}
		return first.firstName.text
	}
}

private func singleExpression(from closure: ClosureExprSyntax) throws -> ExprSyntax {
	guard let onlyItem = closure.statements.first, closure.statements.count == 1 else {
		throw PredicateExpansionError(diagnostic: .unsupportedBody)
	}

	switch onlyItem.item {
	case .expr(let expr):
		return expr
	case .stmt(let stmt):
		guard let returnStmt = stmt.as(ReturnStmtSyntax.self), let expr = returnStmt.expression else {
			throw PredicateExpansionError(diagnostic: .unsupportedBody)
		}
		return expr
	case .decl:
		throw PredicateExpansionError(diagnostic: .unsupportedBody)
	}
}

// MARK: - Boolean expression tree

private struct Leaf {
	let objectKey: String
	let conditionOperator: String
	let valueExpr: String
}

private indirect enum BoolExpr {
	case leaf(Leaf)
	case and([BoolExpr])
	case or([BoolExpr])
}

private func parseBoolExpr(_ expr: ExprSyntax, paramName: String) throws -> BoolExpr {
	if let tuple = expr.as(TupleExprSyntax.self), tuple.elements.count == 1, tuple.elements.first?.label == nil {
		return try parseBoolExpr(tuple.elements.first!.expression, paramName: paramName)
	}

	if let infix = expr.as(InfixOperatorExprSyntax.self) {
		guard let binaryOperator = infix.operator.as(BinaryOperatorExprSyntax.self) else {
			throw PredicateExpansionError(diagnostic: .unsupportedExpression(infix.operator.trimmedDescription))
		}

		switch binaryOperator.operator.text {
		case "&&":
			return .and([try parseBoolExpr(infix.leftOperand, paramName: paramName), try parseBoolExpr(infix.rightOperand, paramName: paramName)])
		case "||":
			return .or([try parseBoolExpr(infix.leftOperand, paramName: paramName), try parseBoolExpr(infix.rightOperand, paramName: paramName)])
		case "==", "!=", "<", ">", "<=", ">=":
			return .leaf(try parseComparison(infix, operatorText: binaryOperator.operator.text, paramName: paramName))
		default:
			throw PredicateExpansionError(diagnostic: .unsupportedExpression(binaryOperator.operator.text))
		}
	}

	if let functionCall = expr.as(FunctionCallExprSyntax.self) {
		return .leaf(try parseContainsCall(functionCall, paramName: paramName))
	}

	throw PredicateExpansionError(diagnostic: .unsupportedExpression(expr.trimmedDescription))
}

private let comparisonOperatorNames: [String: String] = [
	"==": ".equal",
	"!=": ".notEqual",
	"<": ".lessThan",
	">": ".greaterThan",
	"<=": ".lessThanOrEqual",
	">=": ".greaterThanOrEqual",
]

private let swappedComparisonOperator: [String: String] = [
	"==": "==",
	"!=": "!=",
	"<": ">",
	">": "<",
	"<=": ">=",
	">=": "<=",
]

private func parseComparison(_ infix: InfixOperatorExprSyntax, operatorText: String, paramName: String) throws -> Leaf {
	if let path = propertyPath(infix.leftOperand, paramName: paramName) {
		return Leaf(objectKey: path, conditionOperator: comparisonOperatorNames[operatorText]!, valueExpr: infix.rightOperand.trimmedDescription)
	}

	if let path = propertyPath(infix.rightOperand, paramName: paramName) {
		let swapped = swappedComparisonOperator[operatorText]!
		return Leaf(objectKey: path, conditionOperator: comparisonOperatorNames[swapped]!, valueExpr: infix.leftOperand.trimmedDescription)
	}

	throw PredicateExpansionError(diagnostic: .unsupportedExpression(infix.trimmedDescription))
}

private func parseContainsCall(_ functionCall: FunctionCallExprSyntax, paramName: String) throws -> Leaf {
	guard let member = functionCall.calledExpression.as(MemberAccessExprSyntax.self),
	      member.declName.baseName.text == "contains",
	      let receiver = member.base,
	      functionCall.arguments.count == 1,
	      let argument = functionCall.arguments.first?.expression
	else {
		throw PredicateExpansionError(diagnostic: .unsupportedExpression(functionCall.trimmedDescription))
	}

	if let path = propertyPath(receiver, paramName: paramName) {
		return Leaf(objectKey: path, conditionOperator: ".contains", valueExpr: argument.trimmedDescription)
	}

	if let path = propertyPath(argument, paramName: paramName) {
		return Leaf(objectKey: path, conditionOperator: ".inList", valueExpr: receiver.trimmedDescription)
	}

	throw PredicateExpansionError(diagnostic: .unsupportedExpression(functionCall.trimmedDescription))
}

/// Resolves a member-access chain rooted at the closure's parameter (e.g. `$0.account.name`)
/// to a dotted object key (`"account.name"`). Returns nil for anything else, including
/// leading-dot shorthand like `.checking`, which has no base to anchor on.
private func propertyPath(_ expr: ExprSyntax, paramName: String) -> String? {
	guard let member = expr.as(MemberAccessExprSyntax.self), let base = member.base else { return nil }

	let memberName = member.declName.baseName.text
	if base.trimmedDescription == paramName {
		return memberName
	}

	if let basePath = propertyPath(base, paramName: paramName) {
		return "\(basePath).\(memberName)"
	}

	return nil
}

// MARK: - Disjunctive normal form

/// Flattens the AND/OR tree into a list of AND-groups (one per `DBCondition` set), matching
/// `DBCondition`'s own "(set0 AND set0) OR (set1 AND set1)" semantics.
private func dnf(_ expr: BoolExpr) -> [[Leaf]] {
	switch expr {
	case .leaf(let leaf):
		return [[leaf]]
	case .or(let subExprs):
		return subExprs.flatMap { dnf($0) }
	case .and(let subExprs):
		return subExprs.reduce([[]]) { combinations, subExpr in
			let subCombinations = dnf(subExpr)
			return combinations.flatMap { combination in
				subCombinations.map { combination + $0 }
			}
		}
	}
}
