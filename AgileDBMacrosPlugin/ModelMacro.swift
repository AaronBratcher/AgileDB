//
//  ModelMacro.swift
//  AgileDBMacrosPlugin
//

import SwiftSyntax
import SwiftSyntaxMacros
import SwiftDiagnostics

enum ModelDiagnostic: String, DiagnosticMessage {
	case notAClassOrStruct

	var message: String {
		switch self {
		case .notAClassOrStruct:
			return "@Model can only be attached to a class or struct declaration"
		}
	}

	var diagnosticID: MessageID {
		MessageID(domain: "AgileDBMacrosPlugin", id: rawValue)
	}

	var severity: DiagnosticSeverity { .error }
}

/// Adds `DBObject` conformance to the attached class/struct, and synthesizes:
/// - `key`, if the type doesn't already declare one
/// - `static var table`, from the macro's `table:` argument, or derived from the type name if omitted
/// - `codingKeys`, listing every stored property not marked `@Transient` (only emitted if at least one property is ignored)
///
/// When at least one property is `@Transient`, the generated `CodingKeys` enum uses the
/// literal name `CodingKeys` (not a type-specific name) so the compiler's own Codable
/// synthesis recognizes it: properties excluded from `CodingKeys` are skipped entirely
/// during decode and keep their declared default value instead of requiring the key to
/// be present. This is what lets a `@Transient` property stay non-optional.
public struct ModelMacro: MemberMacro, ExtensionMacro {
	public static func expansion(
		of node: AttributeSyntax,
		attachedTo declaration: some DeclGroupSyntax,
		providingExtensionsOf type: some TypeSyntaxProtocol,
		conformingTo protocols: [TypeSyntax],
		in context: some MacroExpansionContext
	) throws -> [ExtensionDeclSyntax] {
		let ext: DeclSyntax = "extension \(type.trimmed): DBObject {}"
		return [ext.cast(ExtensionDeclSyntax.self)]
	}

	public static func expansion(
		of node: AttributeSyntax,
		providingMembersOf declaration: some DeclGroupSyntax,
		in context: some MacroExpansionContext
	) throws -> [DeclSyntax] {
		guard let typeName = declaration.agileDBTypeName else {
			context.diagnose(Diagnostic(node: Syntax(node), message: ModelDiagnostic.notAClassOrStruct))
			return []
		}

		let access = declaration.agileDBAccessModifierPrefix
		let members = declaration.memberBlock.members

		var propertyNames: [String] = []
		var hasKeyProperty = false
		var hasIgnoredProperty = false

		for member in members {
			guard let varDecl = member.decl.as(VariableDeclSyntax.self),
			      !varDecl.modifiers.contains(where: { $0.name.tokenKind == .keyword(.static) }),
			      let binding = varDecl.bindings.first,
			      let pattern = binding.pattern.as(IdentifierPatternSyntax.self),
			      isStoredBinding(binding)
			else { continue }

			let name = pattern.identifier.text
			if name == "key" { hasKeyProperty = true }

			if varDecl.attributes.contains(where: { $0.agileDBIsNamed("Transient") }) {
				hasIgnoredProperty = true
				continue
			}

			propertyNames.append(name)
		}

		var generatedMembers: [DeclSyntax] = []

		if !hasKeyProperty {
			generatedMembers.append("\(raw: access)var key = UUID().uuidString")
			propertyNames.insert("key", at: 0)
		}

		let tableExpression = node.agileDBTableArgument ?? "DBTable(name: \"\(typeName)\")"
		generatedMembers.append("\(raw: access)static var table: DBTable { \(raw: tableExpression) }")

		if hasIgnoredProperty {
			let cases = propertyNames.joined(separator: ", ")
			generatedMembers.append("""
			private enum CodingKeys: String, CodingKey, CaseIterable {
			    case \(raw: cases)
			}
			""")

			generatedMembers.append("""
			\(raw: access)var codingKeys: [CodingKey] {
			    CodingKeys.allCases
			}
			""")
		}

		return generatedMembers
	}

	private static func isStoredBinding(_ binding: PatternBindingSyntax) -> Bool {
		guard let accessorBlock = binding.accessorBlock else { return true }
		switch accessorBlock.accessors {
		case .accessors(let accessors):
			return accessors.allSatisfy {
				$0.accessorSpecifier.tokenKind == .keyword(.willSet) || $0.accessorSpecifier.tokenKind == .keyword(.didSet)
			}
		case .getter:
			return false
		}
	}
}

extension DeclGroupSyntax {
	fileprivate var agileDBTypeName: String? {
		if let classDecl = self.as(ClassDeclSyntax.self) { return classDecl.name.text }
		if let structDecl = self.as(StructDeclSyntax.self) { return structDecl.name.text }
		return nil
	}

	fileprivate var agileDBAccessModifierPrefix: String {
		modifiers.contains(where: { $0.name.tokenKind == .keyword(.public) }) ? "public " : ""
	}
}

extension AttributeListSyntax.Element {
	fileprivate func agileDBIsNamed(_ name: String) -> Bool {
		guard case .attribute(let attribute) = self else { return false }
		return attribute.attributeName.as(IdentifierTypeSyntax.self)?.name.text == name
	}
}

extension AttributeSyntax {
	fileprivate var agileDBTableArgument: String? {
		guard let arguments = arguments?.as(LabeledExprListSyntax.self),
		      let tableArgument = arguments.first(where: { $0.label?.text == "table" })
		else { return nil }

		return tableArgument.expression.trimmedDescription
	}
}
