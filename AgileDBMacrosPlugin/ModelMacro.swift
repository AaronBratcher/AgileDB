//
//  ModelMacro.swift
//  AgileDBMacrosPlugin
//

import SwiftSyntax
import SwiftSyntaxMacros
import SwiftDiagnostics

enum ModelDiagnostic: DiagnosticMessage {
	case notAClass
	case missingTypeAnnotation(propertyName: String)

	var message: String {
		switch self {
		case .notAClass:
			return "@Model can only be attached to a class declaration (structs can't conform to Observable)"
		case .missingTypeAnnotation(let propertyName):
			return "Property '\(propertyName)' needs an explicit type annotation for @Model to generate Codable conformance (type inference from initializers isn't available to macros)"
		}
	}

	var diagnosticID: MessageID {
		switch self {
		case .notAClass:
			return MessageID(domain: "AgileDBMacrosPlugin", id: "notAClass")
		case .missingTypeAnnotation:
			return MessageID(domain: "AgileDBMacrosPlugin", id: "missingTypeAnnotation")
		}
	}

	var severity: DiagnosticSeverity { .error }
}

private struct ModelProperty {
	let name: String
	let typeText: String
	let isOptional: Bool
}

/// Conforms the attached class to `DBObject` and `Observable`, and synthesizes:
/// - `key`, if the type doesn't already declare one
/// - `static var table`, from the macro's `table:` argument, or derived from the type name if omitted
/// - an `ObservationRegistrar` plus the `access`/`withMutation` helpers `@Observable` relies on,
///   and `@ObservationTracked` on every eligible stored property, so instances participate in
///   SwiftUI observation
/// - `CodingKeys`, `init(from:)`, and `encode(to:)`, listing every stored property not marked
///   `@Transient`
///
/// Because `@ObservationTracked` rewrites stored properties into computed ones, the compiler's
/// own `Codable` synthesis can no longer see them as stored — so this macro generates
/// `init(from:)`/`encode(to:)` itself instead of relying on that synthesis. Doing so requires
/// every non-`@Transient` stored property to carry an explicit type annotation: macros expand
/// before type-checking, so there's no way to infer a property's type from its initializer
/// expression the way the compiler can.
///
/// `@Transient` properties are skipped entirely in the generated `init(from:)`/`encode(to:)`,
/// so they're never present in the stored dictionary and keep their declared default value
/// after decoding instead of requiring the key to be present.
public struct ModelMacro: MemberMacro, ExtensionMacro, MemberAttributeMacro {
	public static func expansion(
		of node: AttributeSyntax,
		attachedTo declaration: some DeclGroupSyntax,
		providingExtensionsOf type: some TypeSyntaxProtocol,
		conformingTo protocols: [TypeSyntax],
		in context: some MacroExpansionContext
	) throws -> [ExtensionDeclSyntax] {
		guard declaration.agileDBTypeName != nil else { return [] } // notAClass is reported by the member role

		let ext: DeclSyntax = "extension \(type.trimmed): DBObject, Observable {}"
		return [ext.cast(ExtensionDeclSyntax.self)]
	}

	public static func expansion(
		of node: AttributeSyntax,
		attachedTo declaration: some DeclGroupSyntax,
		providingAttributesFor member: some DeclSyntaxProtocol,
		in context: some MacroExpansionContext
	) throws -> [AttributeSyntax] {
		guard declaration.agileDBTypeName != nil, // don't stamp anything once @Model itself is going to be rejected
		      let varDecl = member.as(VariableDeclSyntax.self),
		      varDecl.bindingSpecifier.tokenKind == .keyword(.var),
		      !varDecl.modifiers.contains(where: { $0.name.tokenKind == .keyword(.static) }),
		      let binding = varDecl.bindings.first,
		      binding.accessorBlock == nil, // plain stored var only — no computed/willSet/didSet
		      !varDecl.attributes.contains(where: { $0.agileDBIsNamed("Transient") })
		else { return [] }

		return ["@ObservationTracked"]
	}

	public static func expansion(
		of node: AttributeSyntax,
		providingMembersOf declaration: some DeclGroupSyntax,
		conformingTo protocols: [TypeSyntax],
		in context: some MacroExpansionContext
	) throws -> [DeclSyntax] {
		guard let typeName = declaration.agileDBTypeName else {
			context.diagnose(Diagnostic(node: Syntax(node), message: ModelDiagnostic.notAClass))
			return []
		}

		let access = declaration.agileDBAccessModifierPrefix
		let members = declaration.memberBlock.members

		var properties: [ModelProperty] = []
		var hasKeyProperty = false

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
				continue
			}

			guard let typeAnnotation = binding.typeAnnotation?.type else {
				context.diagnose(Diagnostic(node: Syntax(binding), message: ModelDiagnostic.missingTypeAnnotation(propertyName: name)))
				continue
			}

			if let optionalType = typeAnnotation.as(OptionalTypeSyntax.self) {
				properties.append(ModelProperty(name: name, typeText: optionalType.wrappedType.trimmedDescription, isOptional: true))
			} else {
				properties.append(ModelProperty(name: name, typeText: typeAnnotation.trimmedDescription, isOptional: false))
			}
		}

		var generatedMembers: [DeclSyntax] = []

		if !hasKeyProperty {
			generatedMembers.append("\(raw: access)var key: String = UUID().uuidString")
			properties.insert(ModelProperty(name: "key", typeText: "String", isOptional: false), at: 0)
		}

		let tableExpression = node.agileDBTableArgument ?? "DBTable(name: \"\(typeName)\")"
		generatedMembers.append("\(raw: access)static var table: DBTable { \(raw: tableExpression) }")

		// A class loses its compiler-synthesized no-arg init() as soon as it declares any
		// initializer of its own — and `init(from:)` below counts. Restore it here so
		// `TypeName()` keeps working for types that don't declare their own initializer.
		let hasExplicitInitializer = members.contains { $0.decl.is(InitializerDeclSyntax.self) }
		if !hasExplicitInitializer {
			generatedMembers.append("\(raw: access)init() {}")
		}

		generatedMembers.append("""
		@ObservationIgnored private let _$observationRegistrar = Observation.ObservationRegistrar()
		""")

		generatedMembers.append("""
		internal nonisolated func access<Member>(keyPath: KeyPath<\(raw: typeName), Member>) {
		    _$observationRegistrar.access(self, keyPath: keyPath)
		}
		""")

		generatedMembers.append("""
		internal nonisolated func withMutation<Member, MutationResult>(keyPath: KeyPath<\(raw: typeName), Member>, _ mutation: () throws -> MutationResult) rethrows -> MutationResult {
		    try _$observationRegistrar.withMutation(of: self, keyPath: keyPath, mutation)
		}
		""")

		// `@ObservationTracked`'s generated setter calls this (unqualified) to skip firing
		// observers when a mutation doesn't actually change the value. These four overloads
		// mirror exactly what Apple's own `@Observable` macro generates (verified via
		// `swiftc -Xfrontend -dump-macro-expansions`), since `@ObservationTracked` is Apple's
		// real macro, not something this macro implements itself.
		generatedMembers.append("""
		private nonisolated func shouldNotifyObservers<Member>(_ lhs: Member, _ rhs: Member) -> Bool {
		    true
		}
		""")

		generatedMembers.append("""
		private nonisolated func shouldNotifyObservers<Member: Equatable>(_ lhs: Member, _ rhs: Member) -> Bool {
		    lhs != rhs
		}
		""")

		generatedMembers.append("""
		private nonisolated func shouldNotifyObservers<Member: AnyObject>(_ lhs: Member, _ rhs: Member) -> Bool {
		    lhs !== rhs
		}
		""")

		generatedMembers.append("""
		private nonisolated func shouldNotifyObservers<Member: Equatable & AnyObject>(_ lhs: Member, _ rhs: Member) -> Bool {
		    lhs != rhs
		}
		""")

		let cases = properties.map(\.name).joined(separator: ", ")
		generatedMembers.append("""
		private enum CodingKeys: String, CodingKey, CaseIterable {
		    case \(raw: cases)
		}
		""")

		let decodeLines = properties.map { property in
			property.isOptional
				? "self.\(property.name) = try container.decodeIfPresent(\(property.typeText).self, forKey: .\(property.name))"
				: "self.\(property.name) = try container.decode(\(property.typeText).self, forKey: .\(property.name))"
		}.joined(separator: "\n")

		generatedMembers.append("""
		\(raw: access)init(from decoder: any Decoder) throws {
		    let container = try decoder.container(keyedBy: CodingKeys.self)
		    \(raw: decodeLines)
		}
		""")

		let encodeLines = properties.map { property in
			property.isOptional
				? "try container.encodeIfPresent(\(property.name), forKey: .\(property.name))"
				: "try container.encode(\(property.name), forKey: .\(property.name))"
		}.joined(separator: "\n")

		generatedMembers.append("""
		\(raw: access)func encode(to encoder: any Encoder) throws {
		    var container = encoder.container(keyedBy: CodingKeys.self)
		    \(raw: encodeLines)
		}
		""")

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
