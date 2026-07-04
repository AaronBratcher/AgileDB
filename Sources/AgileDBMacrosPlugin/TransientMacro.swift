//
//  TransientMacro.swift
//  AgileDBMacrosPlugin
//

import SwiftSyntax
import SwiftSyntaxMacros

/// Pure marker macro. `ModelMacro` looks for this attribute while
/// walking a type's stored properties; it has no expansion of its own.
public struct TransientMacro: PeerMacro {
	public static func expansion(
		of node: AttributeSyntax,
		providingPeersOf declaration: some DeclSyntaxProtocol,
		in context: some MacroExpansionContext
	) throws -> [DeclSyntax] {
		return []
	}
}
