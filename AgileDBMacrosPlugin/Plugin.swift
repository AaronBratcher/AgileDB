//
//  Plugin.swift
//  AgileDBMacrosPlugin
//

import SwiftCompilerPlugin
import SwiftSyntaxMacros

@main
struct AgileDBMacrosPluginProvider: CompilerPlugin {
	let providingMacros: [Macro.Type] = [
		ModelMacro.self,
		TransientMacro.self,
		PredicateMacro.self,
	]
}
