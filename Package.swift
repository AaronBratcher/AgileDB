// swift-tools-version: 6.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription
import CompilerPluginSupport

let package = Package(
	name: "AgileDB",
	platforms: [
		.iOS(.v18), .macOS(.v12), .tvOS(.v18), .watchOS(.v9)
	],
	products: [
		.library(
			name: "AgileDB",
			targets: ["AgileDB"]),
	],
	dependencies: [
		.package(url: "https://github.com/swiftlang/swift-syntax.git", "600.0.0"..<"700.0.0"),
	],
	targets: [
		.macro(
			name: "AgileDBMacrosPlugin",
			dependencies: [
				.product(name: "SwiftSyntax", package: "swift-syntax"),
				.product(name: "SwiftSyntaxMacros", package: "swift-syntax"),
				.product(name: "SwiftSyntaxBuilder", package: "swift-syntax"),
				.product(name: "SwiftDiagnostics", package: "swift-syntax"),
				.product(name: "SwiftCompilerPlugin", package: "swift-syntax"),
			]
		),
		.target(
			name: "AgileDB",
			dependencies: ["AgileDBMacrosPlugin"],
		),
		.testTarget(
			name: "AgileDBTests",
			dependencies: ["AgileDB"]),
		.testTarget(
			name: "AgileDBMacrosPluginTests",
			dependencies: [
				"AgileDBMacrosPlugin",
				.product(name: "SwiftSyntaxMacros", package: "swift-syntax"),
				.product(name: "SwiftSyntaxMacrosTestSupport", package: "swift-syntax"),
			]
		),
	]
)
