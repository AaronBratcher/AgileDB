// swift-tools-version: 6.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

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
	dependencies: [],
	targets: [
		.target(
			name: "AgileDB",
			dependencies: [],
			swiftSettings: [
				// Use Swift 5 language mode to avoid strict Sendable enforcement on AnyObject-based
				// public API types (DBRow, [String: AnyObject]). The actor model is fully supported
				// in Swift 5 language mode and provides all isolation guarantees.
				.swiftLanguageMode(.v5),
			]
		),
		.testTarget(
			name: "AgileDBTests",
			dependencies: ["AgileDB"]),
	]
)
