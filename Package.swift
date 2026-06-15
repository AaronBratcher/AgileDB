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
		),
		.testTarget(
			name: "AgileDBTests",
			dependencies: ["AgileDB"]),
	]
)
