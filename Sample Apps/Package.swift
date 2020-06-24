let package = Package(
    name: "SampleApps",
    platforms: [.iOS(.v13),.macOS(.v10_15)],
    products: [ ],
    dependencies: [ ],
    targets: [
        .target(
            name: "SampleApps",
            dependencies: [],
            path: "AgileDB/AgileDB/Sample Apps")
    ]
)
