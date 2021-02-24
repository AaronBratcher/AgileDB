Pod::Spec.new do |s|
  s.name         = "AgileDB"
  s.version      = "6.2.0"
  s.summary      = "Save and retrieve full object graphs to SQLite"
  s.homepage	 = "https://github.com/AaronBratcher/AgileDB"

  s.license      = "MIT"
  s.author             = { "Aaron Bratcher" => "aaronlbratcher@yahoo.com" }
  s.social_media_url   = "http://twitter.com/AaronLBratcher"

  s.swift_version = '5.2'
  
  s.frameworks = 'Foundation'
  
  s.source				= { :git => "https://github.com/AaronBratcher/AgileDB.git", :tag => s.version }
  s.source_files	= "AgileDB", "AgileDB/AgileDB/**/*.{h,m,swift}"
  
  s.library				= "sqlite3"
end
