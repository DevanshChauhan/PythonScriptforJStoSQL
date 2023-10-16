BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "deviceInfo" (
	"osType"	TEXT,
	"deviceId"	TEXT,
	"deviceType"	TEXT,
	PRIMARY KEY("deviceId")
);
CREATE TABLE IF NOT EXISTS "promotedTweetInfo" (
	"tweetId"	INTEGER,
	"tweetText"	TEXT,
	"urls"	TEXT,
	"mediaUrls"	TEXT,
	PRIMARY KEY("tweetId")
);
CREATE TABLE IF NOT EXISTS "advertiserInfo" (
	"advertiserName"	TEXT,
	"screenName"	TEXT,
	PRIMARY KEY("advertiserName")
);
CREATE TABLE IF NOT EXISTS "TargetingCriteria" (
	"id"	INTEGER,
	"targetingType"	TEXT,
	"targetingValue"	TEXT,
	PRIMARY KEY("id")
);
CREATE TABLE IF NOT EXISTS "matchedTargetingCriteria" (
	"impression"	INTEGER,
	"criteria"	INTEGER,
	FOREIGN KEY("criteria") REFERENCES "TargetingCriteria"("id"),
	FOREIGN KEY("impression") REFERENCES "impressions"("id")
);
CREATE TABLE IF NOT EXISTS "impressions" (
	"id"	INTEGER,
	"deviceInfo"	TEXT,
	"displayLocation"	TEXT,
	"promotedTweetInfo"	INTEGER,
	"impressionTime"	TEXT,
	"advertiserInfo"	TEXT,
	FOREIGN KEY("deviceInfo") REFERENCES "deviceInfo"("deviceId"),
	FOREIGN KEY("promotedTweetInfo") REFERENCES "promotedTweetInfo"("tweetId")
);
COMMIT;
