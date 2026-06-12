// Creates the `metrics` collection in the analytics database with a
// $jsonSchema validator. Run with:
//   mongosh "mongodb://localhost:27017/analytics" /mongo/create_metrics_collection.js
//
// Drops and recreates the collection so it can be re-run cleanly.

const targetDb = db.getSiblingDB("analytics");

if (targetDb.getCollectionNames().includes("metrics")) {
  targetDb.metrics.drop();
}

targetDb.createCollection("metrics", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: [
        "metric_type",
        "website",
        "campaign_id",
        "date_time",
        "value",
        "device",
        "country",
        "user_id",
        "browser",
      ],
      properties: {
        metric_type: {
          bsonType: "string",
          enum: ["click", "call", "impression"],
          description: "must be one of click, call, or impression",
        },
        website: {
          bsonType: "string",
          enum: ["example.com", "sample.com", "filler.com"],
          description: "must be one of the allowed websites",
        },
        // referrer is intentionally unenforced (free-form path string)
        referrer: {
          bsonType: "string",
        },
        campaign_id: {
          bsonType: "int",
          minimum: 1,
          maximum: 5,
          description: "must be an integer 1-5",
        },
        date_time: {
          bsonType: "date",
          description: "must be a BSON date",
        },
        value: {
          bsonType: ["double", "int"],
          description: "numeric metric value (cost, duration, or 1)",
        },
        device: {
          bsonType: "string",
          enum: ["desktop", "mobile", "tablet"],
        },
        country: {
          bsonType: "string",
          enum: ["US", "CA", "UK", "DE", "AU"],
        },
        user_id: {
          bsonType: "int",
          minimum: 1,
          maximum: 10000,
        },
        session_id: {
          bsonType: "string",
        },
        browser: {
          bsonType: "string",
          enum: ["chrome", "safari", "firefox", "edge"],
        },
      },
    },
  },
  validationLevel: "strict",
  validationAction: "error",
});

print("Created 'metrics' collection in 'analytics' database with validator.");
