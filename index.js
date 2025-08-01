const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { DynamoDBClient, PutItemCommand } = require('@aws-sdk/client-dynamodb');
const axios = require('axios');
const crypto = require('crypto');

// AWS SDK clients
const s3 = new S3Client();
const sns = new SNSClient();
const dynamo = new DynamoDBClient();
const CONFIG_BUCKET = process.env.CONFIG_BUCKET;
const CONFIG_KEY = process.env.CONFIG_KEY;
const SNS_TOPIC_ARN = process.env.SNS_TOPIC_ARN;


let configCache = null;
// Validate only the common, user‑provided fields
function validateOrder(order) {
  const required = ['symbol', 'orderQty', 'orderType', 'action'];
  for (const field of required) {
    if (order[field] == null) {
      throw new Error(`Missing required field: ${field}`);
    }
  }
}



  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message: 'Trade Executed.' })
  };
;
