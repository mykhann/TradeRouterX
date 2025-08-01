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

// Load & cache config
async function loadConfig() {
  if (configCache) return configCache;
  const data = await s3.send(new GetObjectCommand({
    Bucket: CONFIG_BUCKET,
    Key: CONFIG_KEY
  }));
  const body = await new Promise((res, rej) => {
    const chunks = [];
    data.Body.on('data', c => chunks.push(c));
    data.Body.on('error', rej);
    data.Body.on('end', () => res(Buffer.concat(chunks).toString('utf8')));
  });
  configCache = JSON.parse(body);
  return configCache;
}

async function sendAlert(subject, message) {
  await sns.send(new PublishCommand({
    TopicArn: SNS_TOPIC_ARN,
    Subject: subject,
    Message: message
  }));
}
// Exponential backoff
const delay = ms => new Promise(res => setTimeout(res, ms));
// Unique trade ID
const generateTradeId = () => crypto.randomBytes(8).toString('hex');

  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message: 'Trade Executed.' })
  };
;
