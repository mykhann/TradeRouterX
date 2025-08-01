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

// DynamoDB logger
async function logTradeToDynamo({ firmId, accountId, action, symbol, qty, fillPrice, errorMessage, orderId }) {
  const item = {
    tradeId: { S: generateTradeId() },
    timestamp: { S: new Date().toISOString() },
    firmId: { S: firmId },
    accountId: { S: accountId.toString() },
    action: { S: action },
    symbol: { S: symbol },
    orderId: { S: orderId.toString() },
    fillPrice: fillPrice != null
      ? { N: fillPrice.toString() }
      : { NULL: true },
    quantity: { N: qty.toString() },
    note: errorMessage ? { S: errorMessage } : { NULL: true }
  };
  try {
    await dynamo.send(new PutItemCommand({
      TableName: process.env.DYNAMO_TABLE,
      Item: item
    }));
  } catch (err) {
    console.error('DynamoDB write failed', err.message);
  }
}

// Tradovate token caching
let tradovateToken = null, tokenExpiry = 0;
async function getTradovateAccessToken() {
  if (tradovateToken && tokenExpiry > Date.now()) {
    return tradovateToken;
  }
  const authPayload = {
    name: process.env.TRADOVATE_USERNAME,
    password: process.env.TRADOVATE_PASSWORD,
    appId: process.env.TRADOVATE_APP_ID,
    appVersion: process.env.TRADOVATE_APP_VERSION,
    deviceId: process.env.TRADOVATE_DEVICE_ID,
    cid: process.env.TRADOVATE_CLIENT_ID,
    sec: process.env.TRADOVATE_CLIENT_SECRET
  };
  const { data } = await axios.post(process.env.TRADOVATE_AUTH_URL, authPayload);
  tradovateToken = data.accessToken;
  tokenExpiry = Date.now() + (data.expirationTime || 8 * 60 * 1000);
  return tradovateToken;
}


  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message: 'Trade Executed.' })
  };
;
