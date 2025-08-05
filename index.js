const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { DynamoDBClient, PutItemCommand } = require('@aws-sdk/client-dynamodb');
const axios = require('axios');
const crypto = require('crypto');
const { performance } = require('perf_hooks');


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

// SNS alert helper
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
async function logTradeToDynamo({ firmId, accountId, action, symbol, qty, fillPrice, errorMessage, orderId, strategy, executionTime, suggestedPrice }) {
  const item = {
    tradeId: { S: generateTradeId() },
    timestamp: { S: new Date().toISOString() },
    firmId: { S: firmId },
    accountId: { S: accountId.toString() },
    action: { S: action },
    strategy: strategy ? { S: strategy } : { NULL: true },
    symbol: { S: symbol },
    orderId: { S: orderId.toString() },
    fillPrice: fillPrice != null
      ? { N: fillPrice.toString() }
      : { NULL: true },
    suggestedPrice: suggestedPrice != null ? { N: suggestedPrice.toString() } : { NULL: true },
    quantity: { N: qty.toString() },
    note: errorMessage ? { S: errorMessage } : { NULL: true },
    executionTime: executionTime != null ? { N: executionTime.toString() } : { NULL: true },

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

async function isTradovateOrderFilled(orderId) {
  const token = await getTradovateAccessToken();
  try {
    const { data: fills } = await axios.get(
      process.env.TRADOVATE_FILL_ORDERS,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    return fills.some(fill => String(fill.orderId) === String(orderId));
  } catch (err) {
    console.error('Error fetching Tradovate fills:', err.response?.data || err.message);
    return false;
  }
}

const tokenCache = {};
async function getDynamicToken(firmId, cfg) {
  const now = Date.now();
  if (tokenCache[firmId] && tokenCache[firmId].expiry > now) {
    return tokenCache[firmId].token;
  }

  const cred = cfg.FirmCredentials?.[firmId];
  if (!cred) throw new Error(`Missing credentials for ${firmId}`);

  const { tokenEndpoint, userName, apiKey } = cred;
  const { data } = await axios.post(tokenEndpoint, { userName, apiKey }, {
    headers: { 'Content-Type': 'application/json' }
  });

  const token = data.token || data.accessToken;
  if (!token) throw new Error(`No token returned from ${firmId}`);

  tokenCache[firmId] = {
    token,
    expiry: now + ((data.expiresIn || 600) * 1000)
  };
  return token;
}

// Fetch contractId 
async function fetchContractIdFromTheFuturesDesk(symbol, cfg) {
  const token = await getDynamicToken('thefuturesdesk', cfg);
  const url = process.env.CONTRACT_ID_URL;

  let data;
  try {
    const response = await axios.post(url, { live: false }, {
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`
      }
    });
    data = response.data;
  } catch (err) {
    console.error(`Failed to fetch contract ID from TheFuturesDesk for symbol "${symbol}":`, err.response?.data || err.message);
    throw new Error(`Contract ID fetch failed for ${symbol}`);
  }

  const contracts = data.contracts || [];
  const contract = contracts.find(c => c.symbolId === symbol || c.name === symbol);
  if (!contract) throw new Error(`No contract found for symbol: ${symbol}`);
  return contract.id;
}

// Payload builders
const payloadBuilders = {
  projectx: (firm, order, accountId, finalQty) => {
    if (!firm.contractId) { throw new Error(`Missing contractId for firm ${firm.firmId}`); }
    return {
      contractId: firm.contractId,
      accountId,
      type: order.orderType === 'Market' ? 2 : 1,
      side: order.action.toLowerCase() === 'buy' ? 0 : 1,
      size: finalQty
    };
  },
  tradovate: (firm, order, accountId, finalQty) => {
    const accountSpec = firm.accountSpec;
    if (!accountSpec) { throw new Error(`Missing accountSpec for firm ${firm.firmId}`); }
    return {
      accountId,
      accountSpec,
      action: order.action,
      symbol: order.symbol,
      orderQty: finalQty,
      orderType: order.orderType,
      isAutomated: true
    };
  }
};

// fetching fill prices for projectXFirms
async function fetchFillPriceFromConfig(firmId, accountId, orderId, config, token) {
  const url = config.PriceFillUrls?.[firmId];
  if (!url) throw new Error(`Fill endpoint not configured for ${firmId}`);

  const headers = {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  };

  const body = { accountId };
  const res = await axios.post(url, body, { headers });
  const trades = res.data.trades || [];
  const match = trades.find(t => String(t.orderId) === String(orderId));
  return match?.price || null;
}
// Main Lambda handler
exports.handler = async (event) => {
  const lambdaStart = performance.now()
  let body;
  try {
    body = JSON.parse(event.body || '{}');
    console.info('Incoming request :', body);
  } catch (err) {
    console.error('Invalid JSON received:', err.message);
    return { statusCode: 400, body: 'Invalid JSON' };
  }

  const REQUIRED_PASSWORD = process.env.REQUIRED_PASSWORD;
  if (body.password !== REQUIRED_PASSWORD) {
    return {
      statusCode: 401,
      body: JSON.stringify({ message: 'Invalid password. Access denied.' })
    };
  }

  let orders = [];
  if (Array.isArray(body.orders)) {
    orders = body.orders;
  } else if (body.symbol && body.orderQty && body.orderType && body.action) {
    orders = [body];
  } else { return { statusCode: 400, body: 'Missing or invalid order input' }; }

  let cfg;
  try {
    cfg = await loadConfig();
  } catch (err) {
    console.error('Failed to load config from S3:', err.message);
    return {
      statusCode: 500,
      body: 'Server error: Could not load configuration'
    };
  }

  for (const order of orders) {
    try {
      validateOrder(order);
    } catch (err) {
      return { statusCode: 400, body: err.message };
    }
    order.symbol = order.symbol.toUpperCase();
    order.action = order.action.charAt(0).toUpperCase() + order.action.slice(1).toLowerCase();
    order.orderType = order.orderType.charAt(0).toUpperCase() + order.orderType.slice(1).toLowerCase();
    const { symbol, orderQty, action, orderType, price } = order;
    const firms = cfg[symbol] || [];
    console.info(`Found ${firms.length} firm(s) configured for symbol "${symbol}":`, firms.map(f => f.firmId));

    if (!firms.length) {
      const msg = `No trading firm is configured for symbol "${symbol}".`;
      console.warn(msg)
      return {
        statusCode: 400,
        body: JSON.stringify({ message: msg })
      }
    }

    const projectXFirms = firms.filter(firm => Object.keys(cfg.Urls.projectx || {}).includes(firm.firmId));
    if (projectXFirms.length) {
      try {
        const sharedContractId = await fetchContractIdFromTheFuturesDesk(symbol, cfg);
        projectXFirms.forEach(firm => {
          firm.contractId = sharedContractId;
        });
      } catch (err) {
        console.error('Could not fetch shared ProjectX contractId:', err.message);
      }
    }

    for (const firm of firms) {
      const projxUrlMap = cfg.Urls.projectx || {};
      const isProjectXSub = Boolean(projxUrlMap[firm.firmId]);
      const builderKey = isProjectXSub ? 'projectx' : firm.firmId;
      const builder = payloadBuilders[builderKey];

      if (!builder) {
        console.warn(`Configuration for firm "${firm.firmId}" is incomplete or unsupported.`);
        continue;
      }
      const handlerStart = performance.now();

      const baseUrl = isProjectXSub
        ? projxUrlMap[firm.firmId]
        : cfg.Urls[firm.firmId];
      if (!baseUrl) {
        console.error(`Missing URL for firm ${firm.firmId}`);
        continue;
      }

      for (const accountId of firm.accountIds) {
        const finalQty = firm.size * orderQty;
        let orderPayload;
        try {
          orderPayload = builder(firm, order, accountId, finalQty);
        } catch (err) {
          console.error(`Payload error for ${firm.firmId}/${accountId}:`, err.message);
          continue;
        }

        const headers = { 'Content-Type': 'application/json' };
        if (firm.firmId === 'tradovate') {
          headers.Authorization = `Bearer ${await getTradovateAccessToken()}`;
        } else if (cfg.FirmCredentials?.[firm.firmId]) {
          headers.Authorization = `Bearer ${await getDynamicToken(firm.firmId, cfg)}`;
        }
        let success = false, errMsg = null;
        let lastError = 'Unknown error';
        let lastResponse = null;
        for (let attempt = 1; attempt <= 3; attempt++) {
          const tradeStart = performance.now()
          try {
            const resp = await axios.post(baseUrl, orderPayload, { headers });
            console.info(`Sending order to ${firm.firmId}/${accountId}:`, {
              url: baseUrl,
              payload: orderPayload
            });
            console.log('Order response:', resp.data)
            const orderId = resp.data.orderId || resp.data.id || resp.data.OrderID;
            let fillPrice = null;

            if (firm.firmId === 'tradovate' && orderId) {
              try {
                const fillResp = await axios.get(
                  `${process.env.FILL_URL}?masterid=${orderId}`,
                  { headers }
                );

                const fills = fillResp.data;
                if (Array.isArray(fills) && fills.length > 0) {
                  fillPrice = fills[0].price;
                }
              } catch (err) {
                console.error('Error fetching fill price (Tradovate):', err.response?.data || err.message);
              }
            }

            else if (cfg.PriceFillUrls?.[firm.firmId] && orderId) {
              try {
                const dynamicToken = await getDynamicToken(firm.firmId, cfg);

                fillPrice = await fetchFillPriceFromConfig(
                  firm.firmId,
                  accountId,
                  orderId,
                  cfg,
                  dynamicToken
                );

              } catch (err) {
                console.warn(`Fill price not found for ${firm.firmId}/${accountId}:`, err.message);
              }
            }

            if (resp.data?.success !== false) {
              let shouldNotify = true;
              if (firm.firmId === 'tradovate') {
                const isFilled = await isTradovateOrderFilled(orderId);
                if (!isFilled) {
                  console.warn(`Tradovate order ${orderId} not found in fill list—skipping success alert.`);
                  shouldNotify = false;
                }
              }

              if (shouldNotify) {
                success = true;
                console.info(
                  `[ ORDER SUCCESS] ${firm.firmId}/${accountId} | ${action} ${finalQty} ${symbol} @ ${fillPrice || 'MKT'} | OrderID: ${orderId || 'N/A'}`
                );
                const tradeEnd = performance.now();
                const tradeExecutionTime = tradeEnd - tradeStart;
                await sendAlert(
                  'Trade Executed',
                  `Trade executed successfully\nFirm ID: ${firm.firmId}\nAccount ID: ${accountId}\nAction: ${action} ${finalQty} ${symbol} @ ${fillPrice || 'MKT'}\nOrder ID: ${orderId || 'N/A'}`
                );
                const executionTime = Number((performance.now() - handlerStart).toFixed(2));

                await logTradeToDynamo({
                  firmId: firm.firmId,
                  accountId,
                  action,
                  symbol,
                  qty: finalQty,
                  fillPrice: fillPrice,
                  suggestedPrice: order.price,
                  status: 'SUCCESS',
                  orderId,
                  strategy: order.strategy,
                  executionTime

                });
                break;
              }
            }

            throw new Error(resp.data.errorMessage || resp.data.message || 'Unknown failure');
          } catch (err) {
            if (err.response) {
              errMsg = JSON.stringify(err.response.data);
              console.error(`Attempt ${attempt} HTTP ${err.response.status}:`, err.response.data);
            } else {
              errMsg = err.message;
              console.error(`Attempt ${attempt} error:`, errMsg);
            }
            if (attempt < 3) await delay(2 ** attempt * 300);
          }
        }

        if (!success) {
          const contractNote = firm.contractId ? ` | Contract: ${firm.contractId}` : '';
          console.error(`Unfortunately all the retries failed for your order for: Firm-ID: ${firm.firmId}, Acc-ID: ${accountId}${contractNote}`);

          await sendAlert(
            'Trade Failure - Retries Exhausted',
            `Order failed: Tried multiple times but could not place the order for Firm: ${firm.firmId}, Account: ${accountId}${contractNote}`
          );

          await logTradeToDynamo({
            firmId: firm.firmId,
            accountId,
            action,
            symbol,
            qty: finalQty,
            fillPrice: null,
            suggestedPrice: order.price,
            errorMessage: errMsg,
            strategy: order.strategy,
            orderId: null,
            executionTime,

          });
        }

      }
    }

  }
  const lambdaEnd = performance.now();
  const totalLambdaExecutionTime = (lambdaEnd - lambdaStart).toFixed(2);
  console.info(`Total execution time: ${totalLambdaExecutionTime} ms`);
  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message: 'Trade Executed.' })
  };
};
