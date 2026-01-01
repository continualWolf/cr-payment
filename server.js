const redis = require('redis');

let client;
let subscriber;

/**
 * Initialize Redis with retry loop
 */
async function initRedis() {
  const url = process.env.REDIS_URL || 'redis://localhost:6379';

  while (true) {
    try {
      client = redis.createClient({ url });
      client.on('error', err => console.error('Redis Client Error', err));

      await client.connect();

      subscriber = client.duplicate();
      await subscriber.connect();

      await subscriber.subscribe('bookingCompleted', async (message) => {
        try {
          await handleBookingCompletedEvent(message);
        } catch (err) {
          console.error('Error handling bookingCompleted event:', err);
        }
      });

      console.log('Redis initialized.');
      break;
    } catch (err) {
      console.error('Redis connection failed, retrying in 2s...', err.message);

      // Clean up partially opened connections
      if (subscriber?.isOpen) await subscriber.quit();
      if (client?.isOpen) await client.quit();

      await new Promise(res => setTimeout(res, 2000));
    }
  }
}

/**
 * Handle booking completed event
 */
async function handleBookingCompletedEvent(message) {
  let data;

  try {
    data = JSON.parse(message);
  } catch {
    console.error('Invalid message format:', message);
    return;
  }

  const { price, cardNumber, expiry, cvv } = data;

  console.log('Processing payment:', { price });

  // POC: pretend payment succeeds
  if (!client?.isOpen) {
    console.error('Redis not ready');
    return;
  }

  await client.publish(
    'paymentCompleted',
    JSON.stringify({
      status: 'Payment complete',
      amount: price,
      timestamp: Date.now()
    })
  );

  console.log('Published paymentCompleted event');
}

/**
 * Graceful shutdown
 */
async function shutdown() {
  console.log('Shutting down...');

  try {
    if (subscriber?.isOpen) {
      await subscriber.unsubscribe('bookingCompleted');
      await subscriber.quit();
    }

    if (client?.isOpen) {
      await client.quit();
    }
  } catch (err) {
    console.error('Shutdown error:', err);
  } finally {
    process.exit(0);
  }
}

/**
 * App bootstrap
 */
(async () => {
  await initRedis();
  console.log('Service started.');

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
})();
