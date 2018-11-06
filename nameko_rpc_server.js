#!/usr/bin/env node
const amqp = require('amqplib/callback_api');


const config = {
  AMQP_HOST: 'amqp://localhost',
  EXCHANGE: 'nameko-rpc',
  SERVICE_NAME: 'meetup.pizza_model',
};

const pizzaFunc = (headCount, flakeFactor = 0.35) => {
  const showUpNumber = parseInt(headCount) * flakeFactor;
  return showUpNumber / 2;
};

// parses nameko response for args and kwargs
const parseRequestContent = msg => JSON.parse(msg.content.toString());

const businessLogic = (msg) => {
  const { args } = parseRequestContent(msg);
  const [headCount] = args;
  return pizzaFunc(headCount);
};

const constructReplyQueue = (msg) => {
  if (msg.properties && msg.properties.headers) {
    const { headers } = msg.properties;

    const services = headers['nameko.call_id_stack'];

    if (services.length > 0) {
      return services[0].split('.').shift();
    }
  }
  return '';
};

amqp.connect(config.AMQP_HOST, (err, conn) => {
  conn.createChannel((err, channel) => {
    const exchange = config.EXCHANGE;

    channel.assertExchange(exchange, 'topic', { durable: true });

    channel.assertQueue('', { exclusive: true }, (err, queueObj) => {
      console.log(' [*] Waiting for logs. To exit press CTRL+C');

      channel.bindQueue(queueObj.queue, exchange, config.SERVICE_NAME);

      channel.consume(queueObj.queue, (msg) => {
        console.log(" [x] %s:'%s'", msg.fields.routingKey, msg.content.toString());

        const serviceName = constructReplyQueue(msg);
        const replyQueue = `rpc.reply-${serviceName}-${msg.properties.replyTo}`;
        const result = businessLogic(msg);

        channel.sendToQueue(replyQueue,
          Buffer.from(JSON.stringify({ result })),
          { correlationId: msg.properties.correlationId, contentType: 'application/json' });
      }, { noAck: true });
    });
  });
});
