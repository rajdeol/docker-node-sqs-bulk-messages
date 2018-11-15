const Consumer = require('sqs-consumer');

// initiate AWS SQS SDK
const sqs = require(process.env.APP_SERVICES_DIR + 'sqs')(
  process.env.SQS_ACCESS_KEY,
  process.env.SQS_SECRET_KEY,
  process.env.SQS_REGION,
  process.env.SQS_API_VERSION
);

// initiate logger
const logger = require(process.env.APP_SERVICES_DIR + 'logger');

// initiate Handler
const handleMessage = require(process.env.APP_SERVICES_DIR + 'handler');

// consumer array
var consumers = [];

// default consumer instance to run
var number_of_consumers = 1;

// check if we have to create multiple consumers
if (process.env.NUMBER_OF_CONSUMERS) {
  number_of_consumers = process.env.NUMBER_OF_CONSUMERS;
}

// Create five consumers
for (i = 0; i < number_of_consumers; i++) {
  consumers[i] = createConsumer(i);
}

function createConsumer(i) {
  //initiate sqs-consumer
  const app = Consumer.create({
    sqs: sqs,
    queueUrl: process.env.SQS_QUEUE_URL,
    visibilityTimeout: process.env.SQS_VISIBILITY_TIMEOUT,
    terminateVisibilityTimeout: true,
    attributeNames: ['All'],
    batchSize: 10,
    handleMessage
  });

  // attach event handlers to the sqs-consumer to log messages and track if there is any error
  app.on('error', err => {
    logger.error('Consumer - ' + i + ': Error in Consumer : ' + err.message);
    // stop the polling
    app.stop();
  });

  app.on('message_received', message => {
    // logger.info(
    //   'Consumer - ' + i + ':message received from queue : ' + message.Body
    // );
  });

  app.on('message_processed', message => {
    logger.info('Consumer - ' + i + ':message processed : ' + message.Body);
  });

  app.on('response_processed', () => {
    logger.info('Consumer - ' + i + ':Batch of messages processed');
  });

  app.on('empty', () => {
    logger.info('Consumer - ' + i + ':Queue is Empty');
  });

  app.on('stopped', () => {
    logger.info('Consumer - ' + i + ':Consumer has stopped');
    // delete the consumer if it has stopped
    delete consumers[i];
    // check if all consumers are deleted then exit
    for (key in consumers) {
      if (key != 'undefined') {
        return;
      }
    }
    process.exit(1);
  });
  logger.info('Starting the consumer - ' + i);
  app.start();

  return app;
}
