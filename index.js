const AWS = require('aws-sdk');

const AWS_CONFIG = {
	region: 'us-east-1',
	accountId: '000000000000',
	accessKeyId: 'accessKeyId',
	secretAccessKey: 'secretAccessKey'
}

// Localstack
const sqs = new AWS.SQS({ ...AWS_CONFIG, endpoint: 'http://localhost:4566' });
const sns = new AWS.SNS({ ...AWS_CONFIG, endpoint: 'http://localhost:4566' });

// AWS
// const sqs = new AWS.SQS({ region: 'eu-west-1' });
// const sns = new AWS.SNS({ region: 'eu-west-1' });

const NUMBER_QUEUES = 30;
const NUMBER_MESSAGES = 5;
const NUMBER_TOPICS = 1;

const queueHandlers = [];
const receiveQueueMessages = {};

let isRunning = true;

const topicArns = [];
const queueUrls = [];

(async function () {
	for (let i = 0; i < NUMBER_TOPICS; i++) {
		const topicName = `topic-${i}`;

		const { TopicArn: topicArn } = await sns.createTopic({ Name: topicName }).promise();

		topicArns.push(topicArn);

		for (let i = 0; i < NUMBER_QUEUES; i++) {
			const queueName = `${topicName}-queue-${i}`;

			receiveQueueMessages[queueName] = [];

			const { QueueUrl: queueUrl } = await sqs.createQueue(
				{
					QueueName: queueName
				}
			).promise()


			const { Attributes: { QueueArn: queueArn } } = await sqs.getQueueAttributes({
				QueueUrl: queueUrl,
				AttributeNames: ['QueueArn']
			}).promise();

			queueUrls.push(queueUrl);

			await sns.subscribe({
				Protocol: 'sqs',
				TopicArn: topicArn,
				Endpoint: queueArn,
				Attributes: {
					RawMessageDelivery: 'true'
				}
			}).promise();

			await sqs.setQueueAttributes(
				{
					QueueUrl: queueUrl,
					Attributes: {
						Policy: JSON.stringify(
							{
								Version: '2012-10-17',
								Id: `${queueArn}/SQSDefaultPolicy`,
								Statement: [
									{
										Sid: `Sid${Date.now()}`,
										Effect: 'Allow',
										'Principal': {
											AWS: '*'
										},
										Action: 'SQS:SendMessage',
										Resource: queueArn,
										Condition: {
											ArnEquals: {
												"aws:SourceArn": topicArn
											}
										}
									}
								]
							}
						)
					}
				}
			).promise();

			queueHandlers.push((async () => {
				while (isRunning) {
					const { Messages: messages } = await sqs.receiveMessage(
						{
							QueueUrl: queueUrl,
							MaxNumberOfMessages: 5,
							VisibilityTimeout: 30,
							WaitTimeSeconds: 20
						}
					).promise();

					if (!messages || !messages.length) {
						continue;
					}


					if (!receiveQueueMessages[queueName]) {
						receiveQueueMessages[queueName] = [];
					}

					receiveQueueMessages[queueName].push(...messages.map(message => Date.now() - parseInt(message.Body)));

					for (const message of messages) {
						// not await - fire and forgot
						sqs.deleteMessage(
							{
								QueueUrl: queueUrl,
								ReceiptHandle: message.ReceiptHandle
							}
						).promise();
					}
				}
			})());
		}


		await Promise.all([...new Array(NUMBER_MESSAGES)].map(async () => {
			await sns.publish(
				{
					TopicArn: topicArn,
					Message: Date.now().toString()
				}
			).promise();
		}));
	}
})();

setTimeout(async () => {
	isRunning = false;

	for (const topicQueue in receiveQueueMessages) {
		console.log(`Deliver times for ${topicQueue}:`, receiveQueueMessages[topicQueue].join(', '));
	}

	for (const queueUrl of queueUrls) {
		await sqs.purgeQueue({ QueueUrl: queueUrl }).promise();
		await sqs.deleteQueue({ QueueUrl: queueUrl }).promise();
	}

	for (const topicArn of topicArns) {
		await sns.deleteTopic({ TopicArn: topicArn }).promise();
	}
}, 30000);

