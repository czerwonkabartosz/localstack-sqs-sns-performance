const AWS = require('aws-sdk');

const AWS_CONFIG = {
	region: 'us-east-1',
	accountId: '000000000000',
	accessKeyId: 'accessKeyId',
	secretAccessKey: 'secretAccessKey'
}

const sqs = new AWS.SQS({ ...AWS_CONFIG, endpoint: 'http://localhost:4566' });
const sns = new AWS.SNS({ ...AWS_CONFIG, endpoint: 'http://localhost:4566' });

const NUMBER_QUEUES = 10;
const NUMBER_MESSAGES = 10;
const NUMBER_TOPICS = 3;

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

			const { QueueUrl: queueUrl } = await sqs.createQueue(
				{
					QueueName: queueName
				}
			).promise()

			queueUrls.push(queueUrl);

			await sns.subscribe({
				Protocol: 'sqs',
				TopicArn: topicArn,
				Endpoint: queueUrl,
				Attributes: {
					RawMessageDelivery: 'true'
				}
			}).promise();

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
						await sqs.deleteMessage(
							{
								QueueUrl: queueUrl,
								ReceiptHandle: message.ReceiptHandle
							}
						).promise();
					}
				}
			})());
		}


		for (let i = 0; i < NUMBER_MESSAGES; i++) {
			await sns.publish(
				{
					TopicArn: topicArn,
					Message: Date.now().toString()
				}
			).promise();
		}
	}
})();

setTimeout(async () => {
	isRunning = false;

	for (const queueUrl of queueUrls) {
		await sqs.purgeQueue({ QueueUrl: queueUrl }).promise();
		await sqs.deleteQueue({ QueueUrl: queueUrl }).promise();
	}

	for (const topicArn of topicArns) {
		await sns.deleteTopic({ TopicArn: topicArn }).promise();
	}

	for (const topicQueue in receiveQueueMessages) {
		console.log(`Deliver times for ${topicQueue}:`, receiveQueueMessages[topicQueue].join(', '));
	}
}, 30000);