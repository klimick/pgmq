<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Postgres\PostgresQueryError;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Thesis\Time\TimeSpan;
use function Amp\delay;

#[CoversClass(Supervisor::class)]
#[CoversClass(Queue::class)]
final class PgmqTest extends TestCase
{
    private const string TESTING_MESSAGE = '{"ping": "pong"}';
    private const string TESTING_HEADERS = '{"x": "y"}';

    private Supervisor $supervisor;

    protected function setUp(): void
    {
        parent::setUp();

        $dsn = getenv('THESIS_PGMQ_DSN');

        if (!\is_string($dsn) || $dsn === '') {
            self::markTestSkipped('Set the THESIS_PGMQ_DSN environment variable.');
        }

        $this->supervisor = Supervisor::fromDsn($dsn);

        foreach ($this->supervisor->listQueues() as $queue) {
            $this->supervisor->dropQueue($queue->name);
        }
    }

    public function testValidateQueueName(): void
    {
        $this->supervisor->validateQueueName($this->randomQueueName());

        self::expectException(PostgresQueryError::class);
        self::expectExceptionMessage('queue name is too long, maximum length is 47 characters');

        $this->supervisor->validateQueueName($this->randomQueueName() . $this->randomQueueName());
    }

    public function testCreateQueue(): void
    {
        $queue = $this->supervisor->createQueue($this->randomQueueName());

        $metrics = $queue->metrics();
        self::assertSame($queue->name, $metrics->name);
        self::assertSame(0, $metrics->totalMessages);
        self::assertSame(0, $metrics->length);
        self::assertSame(0, $metrics->queueVisibleLength);
        self::assertTrue($metrics->newestMsgAge->isZero());
        self::assertTrue($metrics->oldestMsgAge->isZero());

        $metadata = $queue->metadata();

        self::assertSame($queue->name, $metadata->name);
        self::assertFalse($metadata->partitioned);
        self::assertFalse($metadata->unlogged);

        $queue->drop();

        self::expectException(QueueNotFound::class);
        $queue->metadata();
    }

    public function testCreateUnloggedQueue(): void
    {
        $queue = $this->supervisor->createUnloggedQueue($this->randomQueueName());

        $metadata = $queue->metadata();

        self::assertSame($queue->name, $metadata->name);
        self::assertFalse($metadata->partitioned);
        self::assertTrue($metadata->unlogged);

        $queue->drop();
    }

    public function testMetricsAll(): void
    {
        $this->supervisor->createQueue($this->randomQueueName());
        $this->supervisor->createQueue($this->randomQueueName());

        foreach ($this->supervisor->metrics() as $metrics) {
            self::assertSame(0, $metrics->totalMessages);
            self::assertSame(0, $metrics->length);
            self::assertSame(0, $metrics->queueVisibleLength);
            self::assertTrue($metrics->newestMsgAge->isZero());
            self::assertTrue($metrics->oldestMsgAge->isZero());
        }
    }

    public function testSendAndReadMessage(): void
    {
        $queue = $this->supervisor->createQueue($this->randomQueueName());

        $messageId = $queue->send(new SendMessage(self::TESTING_MESSAGE, self::TESTING_HEADERS));

        $message = $queue->read(TimeSpan::fromSeconds(20));
        self::assertNotNull($message);
        self::assertSame($messageId, $message->id);
        self::assertSame(self::TESTING_MESSAGE, $message->value);
        self::assertSame(self::TESTING_HEADERS, $message->headers);
    }

    public function testSendAndReadDelayedMessage(): void
    {
        $queue = $this->supervisor->createQueue($this->randomQueueName());

        $messageId = $queue->send(new SendMessage(self::TESTING_MESSAGE), delay: $delay = TimeSpan::fromSeconds(1));

        self::assertNull($queue->read());

        delay($delay->add(TimeSpan::fromMilliseconds(50))->toSeconds(PHP_ROUND_HALF_UP));

        /** @var ?Message $message */
        $message = $queue->read();
        self::assertNotNull($message);
        self::assertSame($messageId, $message->id);
        self::assertSame(self::TESTING_MESSAGE, $message->value);
    }

    public function testSendAndReadDelayedWithTimestampMessage(): void
    {
        $queue = $this->supervisor->createQueue($this->randomQueueName());

        $messageId = $queue->send(new SendMessage(self::TESTING_MESSAGE), delay: new \DateTimeImmutable('+1 seconds'));

        self::assertNull($queue->read());

        delay(1.05);

        /** @var ?Message $message */
        $message = $queue->read();
        self::assertNotNull($message);
        self::assertSame($messageId, $message->id);
        self::assertSame(self::TESTING_MESSAGE, $message->value);
    }

    public function testArchiveMessage(): void
    {
        $queue = $this->supervisor->createQueue($this->randomQueueName());

        $messageId = $queue->send(new SendMessage(self::TESTING_MESSAGE));

        /** @var ?Message $message */
        $message = $queue->read();
        self::assertNotNull($message);
        self::assertSame($messageId, $message->id);

        $queue->archive($message->id);

        self::assertNull($queue->read());
    }

    public function testDeleteMessage(): void
    {
        $queue = $this->supervisor->createQueue($this->randomQueueName());

        $messageId = $queue->send(new SendMessage(self::TESTING_MESSAGE));

        /** @var ?Message $message */
        $message = $queue->read();
        self::assertNotNull($message);
        self::assertSame($messageId, $message->id);

        $queue->delete($message->id);

        self::assertNull($queue->read());
    }

    public function testSendAndReadBatch(): void
    {
        $queue = $this->supervisor->createQueue($this->randomQueueName());

        $messageIds = $queue->sendBatch([
            new SendMessage(self::TESTING_MESSAGE, self::TESTING_HEADERS),
            new SendMessage(self::TESTING_MESSAGE, self::TESTING_HEADERS),
        ]);
        self::assertCount(2, $messageIds);

        $messages = [...$queue->readBatch(2)];
        self::assertCount(2, $messages);

        /** @var Message $message */
        foreach ($messages as $message) {
            self::assertSame(self::TESTING_MESSAGE, $message->value);
            self::assertSame(self::TESTING_HEADERS, $message->headers);
        }

        self::assertCount(0, [...$queue->readBatch(2)]);
    }

    public function testPopMessage(): void
    {
        $queue = $this->supervisor->createQueue($this->randomQueueName());

        $messageId = $queue->send(new SendMessage(self::TESTING_MESSAGE));

        $message = $queue->pop();
        self::assertNotNull($message);
        self::assertSame($messageId, $message->id);
        self::assertSame(self::TESTING_MESSAGE, $message->value);
    }

    public function testReadPoll(): void
    {
        $queue = $this->supervisor->createQueue($this->randomQueueName());

        $queue->send(new SendMessage(self::TESTING_MESSAGE));

        $messages = [...$queue->readPoll()];
        self::assertCount(1, $messages);
        self::assertSame(self::TESTING_MESSAGE, $messages[0]->value);
    }

    public function testArchiveBatch(): void
    {
        $queue = $this->supervisor->createQueue($this->randomQueueName());

        $messageIds = $queue->sendBatch([
            new SendMessage(self::TESTING_MESSAGE),
            new SendMessage(self::TESTING_MESSAGE),
        ]);
        $queue->archiveBatch($messageIds);

        $messages = [...$queue->readBatch(2)];
        self::assertCount(0, $messages);
    }

    public function testDeleteBatch(): void
    {
        $queue = $this->supervisor->createQueue($this->randomQueueName());

        $messageIds = $queue->sendBatch([
            new SendMessage(self::TESTING_MESSAGE),
            new SendMessage(self::TESTING_MESSAGE),
        ]);
        $queue->deleteBatch($messageIds);

        $messages = [...$queue->readBatch(2)];
        self::assertCount(0, $messages);
    }

    public function testPurgeQueue(): void
    {
        $queue = $this->supervisor->createQueue($this->randomQueueName());

        $queue->send(new SendMessage(self::TESTING_MESSAGE));

        self::assertSame(1, $queue->metrics()->length);

        self::assertSame(1, $queue->purge());

        self::assertSame(0, $queue->metrics()->length);
    }

    /**
     * @return non-empty-string
     */
    private function randomQueueName(): string
    {
        /** @var non-empty-string */
        return substr(bin2hex(random_bytes(30)), 0, length: 30);
    }
}
