<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnectionPool;
use Amp\Postgres\PostgresLink;
use Amp\Postgres\PostgresQueryError;
use Thesis\Time\TimeSpan;

/**
 * @api
 */
final readonly class Supervisor
{
    public function __construct(
        private PostgresLink $pg,
    ) {
        createExtension($pg);
    }

    /**
     * @param non-empty-string $dsn
     */
    public static function fromDsn(string $dsn): self
    {
        return new self(
            new PostgresConnectionPool(
                PostgresConfig::fromString($dsn),
            ),
        );
    }

    /**
     * @param non-empty-string $queue
     * @throws PostgresQueryError if queue name is invalid
     */
    public function validateQueueName(string $queue): void
    {
        validateQueueName($this->pg, $queue);
    }

    /**
     * @param non-empty-string $queue
     */
    public function createQueue(string $queue): Queue
    {
        return createQueue($this->pg, $queue);
    }

    /**
     * @param non-empty-string $queue
     */
    public function createUnloggedQueue(string $queue): Queue
    {
        return createUnloggedQueue($this->pg, $queue);
    }

    /**
     * @param non-empty-string $queue
     * @param non-negative-int|non-empty-string $partitionInterval this can be either any valid Postgres Duration supported by pg_partman, or an integer value
     * @param non-negative-int|non-empty-string $retentionInterval this can be either any valid Postgres Duration supported by pg_partman, or an integer value
     */
    public function createPartitionedQueue(
        string $queue,
        int|string $partitionInterval,
        int|string $retentionInterval,
    ): Queue {
        return createPartitionedQueue($this->pg, $queue, $partitionInterval, $retentionInterval);
    }

    /**
     * @return iterable<QueueMetadata>
     */
    public function listQueues(): iterable
    {
        return listQueues($this->pg);
    }

    /**
     * @param non-empty-string $queue
     */
    public function dropQueue(string $queue): bool
    {
        return dropQueue($this->pg, $queue);
    }

    /**
     * @param non-empty-string $queue
     */
    public function purgeQueue(string $queue): int
    {
        return purgeQueue($this->pg, $queue);
    }

    /**
     * @param non-empty-string $queue
     * @throws QueueNotFound
     */
    public function queueMetrics(string $queue): QueueMetric
    {
        return queueMetrics($this->pg, $queue);
    }

    /**
     * @param non-empty-string $queue
     * @throws QueueNotFound
     */
    public function queueMetadata(string $queue): QueueMetadata
    {
        return queueMetadata($this->pg, $queue);
    }

    /**
     * @return iterable<QueueMetric>
     */
    public function metrics(): iterable
    {
        return metrics($this->pg);
    }

    /**
     * @param non-empty-string $queue
     * @param non-empty-string $json
     * @return int the message id, unique to the queue, is returned
     */
    public function send(string $queue, string $json, null|TimeSpan|\DateTimeImmutable $delay = null): int
    {
        return send($this->pg, $queue, $json, $delay);
    }

    /**
     * @param non-empty-string $queue
     * @param non-empty-list<non-empty-string> $messages
     * @return list<int>
     */
    public function sendBatch(string $queue, array $messages, null|TimeSpan|\DateTimeImmutable $delay = null): array
    {
        return sendBatch($this->pg, $queue, $messages, $delay);
    }

    /**
     * @param non-empty-string $queue
     * @param positive-int $batch
     * @return iterable<Message>
     */
    public function readPoll(
        string $queue,
        int $batch = 1,
        ?TimeSpan $visibilityTimeout = null,
        ?TimeSpan $maxPoll = null,
        ?TimeSpan $pollInterval = null,
    ): iterable {
        return readPoll($this->pg, $queue, $batch, $visibilityTimeout, $maxPoll, $pollInterval);
    }

    /**
     * @param non-empty-string $queue
     */
    public function read(string $queue, ?TimeSpan $visibilityTimeout = null): ?Message
    {
        return read($this->pg, $queue, $visibilityTimeout);
    }

    /**
     * @param non-empty-string $queue
     * @param positive-int $count
     * @return iterable<Message>
     */
    public function readBatch(string $queue, int $count, ?TimeSpan $visibilityTimeout = null): iterable
    {
        return readBatch($this->pg, $queue, $count, $visibilityTimeout);
    }

    /**
     * @param non-empty-string $queue
     */
    public function pop(string $queue): ?Message
    {
        return pop($this->pg, $queue);
    }

    /**
     * @param non-empty-string $queue
     */
    public function archive(string $queue, int $messageId): bool
    {
        return archive($this->pg, $queue, $messageId);
    }

    /**
     * @param non-empty-string $queue
     * @param list<int> $messageIds
     * @return list<int>
     */
    public function archiveBatch(string $queue, array $messageIds): array
    {
        return archiveBatch($this->pg, $queue, $messageIds);
    }

    /**
     * @param non-empty-string $queue
     */
    public function detachArchive(string $queue): void
    {
        detachArchive($this->pg, $queue);
    }

    /**
     * @param non-empty-string $queue
     */
    public function delete(string $queue, int $messageId): bool
    {
        return delete($this->pg, $queue, $messageId);
    }

    /**
     * @param non-empty-string $queue
     * @param list<int> $messageIds
     * @return list<int>
     */
    public function deleteBatch(string $queue, array $messageIds): array
    {
        return deleteBatch($this->pg, $queue, $messageIds);
    }

    /**
     * @param non-empty-string $queue
     */
    public function setVisibilityTimeout(string $queue, int $messageId, TimeSpan $visibilityTimeout): ?Message
    {
        return setVisibilityTimeout($this->pg, $queue, $messageId, $visibilityTimeout);
    }
}
