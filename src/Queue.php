<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Postgres\PostgresLink;
use Thesis\Time\TimeSpan;

/**
 * @api
 */
final readonly class Queue
{
    /**
     * @param non-empty-string $name
     */
    public function __construct(
        public string $name,
        private PostgresLink $pg,
    ) {}

    public function drop(): bool
    {
        return dropQueue($this->pg, $this->name);
    }

    public function purge(): int
    {
        return purgeQueue($this->pg, $this->name);
    }

    /**
     * @throws QueueNotFound
     */
    public function metrics(): QueueMetric
    {
        return queueMetrics($this->pg, $this->name);
    }

    /**
     * @throws QueueNotFound
     */
    public function metadata(): QueueMetadata
    {
        return queueMetadata($this->pg, $this->name);
    }

    /**
     * @return int the message id, unique to the queue, is returned
     */
    public function send(SendMessage $message, null|TimeSpan|\DateTimeImmutable $delay = null): int
    {
        return send($this->pg, $this->name, $message, $delay);
    }

    /**
     * @param non-empty-list<SendMessage> $messages
     * @return list<int>
     */
    public function sendBatch(array $messages, null|TimeSpan|\DateTimeImmutable $delay = null): array
    {
        return sendBatch($this->pg, $this->name, $messages, $delay);
    }

    /**
     * @param positive-int $batch
     * @return iterable<Message>
     */
    public function readPoll(
        int $batch = 1,
        ?TimeSpan $visibilityTimeout = null,
        ?TimeSpan $maxPoll = null,
        ?TimeSpan $pollInterval = null,
    ): iterable {
        return readPoll($this->pg, $this->name, $batch, $visibilityTimeout, $maxPoll, $pollInterval);
    }

    public function read(?TimeSpan $visibilityTimeout = null): ?Message
    {
        return read($this->pg, $this->name, $visibilityTimeout);
    }

    /**
     * @param positive-int $count
     * @return iterable<Message>
     */
    public function readBatch(int $count, ?TimeSpan $visibilityTimeout = null): iterable
    {
        return readBatch($this->pg, $this->name, $count, $visibilityTimeout);
    }

    public function pop(): ?Message
    {
        return pop($this->pg, $this->name);
    }

    public function archive(int $messageId): bool
    {
        return archive($this->pg, $this->name, $messageId);
    }

    /**
     * @param list<int> $messageIds
     * @return list<int>
     */
    public function archiveBatch(array $messageIds): array
    {
        return archiveBatch($this->pg, $this->name, $messageIds);
    }

    public function detachArchive(): void
    {
        detachArchive($this->pg, $this->name);
    }

    public function delete(int $messageId): bool
    {
        return delete($this->pg, $this->name, $messageId);
    }

    /**
     * @param list<int> $messageIds
     * @return list<int>
     */
    public function deleteBatch(array $messageIds): array
    {
        return deleteBatch($this->pg, $this->name, $messageIds);
    }

    public function setVisibilityTimeout(int $messageId, TimeSpan $visibilityTimeout): ?Message
    {
        return setVisibilityTimeout($this->pg, $this->name, $messageId, $visibilityTimeout);
    }
}
