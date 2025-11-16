<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

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
        private Supervisor $supervisor,
    ) {}

    public function drop(): bool
    {
        return $this->supervisor->dropQueue($this->name);
    }

    public function purge(): int
    {
        return $this->supervisor->purgeQueue($this->name);
    }

    /**
     * @throws QueueNotFound
     */
    public function metrics(): QueueMetric
    {
        return $this->supervisor->queueMetrics($this->name);
    }

    /**
     * @throws QueueNotFound
     */
    public function metadata(): QueueMetadata
    {
        return $this->supervisor->queueMetadata($this->name);
    }

    /**
     * @param non-empty-string $json
     * @return int the message id, unique to the queue, is returned
     */
    public function send(string $json, null|TimeSpan|\DateTimeImmutable $delay = null): int
    {
        return $this->supervisor->send($this->name, $json, $delay);
    }

    /**
     * @param non-empty-list<non-empty-string> $messages
     * @return list<int>
     */
    public function sendBatch(array $messages, null|TimeSpan|\DateTimeImmutable $delay = null): array
    {
        return $this->supervisor->sendBatch($this->name, $messages, $delay);
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
        return $this->supervisor->readPoll($this->name, $batch, $visibilityTimeout, $maxPoll, $pollInterval);
    }

    public function read(?TimeSpan $visibilityTimeout = null): ?Message
    {
        return $this->supervisor->read($this->name, $visibilityTimeout);
    }

    /**
     * @param positive-int $count
     * @return iterable<Message>
     */
    public function readBatch(int $count, ?TimeSpan $visibilityTimeout = null): iterable
    {
        return $this->supervisor->readBatch($this->name, $count, $visibilityTimeout);
    }

    public function pop(): ?Message
    {
        return $this->supervisor->pop($this->name);
    }

    public function archive(int $messageId): bool
    {
        return $this->supervisor->archive($this->name, $messageId);
    }

    /**
     * @param list<int> $messageIds
     * @return list<int>
     */
    public function archiveBatch(array $messageIds): array
    {
        return $this->supervisor->archiveBatch($this->name, $messageIds);
    }

    public function detachArchive(): void
    {
        $this->supervisor->detachArchive($this->name);
    }

    public function delete(int $messageId): bool
    {
        return $this->supervisor->delete($this->name, $messageId);
    }

    /**
     * @param list<int> $messageIds
     * @return list<int>
     */
    public function deleteBatch(array $messageIds): array
    {
        return $this->supervisor->deleteBatch($this->name, $messageIds);
    }

    public function setVisibilityTimeout(int $messageId, TimeSpan $visibilityTimeout): ?Message
    {
        return $this->supervisor->setVisibilityTimeout($this->name, $messageId, $visibilityTimeout);
    }
}
