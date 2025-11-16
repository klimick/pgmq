<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresConnectionPool;
use Amp\Postgres\PostgresQueryError;
use Thesis\Time\TimeSpan;

/**
 * @api
 * @phpstan-type RawMessage = array{
 *     msg_id: int,
 *     read_ct: non-negative-int,
 *     enqueued_at: non-empty-string,
 *     vt: non-empty-string,
 *     message: non-empty-string,
 * }
 * @phpstan-type RawMetric = array{
 *     queue_name: non-empty-string,
 *     queue_length: non-negative-int,
 *     newest_msg_age_sec: ?non-negative-int,
 *     oldest_msg_age_sec: ?non-negative-int,
 *     total_messages: non-negative-int,
 *     scrape_time: non-empty-string,
 *     queue_visible_length: non-negative-int,
 * }
 * @phpstan-type RawQueue = array{
 *     queue_name: non-empty-string,
 *     created_at: non-empty-string,
 *     is_partitioned: bool,
 *     is_unlogged: bool,
 * }
 */
final readonly class Supervisor
{
    private const int DEFAULT_VISIBILITY_TIMEOUT_SECS = 30;

    public function __construct(
        private PostgresConnection $pg,
    ) {
        $this->createExtension();
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
        $this->pg->execute('SELECT pgmq.validate_queue_name($1);', [
            $queue,
        ]);
    }

    /**
     * @param non-empty-string $queue
     */
    public function createQueue(string $queue): Queue
    {
        $this->pg->execute('SELECT pgmq.create($1)', [
            $queue,
        ]);

        return new Queue($queue, $this);
    }

    /**
     * @param non-empty-string $queue
     */
    public function createUnloggedQueue(string $queue): Queue
    {
        $this->pg->execute('SELECT pgmq.create_unlogged($1)', [
            $queue,
        ]);

        return new Queue($queue, $this);
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
        $this->pg->execute('SELECT pgmq.create($1, $2::text, $3::text)', [
            $queue,
            (string) $partitionInterval,
            (string) $retentionInterval,
        ]);

        return new Queue($queue, $this);
    }

    /**
     * @return iterable<QueueMetadata>
     */
    public function listQueues(): iterable
    {
        $result = $this->pg->query('SELECT queue_name, created_at, is_partitioned, is_unlogged FROM pgmq.list_queues()');

        /** @var RawQueue $row */
        foreach ($result as $row) {
            yield $this->createQueueMetadata($row);
        }
    }

    /**
     * @param non-empty-string $queue
     */
    public function dropQueue(string $queue): bool
    {
        /** @var array{drop_queue?: bool} $result */
        $result = $this->pg
            ->execute('SELECT pgmq.drop_queue($1)', [$queue])
            ->fetchRow() ?? [];

        return $result['drop_queue'] ?? false;
    }

    /**
     * @param non-empty-string $queue
     */
    public function purgeQueue(string $queue): int
    {
        /** @var array{purge_queue?: non-negative-int} $result */
        $result = $this->pg
            ->execute('SELECT pgmq.purge_queue($1)', [$queue])
            ->fetchRow() ?? [];

        return $result['purge_queue'] ?? 0;
    }

    /**
     * @param non-empty-string $queue
     * @throws QueueNotFound
     */
    public function queueMetrics(string $queue): QueueMetric
    {
        /** @var RawMetric $result */
        $result = $this->pg
            ->execute('SELECT * FROM pgmq.metrics($1)', [$queue])
            ->fetchRow() ?? throw new QueueNotFound();

        return $this->createQueueMetric($result);
    }

    /**
     * @param non-empty-string $queue
     * @throws QueueNotFound
     */
    public function queueMetadata(string $queue): QueueMetadata
    {
        /** @var RawQueue $result */
        $result = $this->pg
            ->execute('SELECT queue_name, created_at, is_partitioned, is_unlogged FROM pgmq.list_queues() WHERE queue_name = $1', [$queue])
            ->fetchRow() ?? throw new QueueNotFound();

        return $this->createQueueMetadata($result);
    }

    /**
     * @return iterable<QueueMetric>
     */
    public function metrics(): iterable
    {
        $result = $this->pg->query('SELECT * FROM pgmq.metrics_all();');

        /** @var RawMetric $row */
        foreach ($result as $row) {
            yield $this->createQueueMetric($row);
        }
    }

    /**
     * @param non-empty-string $queue
     * @param non-empty-string $json
     * @return int the message id, unique to the queue, is returned
     */
    public function send(string $queue, string $json, null|TimeSpan|\DateTimeImmutable $delay = null): int
    {
        $delay ??= TimeSpan::fromSeconds(0);

        $sql = match (true) {
            $delay instanceof TimeSpan => 'SELECT * FROM pgmq.send($1, $2, $3::int)',
            default => 'SELECT * FROM pgmq.send($1, $2, $3::timestamptz)',
        };

        /** @var array{send: int} $result */
        $result = $this->pg
            ->execute($sql, [
                $queue,
                $json,
                $delay instanceof TimeSpan ? (int) $delay->toSeconds(PHP_ROUND_HALF_UP) : $delay->format(\DateTimeInterface::RFC3339),
            ])
            ->fetchRow() ?? throw new \RuntimeException("Failed to send message to the queue {$queue}.");

        return $result['send'];
    }

    /**
     * @param non-empty-string $queue
     * @param non-empty-list<non-empty-string> $messages
     * @return list<int>
     */
    public function sendBatch(string $queue, array $messages, null|TimeSpan|\DateTimeImmutable $delay = null): array
    {
        $delay ??= TimeSpan::fromSeconds(0);

        $sql = match (true) {
            $delay instanceof TimeSpan => 'SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3::int)',
            default => 'SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3::timestamptz)',
        };

        $result = $this->pg->execute($sql, [
            $queue,
            $messages,
            $delay instanceof TimeSpan ? $delay->toSeconds(PHP_ROUND_HALF_UP) : $delay->format(\DateTimeInterface::RFC3339),
        ]);

        $messageIds = [];

        /** @var array{send_batch: int} $row */
        foreach ($result as $row) {
            $messageIds[] = $row['send_batch'];
        }

        return $messageIds;
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
        $result = $this->pg->execute('SELECT * FROM pgmq.read_with_poll($1, $2, $3, $4, $5);', [
            $queue,
            ($visibilityTimeout ?? TimeSpan::fromSeconds(self::DEFAULT_VISIBILITY_TIMEOUT_SECS))->toSeconds(PHP_ROUND_HALF_UP),
            $batch,
            ($maxPoll ?? TimeSpan::fromSeconds(5))->toSeconds(PHP_ROUND_HALF_UP),
            ($pollInterval ?? TimeSpan::fromMilliseconds(100))->toMilliseconds(PHP_ROUND_HALF_UP),
        ]);

        /** @var RawMessage $row */
        foreach ($result as $row) {
            yield $this->createMessage($row);
        }
    }

    /**
     * @param non-empty-string $queue
     */
    public function read(string $queue, ?TimeSpan $visibilityTimeout = null): ?Message
    {
        foreach ($this->readBatch($queue, 1, $visibilityTimeout) as $message) {
            return $message;
        }

        return null;
    }

    /**
     * @param non-empty-string $queue
     * @param positive-int $count
     * @return iterable<Message>
     */
    public function readBatch(string $queue, int $count, ?TimeSpan $visibilityTimeout = null): iterable
    {
        $visibilityTimeout ??= TimeSpan::fromSeconds(self::DEFAULT_VISIBILITY_TIMEOUT_SECS);

        $result = $this->pg->execute('SELECT * FROM pgmq.read($1, $2, $3)', [
            $queue,
            $visibilityTimeout->toSeconds(PHP_ROUND_HALF_UP),
            $count,
        ]);

        /** @var RawMessage $row */
        foreach ($result as $row) {
            yield $this->createMessage($row);
        }
    }

    /**
     * @param non-empty-string $queue
     */
    public function pop(string $queue): ?Message
    {
        /** @var ?RawMessage $row */
        $row = $this->pg
            ->execute('SELECT * FROM pgmq.pop($1)', [
                $queue,
            ])
            ->fetchRow();

        return $row !== null ? $this->createMessage($row) : null;
    }

    /**
     * @param non-empty-string $queue
     */
    public function archive(string $queue, int $messageId): bool
    {
        return \in_array($messageId, $this->archiveBatch($queue, [$messageId]), true);
    }

    /**
     * @param non-empty-string $queue
     * @param list<int> $messageIds
     * @return list<int>
     */
    public function archiveBatch(string $queue, array $messageIds): array
    {
        $result = $this->pg->execute('SELECT * FROM pgmq.archive($1, $2::bigint[])', [
            $queue,
            $messageIds,
        ]);

        $archive = [];

        /** @var array{archive: int} $row */
        foreach ($result as $row) {
            $archive[] = $row['archive'];
        }

        return $archive;
    }

    /**
     * @param non-empty-string $queue
     */
    public function detachArchive(string $queue): void
    {
        $this->pg->execute('SELECT pgmq.detach_archive(%1);', [$queue]);
    }

    /**
     * @param non-empty-string $queue
     */
    public function delete(string $queue, int $messageId): bool
    {
        return \in_array($messageId, $this->deleteBatch($queue, [$messageId]), true);
    }

    /**
     * @param non-empty-string $queue
     * @param list<int> $messageIds
     * @return list<int>
     */
    public function deleteBatch(string $queue, array $messageIds): array
    {
        $result = $this->pg->execute('SELECT pgmq.delete($1, $2::bigint[])', [
            $queue,
            $messageIds,
        ]);

        $deleted = [];

        /** @var array{delete: int} $row */
        foreach ($result as $row) {
            $deleted[] = $row['delete'];
        }

        return $deleted;
    }

    /**
     * @param non-empty-string $queue
     */
    public function setVisibilityTimeout(string $queue, int $messageId, TimeSpan $visibilityTimeout): ?Message
    {
        /** @var ?RawMessage $row */
        $row = $this->pg
            ->execute('SELECT * FROM pgmq.set_vt($1, $2::bigint, $3::int)', [
                $queue,
                $messageId,
                $visibilityTimeout->toSeconds(PHP_ROUND_HALF_UP),
            ])
            ->fetchRow();

        return $row !== null ? $this->createMessage($row) : null;
    }

    /**
     * @param RawMessage $row
     */
    private function createMessage(array $row): Message
    {
        return new Message(
            id: $row['msg_id'],
            readCount: $row['read_ct'],
            enqueuedAt: new \DateTimeImmutable($row['enqueued_at']),
            value: $row['message'],
            visibilityTimeout: new \DateTimeImmutable($row['vt']),
        );
    }

    /**
     * @param RawMetric $row
     */
    private function createQueueMetric(array $row): QueueMetric
    {
        return new QueueMetric(
            name: $row['queue_name'],
            length: $row['queue_length'],
            newestMsgAge: TimeSpan::fromSeconds($row['newest_msg_age_sec'] ?? 0),
            oldestMsgAge: TimeSpan::fromSeconds($row['oldest_msg_age_sec'] ?? 0),
            totalMessages: $row['total_messages'],
            scrapeTime: new \DateTimeImmutable($row['scrape_time']),
            queueVisibleLength: $row['queue_visible_length'],
        );
    }

    /**
     * @param RawQueue $row
     */
    private function createQueueMetadata(array $row): QueueMetadata
    {
        return new QueueMetadata(
            name: $row['queue_name'],
            createdAt: new \DateTimeImmutable($row['created_at']),
            partitioned: $row['is_partitioned'],
            unlogged: $row['is_unlogged'],
        );
    }

    private function createExtension(): void
    {
        $this->pg->execute('CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;');
    }
}
