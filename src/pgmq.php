<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Postgres\PostgresLink;
use Amp\Postgres\PostgresQueryError;
use Thesis\Time\TimeSpan;

/**
 * @api
 */
function createExtension(PostgresLink $pg): void
{
    $pg->execute('CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;');
}

/**
 * @api
 * @param non-empty-string $queue
 * @throws PostgresQueryError if queue name is invalid
 */
function validateQueueName(
    PostgresLink $pg,
    string $queue,
): void {
    $pg->execute('SELECT pgmq.validate_queue_name($1);', [
        $queue,
    ]);
}

/**
 * @api
 * @param non-empty-string $queue
 */
function createQueue(
    PostgresLink $pg,
    string $queue,
): Queue {
    $pg->execute('SELECT pgmq.create($1)', [
        $queue,
    ]);

    return new Queue($queue, $pg);
}

/**
 * @api
 * @param non-empty-string $queue
 */
function createUnloggedQueue(
    PostgresLink $pg,
    string $queue,
): Queue {
    $pg->execute('SELECT pgmq.create_unlogged($1)', [
        $queue,
    ]);

    return new Queue($queue, $pg);
}

/**
 * @api
 * @param non-empty-string $queue
 * @param non-negative-int|non-empty-string $partitionInterval this can be either any valid Postgres Duration supported by pg_partman, or an integer value
 * @param non-negative-int|non-empty-string $retentionInterval this can be either any valid Postgres Duration supported by pg_partman, or an integer value
 */
function createPartitionedQueue(
    PostgresLink $pg,
    string $queue,
    int|string $partitionInterval,
    int|string $retentionInterval,
): Queue {
    $pg->execute('SELECT pgmq.create($1, $2::text, $3::text)', [
        $queue,
        (string) $partitionInterval,
        (string) $retentionInterval,
    ]);

    return new Queue($queue, $pg);
}

/**
 * @api
 * @return iterable<QueueMetadata>
 */
function listQueues(PostgresLink $pg): iterable
{
    $result = $pg->query('SELECT queue_name, created_at, is_partitioned, is_unlogged FROM pgmq.list_queues()');

    foreach ($result as $row) {
        yield QueueMetadata::fromArray($row);
    }
}

/**
 * @api
 * @param non-empty-string $queue
 */
function dropQueue(
    PostgresLink $pg,
    string $queue,
): bool {
    /** @var array{drop_queue?: bool} $result */
    $result = $pg
        ->execute('SELECT pgmq.drop_queue($1)', [$queue])
        ->fetchRow() ?? [];

    return $result['drop_queue'] ?? false;
}

/**
 * @api
 * @param non-empty-string $queue
 */
function purgeQueue(
    PostgresLink $pg,
    string $queue,
): int {
    /** @var array{purge_queue?: non-negative-int} $result */
    $result = $pg
        ->execute('SELECT pgmq.purge_queue($1)', [$queue])
        ->fetchRow() ?? [];

    return $result['purge_queue'] ?? 0;
}

/**
 * @api
 * @param non-empty-string $queue
 * @throws QueueNotFound
 */
function queueMetrics(
    PostgresLink $pg,
    string $queue,
): QueueMetric {
    $result = $pg
        ->execute('SELECT * FROM pgmq.metrics($1)', [$queue])
        ->fetchRow() ?? throw new QueueNotFound();

    return QueueMetric::fromArray($result);
}

/**
 * @api
 * @param non-empty-string $queue
 * @throws QueueNotFound
 */
function queueMetadata(
    PostgresLink $pg,
    string $queue,
): QueueMetadata {
    $result = $pg
        ->execute('SELECT queue_name, created_at, is_partitioned, is_unlogged FROM pgmq.list_queues() WHERE queue_name = $1', [$queue])
        ->fetchRow() ?? throw new QueueNotFound();

    return QueueMetadata::fromArray($result);
}

/**
 * @api
 * @return iterable<QueueMetric>
 */
function metrics(PostgresLink $pg): iterable
{
    $result = $pg->query('SELECT * FROM pgmq.metrics_all();');

    foreach ($result as $row) {
        yield QueueMetric::fromArray($row);
    }
}

/**
 * @api
 * @param non-empty-string $queue
 * @param non-empty-string $json
 * @return int the message id, unique to the queue, is returned
 */
function send(
    PostgresLink $pg,
    string $queue,
    string $json,
    null|TimeSpan|\DateTimeImmutable $delay = null,
): int {
    $delay ??= TimeSpan::fromSeconds(0);

    $sql = match (true) {
        $delay instanceof TimeSpan => 'SELECT * FROM pgmq.send($1, $2, $3::int)',
        default => 'SELECT * FROM pgmq.send($1, $2, $3::timestamptz)',
    };

    /** @var array{send: int} $result */
    $result = $pg
        ->execute($sql, [
            $queue,
            $json,
            $delay instanceof TimeSpan ? (int) $delay->toSeconds(PHP_ROUND_HALF_UP) : $delay->format(\DateTimeInterface::RFC3339),
        ])
        ->fetchRow() ?? throw new \RuntimeException("Failed to send message to the queue {$queue}.");

    return $result['send'];
}

/**
 * @api
 * @param non-empty-string $queue
 * @param non-empty-list<non-empty-string> $messages
 * @return list<int>
 */
function sendBatch(
    PostgresLink $pg,
    string $queue,
    array $messages,
    null|TimeSpan|\DateTimeImmutable $delay = null,
): array {
    $delay ??= TimeSpan::fromSeconds(0);

    $sql = match (true) {
        $delay instanceof TimeSpan => 'SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3::int)',
        default => 'SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3::timestamptz)',
    };

    $result = $pg->execute($sql, [
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
 * @api
 * @param non-empty-string $queue
 * @param positive-int $batch
 * @return iterable<Message>
 */
function readPoll(
    PostgresLink $pg,
    string $queue,
    int $batch = 1,
    ?TimeSpan $visibilityTimeout = null,
    ?TimeSpan $maxPoll = null,
    ?TimeSpan $pollInterval = null,
): iterable {
    $result = $pg->execute('SELECT * FROM pgmq.read_with_poll($1, $2, $3, $4, $5);', [
        $queue,
        ($visibilityTimeout ?? TimeSpan::fromSeconds(30))->toSeconds(PHP_ROUND_HALF_UP),
        $batch,
        ($maxPoll ?? TimeSpan::fromSeconds(5))->toSeconds(PHP_ROUND_HALF_UP),
        ($pollInterval ?? TimeSpan::fromMilliseconds(100))->toMilliseconds(PHP_ROUND_HALF_UP),
    ]);

    foreach ($result as $row) {
        yield Message::fromArray($row);
    }
}

/**
 * @api
 * @param non-empty-string $queue
 */
function read(
    PostgresLink $pg,
    string $queue,
    ?TimeSpan $visibilityTimeout = null,
): ?Message {
    foreach (readBatch($pg, $queue, 1, $visibilityTimeout) as $message) {
        return $message;
    }

    return null;
}

/**
 * @api
 * @param non-empty-string $queue
 * @param positive-int $count
 * @return iterable<Message>
 */
function readBatch(
    PostgresLink $pg,
    string $queue,
    int $count,
    ?TimeSpan $visibilityTimeout = null,
): iterable {
    $visibilityTimeout ??= TimeSpan::fromSeconds(30);

    $result = $pg->execute('SELECT * FROM pgmq.read($1, $2, $3)', [
        $queue,
        $visibilityTimeout->toSeconds(PHP_ROUND_HALF_UP),
        $count,
    ]);

    foreach ($result as $row) {
        yield Message::fromArray($row);
    }
}

/**
 * @api
 * @param non-empty-string $queue
 */
function pop(
    PostgresLink $pg,
    string $queue,
): ?Message {
    $row = $pg
        ->execute('SELECT * FROM pgmq.pop($1)', [
            $queue,
        ])
        ->fetchRow();

    return $row !== null ? Message::fromArray($row) : null;
}

/**
 * @api
 * @param non-empty-string $queue
 */
function archive(
    PostgresLink $pg,
    string $queue,
    int $messageId,
): bool {
    return \in_array($messageId, archiveBatch($pg, $queue, [$messageId]), true);
}

/**
 * @api
 * @param non-empty-string $queue
 * @param list<int> $messageIds
 * @return list<int>
 */
function archiveBatch(
    PostgresLink $pg,
    string $queue,
    array $messageIds,
): array {
    $result = $pg->execute('SELECT * FROM pgmq.archive($1, $2::bigint[])', [
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
 * @api
 * @param non-empty-string $queue
 */
function detachArchive(
    PostgresLink $pg,
    string $queue,
): void {
    $pg->execute('SELECT pgmq.detach_archive(%1);', [$queue]);
}

/**
 * @api
 * @param non-empty-string $queue
 */
function delete(
    PostgresLink $pg,
    string $queue,
    int $messageId,
): bool {
    return \in_array($messageId, deleteBatch($pg, $queue, [$messageId]), true);
}

/**
 * @api
 * @param non-empty-string $queue
 * @param list<int> $messageIds
 * @return list<int>
 */
function deleteBatch(
    PostgresLink $pg,
    string $queue,
    array $messageIds,
): array {
    $result = $pg->execute('SELECT pgmq.delete($1, $2::bigint[])', [
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
 * @api
 * @phpstan-import-type RawMessage from Supervisor
 * @param non-empty-string $queue
 */
function setVisibilityTimeout(
    PostgresLink $pg,
    string $queue,
    int $messageId,
    TimeSpan $visibilityTimeout,
): ?Message {
    $row = $pg
        ->execute('SELECT * FROM pgmq.set_vt($1, $2::bigint, $3::int)', [
            $queue,
            $messageId,
            $visibilityTimeout->toSeconds(PHP_ROUND_HALF_UP),
        ])
        ->fetchRow();

    return $row !== null ? Message::fromArray($row) : null;
}
