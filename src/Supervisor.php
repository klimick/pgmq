<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnectionPool;
use Amp\Postgres\PostgresLink;
use Amp\Postgres\PostgresQueryError;

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
        validateQueueName(
            pg: $this->pg,
            queue: $queue,
        );
    }

    /**
     * @param non-empty-string $queue
     */
    public function createQueue(string $queue): Queue
    {
        return createQueue(
            pg: $this->pg,
            queue: $queue,
        );
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
        return createPartitionedQueue(
            pg: $this->pg,
            queue: $queue,
            partitionInterval: $partitionInterval,
            retentionInterval: $retentionInterval,
        );
    }

    /**
     * @return iterable<QueueMetadata>
     */
    public function listQueueMetadata(): iterable
    {
        return listQueueMetadata(pg: $this->pg);
    }

    /**
     * @return iterable<Queue>
     */
    public function listQueues(): iterable
    {
        return listQueues(pg: $this->pg);
    }

    /**
     * @return iterable<QueueMetric>
     */
    public function metrics(): iterable
    {
        return metrics(pg: $this->pg);
    }
}
