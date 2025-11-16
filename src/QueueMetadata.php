<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

/**
 * @api
 * @phpstan-type RawQueue = array{
 *      queue_name: non-empty-string,
 *      created_at: non-empty-string,
 *      is_partitioned: bool,
 *      is_unlogged: bool,
 *  }
 */
final readonly class QueueMetadata
{
    /**
     * @internal
     * @param array<array-key, mixed> $row
     */
    public static function fromArray(array $row): self
    {
        /** @var RawQueue $row */
        return new self(
            name: $row['queue_name'],
            createdAt: new \DateTimeImmutable($row['created_at']),
            partitioned: $row['is_partitioned'],
            unlogged: $row['is_unlogged'],
        );
    }

    /**
     * @param non-empty-string $name
     */
    public function __construct(
        public string $name,
        public \DateTimeImmutable $createdAt,
        public bool $partitioned,
        public bool $unlogged,
    ) {}
}
