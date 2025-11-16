<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

/**
 * @api
 */
final readonly class QueueMetadata
{
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
