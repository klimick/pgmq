<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

/**
 * @api
 */
final readonly class Message
{
    /**
     * @param non-empty-string $value
     * @param non-negative-int $readCount
     */
    public function __construct(
        public int $id,
        public int $readCount,
        public \DateTimeImmutable $enqueuedAt,
        public string $value,
        public \DateTimeImmutable $visibilityTimeout,
    ) {}
}
