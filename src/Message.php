<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

/**
 * @api
 * @phpstan-type RawMessage = array{
 *      msg_id: int,
 *      read_ct: non-negative-int,
 *      enqueued_at: non-empty-string,
 *      vt: non-empty-string,
 *      message: non-empty-string,
 *  }
 */
final readonly class Message
{
    /**
     * @internal
     * @param array<array-key, mixed> $row
     */
    public static function fromArray(array $row): self
    {
        /** @var RawMessage $row */
        return new self(
            id: $row['msg_id'],
            readCount: $row['read_ct'],
            enqueuedAt: new \DateTimeImmutable($row['enqueued_at']),
            value: $row['message'],
            visibilityTimeout: new \DateTimeImmutable($row['vt']),
        );
    }

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
