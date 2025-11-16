<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Thesis\Time\TimeSpan;

/**
 * @api
 * @phpstan-type RawMetric = array{
 *      queue_name: non-empty-string,
 *      queue_length: non-negative-int,
 *      newest_msg_age_sec: ?non-negative-int,
 *      oldest_msg_age_sec: ?non-negative-int,
 *      total_messages: non-negative-int,
 *      scrape_time: non-empty-string,
 *      queue_visible_length: non-negative-int,
 *  }
 */
final readonly class QueueMetric
{
    /**
     * @internal
     * @param array<array-key, mixed> $row
     */
    public static function fromArray(array $row): self
    {
        /** @var RawMetric $row */
        return new self(
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
     * @param non-empty-string $name
     * @param non-negative-int $length
     * @param non-negative-int $totalMessages
     * @param non-negative-int $queueVisibleLength
     */
    public function __construct(
        public string $name,
        public int $length,
        public TimeSpan $newestMsgAge,
        public TimeSpan $oldestMsgAge,
        public int $totalMessages,
        public \DateTimeImmutable $scrapeTime,
        public int $queueVisibleLength,
    ) {}
}
