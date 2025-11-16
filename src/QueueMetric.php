<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Thesis\Time\TimeSpan;

/**
 * @api
 */
final readonly class QueueMetric
{
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
