<?php

declare(strict_types=1);

namespace Thesis\Pgmq\Internal;

use Amp\Future;
use function Amp\async;

/**
 * @internal
 */
final readonly class AggregateWatcher implements PollWatcher
{
    /**
     * @param non-empty-list<PollWatcher> $watchers
     */
    public function __construct(
        private array $watchers,
    ) {}

    public function watch(): void
    {
        foreach ($this->watchers as $watcher) {
            $watcher->watch();
        }
    }

    public function cancel(): void
    {
        $futures = [];

        foreach ($this->watchers as $watcher) {
            $futures[] = async($watcher->cancel(...));
        }

        Future\awaitAll($futures);
    }
}
