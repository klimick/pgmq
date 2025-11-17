<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

use Amp\Cancellation;
use Amp\Future;

/**
 * @api
 */
final readonly class ConsumeContext
{
    /**
     * @param \Closure(): void $stop
     * @param Future<*> $completionMarker
     */
    public function __construct(
        private \Closure $stop,
        private Future $completionMarker,
    ) {}

    public function stop(): void
    {
        ($this->stop)();
    }

    public function awaitCompletion(?Cancellation $cancellation = null): void
    {
        $this->completionMarker->await($cancellation);
    }
}
