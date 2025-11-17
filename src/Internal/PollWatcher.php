<?php

declare(strict_types=1);

namespace Thesis\Pgmq\Internal;

/**
 * @internal
 */
interface PollWatcher
{
    public function watch(): void;

    public function cancel(): void;
}
