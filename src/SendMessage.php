<?php

declare(strict_types=1);

namespace Thesis\Pgmq;

/**
 * @api
 */
final readonly class SendMessage
{
    /**
     * @param non-empty-string $valueJson
     * @param ?non-empty-string $headerJson
     */
    public function __construct(
        public string $valueJson,
        public ?string $headerJson = null,
    ) {}
}
