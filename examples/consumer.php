<?php

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use Amp\Postgres;
use Thesis\Pgmq;
use function Amp\trapSignal;

$pg = new Postgres\PostgresConnectionPool(
    Postgres\PostgresConfig::fromString('host=pgmq user=postgres password=postgres'),
);

Pgmq\createExtension($pg);
Pgmq\createQueue($pg, 'events');

$consumer = Pgmq\createConsumer($pg);

$context = $consumer->consume(
    static function (array $messages, Pgmq\ConsumeController $ctrl): void {
        dump(array_map(static fn(Pgmq\Message $message): string => $message->value, $messages));
        $ctrl->ack($messages);
    },
    new Pgmq\ConsumeConfig(
        queue: 'events',
    ),
);

trapSignal([\SIGINT, \SIGTERM]);

$context->stop();
$context->awaitCompletion();
