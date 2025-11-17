<?php

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use Amp\Postgres;
use Thesis\Pgmq;
use function Amp\delay;

$pg = new Postgres\PostgresConnectionPool(
    Postgres\PostgresConfig::fromString('host=pgmq user=postgres password=postgres'),
);

Pgmq\createExtension($pg);
Pgmq\createQueue($pg, 'events');

for ($i = 0; $i < 1_000; ++$i) {
    $msgId = Pgmq\send($pg, 'events', new Pgmq\SendMessage(
        sprintf('{"id": %d}', $i),
    ));
    dump($msgId);
    delay(0.5);
}
