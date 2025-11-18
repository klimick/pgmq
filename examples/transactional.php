<?php

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnectionPool;
use Thesis\Pgmq;

$pg = new PostgresConnectionPool(PostgresConfig::fromString('host=pgmq user=postgres password=postgres'));

Pgmq\dropQueue($pg, 'outbox');
$queue = Pgmq\createQueue($pg, 'outbox');

$tx = $pg->beginTransaction();

Pgmq\send($tx, $queue->name, new Pgmq\SendMessage('{"id": 1}'));
Pgmq\send($tx, $queue->name, new Pgmq\SendMessage('{"id": 2}'));
Pgmq\send($tx, $queue->name, new Pgmq\SendMessage('{"id": 3}'));

$tx->rollback();

$metrics = $queue->metrics();
assert($metrics->length === 0, 'Queue should be empty.');
