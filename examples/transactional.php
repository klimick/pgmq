<?php

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnectionPool;
use Thesis\Pgmq;

$postgres = new PostgresConnectionPool(PostgresConfig::fromString('host=pgmq user=postgres password=postgres'));

$transaction = $postgres->beginTransaction();

$queue = Pgmq\createQueue($transaction, 'outbox');
$queue->send('{"id": 1}');
$transaction->rollback();

Pgmq\queueMetrics($postgres, 'outbox'); // exception will trigger
