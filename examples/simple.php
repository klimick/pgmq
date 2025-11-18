<?php

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use Amp\Postgres;
use Thesis\Pgmq;

$pg = new Postgres\PostgresConnectionPool(Postgres\PostgresConfig::fromString('host=pgmq user=postgres password=postgres'));

$queue = Pgmq\createQueue($pg, 'events');

$queue->send(new Pgmq\SendMessage('{"id": 1}'));

dump($queue->pop());

$queue->drop();
