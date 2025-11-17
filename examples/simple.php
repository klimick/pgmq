<?php

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use Thesis\Pgmq;

$supervisor = Pgmq\Supervisor::fromDsn('host=pgmq user=postgres password=postgres');

$queue = $supervisor->createQueue('events');

$queue->send(new Pgmq\SendMessage('{"id": 1}'));

dump($queue->pop());

$queue->drop();
