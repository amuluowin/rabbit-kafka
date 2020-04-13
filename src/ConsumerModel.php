<?php
declare(strict_types=1);

namespace rabbit\kafka;

use rabbit\exception\InvalidCallException;
use rabbit\kafka\Consumer\ConsumeManager;
use rabbit\kafka\Consumer\Consumer;

/**
 * Class ConsumerModel
 * @package rabbit\kafka
 */
class ConsumerModel extends Model
{
    /** @var Consumer */
    protected $consumer;

    /**
     * @param callable $function
     * @return array
     */
    public function consume(callable $callback): void
    {
        if (!ConsumeManager::register($this->topic, $callback)) {
            throw new InvalidCallException("Already add Monit!");
        }
        $this->consumer->start();
        return;
    }
}