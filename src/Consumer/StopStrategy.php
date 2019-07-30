<?php
declare(strict_types=1);

namespace rabbit\kafka;

use rabbit\kafka\Consumer\Client;

/**
 * Interface StopStrategy
 * @package rabbit\kafka
 */
interface StopStrategy
{
    /**
     * @param Client $consumer
     */
    public function setup(Client $consumer): void;
}
