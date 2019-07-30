<?php
declare(strict_types=1);

namespace rabbit\kafka\Consumer\StopStrategy;

use Amp\Loop;
use rabbit\kafka\Consumer\Client;
use rabbit\kafka\StopStrategy;

/**
 * Class Delay
 * @package rabbit\kafka\Consumer\StopStrategy
 */
final class Delay implements StopStrategy
{
    /**
     * The amount of time, in milliseconds, to stop the consumer
     *
     * @var int
     */
    private $delay;

    public function __construct(int $delay)
    {
        $this->delay = $delay;
    }

    public function setup(Client $consumer): void
    {
        Loop::delay(
            $this->delay,
            function () use ($consumer): void {
                $consumer->stop();
            }
        );
    }
}
