<?php


namespace rabbit\kafka\Consumer;


use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;
use rabbit\core\Loop;

/**
 * Class Consumer
 * @package rabbit\kafka\Consumer
 */
class Client
{
    use LoggerAwareTrait;
    /** @var Process */
    private $process;
    /** @var bool */
    private $running = false;

    /**
     * Consumer constructor.
     * @param Process $process
     */
    public function __construct(
        Process $process,
        LoggerInterface $logger
    ) {
        $this->process = $process;
        $this->logger = $logger;
    }


    /**
     * @param callable $function
     */
    public function start(callable $function): void
    {
        if ($this->running) {
            $this->logger->error('Consumer is already being executed');
            return;
        }
        Loop::run('kafka');
        $this->process->start($function);
        $this->running = true;
    }

    public function stop(): void
    {
        if ($this->running === false) {
            $this->running->error('Consumer is not running');
            return;
        }
        $this->process->stop();
        $this->process = null;
        Loop::stop('kafka');
        $this->running = false;
    }
}