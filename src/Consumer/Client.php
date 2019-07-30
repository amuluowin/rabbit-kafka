<?php


namespace rabbit\kafka\Consumer;


use Amp\Loop;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;
use rabbit\kafka\Amp;

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
    public function __construct(Process $process, LoggerInterface $logger, ?StopStrategy $stopStrategy = null)
    {
        $this->process = $process;
        $this->logger = $logger;
        $this->stopStrategy = $stopStrategy;
        Loop::set(getDI(Amp::class));
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
        $this->process->start($function);
        Loop::run();
        $this->running = true;
    }

    private function setupStopStrategy(): void
    {
        if ($this->stopStrategy === null) {
            return;
        }

        $this->stopStrategy->setup($this);
    }

    public function stop(): void
    {
        if ($this->running === false) {
            $this->running->error('Consumer is not running');
            return;
        }
        $this->process->stop();
        $this->process = null;
        $this->running = false;
        Loop::stop();
    }
}