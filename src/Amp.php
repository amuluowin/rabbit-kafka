<?php

namespace rabbit\kafka;

use Amp\Coroutine;
use Amp\Loop\Driver;
use Amp\Loop\Watcher;
use Amp\Promise;
use rabbit\App;
use React\Promise\PromiseInterface as ReactPromise;
use function Amp\Promise\rethrow;

/**
 * Class Amp
 * @package rabbit\kafka
 */
class Amp extends Driver
{
    private $events = [];
    private $timers;
    private $debug = false;

    function ioCallback($resource, $what, Watcher $watcher)
    {
        $this->log("ioCallback, what={$what}, id={$watcher->id}, type={$watcher->type}, value={$watcher->value}");
        try {
            $result = ($watcher->callback)($watcher->id, $watcher->value, $watcher->data);
            if ($result === null) {
                return;
            }
            if ($result instanceof \Generator) {
                $result = new Coroutine($result);
            }
            if ($result instanceof Promise || $result instanceof ReactPromise) {
                rethrow($result);
            }
        } catch (\Throwable $exception) {
            $this->error($exception);
        }
    }

    function timerCallback($resource, $what, Watcher $watcher)
    {
        try {
            $result = ($watcher->callback)($watcher->id, $watcher->data);
            if ($result === null) {
                return;
            }
            if ($result instanceof \Generator) {
                $result = new Coroutine($result);
            }
            if ($result instanceof Promise || $result instanceof ReactPromise) {
                rethrow($result);
            }
        } catch (\Throwable $exception) {
            $this->error($exception);
        }
    }

    function signalCallback($signum, $what, Watcher $watcher)
    {
        try {
            $result = ($watcher->callback)($watcher->id, $watcher->value, $watcher->data);
            if ($result === null) {
                return;
            }
            if ($result instanceof \Generator) {
                $result = new Coroutine($result);
            }
            if ($result instanceof Promise || $result instanceof ReactPromise) {
                rethrow($result);
            }
        } catch (\Throwable $exception) {
            $this->error($exception);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function cancel(string $watcherId)
    {
        parent::cancel($watcherId);
        if (isset($this->events[$watcherId])) {
            $watcher = $this->events[$watcherId];
            $this->log("cancel event, id={$watcherId->id}, type={$watcher->type}, value={$watcher->value}");
            $this->deactivate($watcher);
            unset($this->events[$watcherId]);
        }
    }

    public static function isSupported(): bool
    {
        return \extension_loaded("swoole");
    }

    /**
     * {@inheritdoc}
     */
    public function run()
    {
        parent::run();
        $this->log(__METHOD__);
    }

    /**
     * {@inheritdoc}
     */
    public function stop()
    {
        $this->log(__METHOD__);
        swoole_event_exit();
        parent::stop();
    }

    /**
     * {@inheritdoc}
     */
    public function getHandle()
    {
        $this->log(__METHOD__);
        return null;
    }

    /**
     * {@inheritdoc}
     */
    protected function dispatch(bool $blocking)
    {
        $this->log(__METHOD__);
        swoole_event_dispatch();
    }

    private function log($msg)
    {
        if (!$this->debug) {
            return;
        }
        App::debug('[' . date('H:i:s') . ']' . "\t" . $msg . "\n", 'Loop');
    }

    /**
     * {@inheritdoc}
     */
    protected function activate(array $watchers)
    {
        foreach ($watchers as $watcher) {
            $this->log("add event, id={$watcher->id}, type={$watcher->type}, value={$watcher->value}");
            if (!isset($this->events[$id = $watcher->id])) {
                $this->events[$id] = $watcher;
                switch ($watcher->type) {
                    case Watcher::READABLE:
                        swoole_event_add($watcher->value, function ($resource) use ($watcher) {
                            $this->ioCallback($resource, $watcher->type, $watcher);
                        }, null,
                            SWOOLE_EVENT_READ);
                        break;
                    case Watcher::WRITABLE:
                        swoole_event_add($watcher->value, null,
                            function ($resource) use ($watcher) {
                                $this->ioCallback($resource, $watcher->type, $watcher);
                            },
                            SWOOLE_EVENT_WRITE);
                        break;
                    case Watcher::DELAY:
                        $timerId = swoole_timer_after($watcher->value, function ($resource) use ($watcher) {
                            $this->timerCallback($resource, $watcher->type, $watcher);
                        });
                        $this->timers[$id] = $timerId;
                        break;
                    case Watcher::REPEAT:
                        $timerId = swoole_timer_tick($watcher->value, function ($resource) use ($watcher) {
                            $this->timerCallback($resource, $watcher->type, $watcher);
                        });
                        $this->timers[$id] = $timerId;
                        break;
                    case Watcher::SIGNAL:
                        \swoole_process::signal($watcher->value, function ($signo) use ($watcher) {
                            $this->signalCallback($signo, $watcher->type, $watcher);
                        });
                        break;
                    default:
                        // @codeCoverageIgnoreStart
                        throw new \Error("Unknown watcher type");
                    // @codeCoverageIgnoreEnd
                }
            }
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function deactivate(Watcher $watcher)
    {
        if (isset($this->events[$watcher->id])) {
            $this->log("del event, id={$watcher->id}, type={$watcher->type}, value={$watcher->value}");
            if ($watcher->type == Watcher::READABLE or $watcher->type == Watcher::WRITABLE) {
                swoole_event_del($watcher->value);
            } elseif ($watcher->type === Watcher::SIGNAL) {
                \swoole_process::signal($watcher->value, null);
            } elseif ($watcher->type === Watcher::REPEAT or $watcher->type === Watcher::DELAY) {
                swoole_timer_clear($this->timers[$watcher->id]);
            }
        }
    }
}