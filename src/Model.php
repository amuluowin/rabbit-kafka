<?php


namespace rabbit\kafka;

use rabbit\exception\InvalidCallException;
use rabbit\kafka\Consumer\ConsumeManager;
use rabbit\kafka\Producter\Producter;
use rabbit\model\Model as BaseModel;

/**
 * Class Model
 * @package rabbit\kafka
 */
abstract class Model extends BaseModel
{
    /** @var Producter */
    protected $producter;
    /** @var string */
    protected $topic;
    /** @var string */
    protected $key = '';

    /**
     * @param $name
     * @param $arguments
     */
    public function __call($name, $arguments)
    {
        if (is_callable(ConsumeManager::$name)) {
            return ConsumeManager::$name(...$arguments);
        }
        throw new InvalidCallException("Can not call method $name");
    }

    /**
     * @throws Exception
     * @throws Exception\Protocol
     */
    public function save(bool $validated = true): void
    {
        $validated && $this->validate();
        $this->producter->send([
            [
                'topic' => $this->topic,
                'value' => json_encode($this->attributes),
                'key' => $this->key
            ]
        ]);
    }

    /**
     * @return bool
     */
    public function addMonit(): bool
    {
        if (!ConsumeManager::register($this->topic, [$this, 'get'])) {
            throw new InvalidCallException("Already add Monit!");
        }
        return true;
    }

    /**
     * @param callable $function
     * @return array
     */
    abstract protected function consume(int $part, array $message): void;
}