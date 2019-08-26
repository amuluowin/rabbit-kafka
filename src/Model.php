<?php


namespace rabbit\kafka;

use rabbit\kafka\Consumer\Consumer;
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
    /** @var Consumer */
    protected $consumer;
    /** @var string */
    protected $topic;
    /** @var string */
    protected $key = '';

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
     * @param callable $function
     * @return array
     */
    public function get(callable $function): array
    {
        $this->consumer->start($function);
    }
}