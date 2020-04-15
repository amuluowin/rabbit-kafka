<?php


namespace rabbit\kafka;

use rabbit\exception\InvalidCallException;
use rabbit\kafka\Consumer\ConsumeManager;
use rabbit\model\Model as BaseModel;

/**
 * Class Model
 * @package rabbit\kafka
 */
abstract class Model extends BaseModel
{
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
        if (is_callable([$this, $name])) {
            return $this->$name(...$arguments);
        }
        if (is_callable(ConsumeManager::class . "::" . $name)) {
            return ConsumeManager::$name(...$arguments);
        }
        throw new InvalidCallException("Can not call method $name");
    }

    /**
     * @return array
     */
    public static function rules(): array
    {
        return [];
    }

    /**
     * @return string
     */
    public function toJsonString(): string
    {
        return json_encode($this->attributes);
    }
}
