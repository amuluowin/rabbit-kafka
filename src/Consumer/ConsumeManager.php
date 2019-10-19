<?php


namespace rabbit\kafka\Consumer;

/**
 * Class Worker
 * @package rabbit\kafka\Consumer
 */
final class ConsumeManager
{
    /** @var array */
    private static $topicWorkers = [];

    /**
     * @return array
     */
    public static function getTopics(): array
    {
        return array_keys(static::$topicWorkers);
    }

    /**
     * @param string $topic
     * @param callable $function
     * @return bool
     */
    public static function register(string $topic, callable $function): bool
    {
        if (isset(static::$topicWorkers[$topic])) {
            return false;
        }
        static::$topicWorkers[$topic] = $function;
        return true;
    }

    /**
     * @param string $topic
     * @param callable $function
     */
    public static function reset(string $topic, callable $function): void
    {
        static::$topicWorkers[$topic] = $function;
    }

    /**
     * @param string $topic
     */
    public static function remove(string $topic): void
    {
        unset(static::$topicWorkers[$topic]);
    }

    /**
     * @param string $topic
     * @param int $partition
     * @param array $message
     */
    public static function consume(string $topic, int $partition, array $message): void
    {
        if (isset(static::$topicWorkers[$topic])) {
            call_user_func(static::$topicWorkers[$topic], $partition, $message);
        }
    }
}
