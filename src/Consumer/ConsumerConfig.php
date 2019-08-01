<?php
declare(strict_types=1);

namespace rabbit\kafka\Consumer;

use Psr\Log\LoggerAwareTrait;
use rabbit\kafka\Config;
use function in_array;
use function trim;

/**
 * @method string|false ietGroupId()
 * @method array|false ietTopics()
 * @method int getSessionTimeout()
 * @method int getRebalanceTimeout()
 * @method string getOffsetReset()
 * @method int getMaxBytes()
 * @method int getMaxWaitTime()
 */
class ConsumerConfig extends Config
{
    use LoggerAwareTrait;

    public const CONSUME_AFTER_COMMIT_OFFSET = 1;
    public const CONSUME_BEFORE_COMMIT_OFFSET = 2;

    /**
     * @var mixed[]
     */
    protected $runtimeOptions = [
        'consume_mode' => self::CONSUME_AFTER_COMMIT_OFFSET,
    ];

    /**
     * @var mixed[]
     */
    protected static $defaults = [
        'groupId' => '',
        'sessionTimeout' => 30000,
        'rebalanceTimeout' => 30000,
        'topics' => [],
        'offsetReset' => 'latest', // earliest
        'maxBytes' => 65536, // 64kb
        'maxWaitTime' => 100,
    ];

    /**
     * @throws \rabbit\kafka\Exception\Config
     */
    public function getGroupId(): string
    {
        $groupId = trim($this->ietGroupId());

        if ($groupId === false || $groupId === '') {
            throw new \rabbit\kafka\Exception\Config('Get group id value is invalid, must set it not empty string');
        }

        return $groupId;
    }

    /**
     * @throws \rabbit\kafka\Exception\Config
     */
    public function setGroupId(string $groupId): void
    {
        $groupId = trim($groupId);

        if ($groupId === false || $groupId === '') {
            throw new \rabbit\kafka\Exception\Config('Set group id value is invalid, must set it not empty string');
        }

        $this->options['groupId'] = $groupId;
    }

    /**
     * @throws \rabbit\kafka\Exception\Config
     */
    public function setSessionTimeout(int $sessionTimeout): void
    {
        if ($sessionTimeout < 1 || $sessionTimeout > 3600000) {
            throw new \rabbit\kafka\Exception\Config('Set session timeout value is invalid, must set it 1 .. 3600000');
        }

        $this->options['sessionTimeout'] = $sessionTimeout;
    }

    /**
     * @throws \rabbit\kafka\Exception\Config
     */
    public function setRebalanceTimeout(int $rebalanceTimeout): void
    {
        if ($rebalanceTimeout < 1 || $rebalanceTimeout > 3600000) {
            throw new \rabbit\kafka\Exception\Config('Set rebalance timeout value is invalid, must set it 1 .. 3600000');
        }

        $this->options['rebalanceTimeout'] = $rebalanceTimeout;
    }

    /**
     * @throws \rabbit\kafka\Exception\Config
     */
    public function setOffsetReset(string $offsetReset): void
    {
        if (!in_array($offsetReset, ['latest', 'earliest'], true)) {
            throw new \rabbit\kafka\Exception\Config('Set offset reset value is invalid, must set it `latest` or `earliest`');
        }

        $this->options['offsetReset'] = $offsetReset;
    }

    /**
     * @return string[]
     *
     * @throws \rabbit\kafka\Exception\Config
     */
    public function getTopics(): array
    {
        $topics = $this->ietTopics();

        if (empty($topics)) {
            throw new \rabbit\kafka\Exception\Config('Get consumer topics value is invalid, must set it not empty');
        }

        return $topics;
    }

    /**
     * @param string[] $topics
     *
     * @throws \rabbit\kafka\Exception\Config
     */
    public function setTopics(string $topics): void
    {
        $topics = trim($topics);
        if (empty($topics)) {
            throw new \rabbit\kafka\Exception\Config('Set consumer topics value is invalid, must set it not empty');
        }
        $topics = explode(',', $topics);
        $this->options['topics'] = $topics;
    }

    public function setConsumeMode(int $mode): void
    {
        if (!in_array($mode, [self::CONSUME_AFTER_COMMIT_OFFSET, self::CONSUME_BEFORE_COMMIT_OFFSET], true)) {
            throw new \rabbit\kafka\Exception\Config(
                'Invalid consume mode given, it must be either "ConsumerConfig::CONSUME_AFTER_COMMIT_OFFSET" or '
                . '"ConsumerConfig::CONSUME_BEFORE_COMMIT_OFFSET"'
            );
        }

        $this->runtimeOptions['consume_mode'] = $mode;
    }

    public function getConsumeMode(): int
    {
        return $this->runtimeOptions['consume_mode'];
    }
}
