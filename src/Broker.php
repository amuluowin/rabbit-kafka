<?php
/**
 * Created by PhpStorm.
 * User: albert
 * Date: 18-3-31
 * Time: 下午5:44
 */

namespace rabbit\kafka;

use Psr\Log\LoggerAwareTrait;
use rabbit\kafka\Sasl\Gssapi;
use rabbit\kafka\Sasl\Plain;
use rabbit\kafka\Sasl\Scram;
use rabbit\socket\pool\SocketPool;
use rabbit\socket\socket\SocketClientInterface;
use function in_array;
use function sprintf;

class Broker
{
    use LoggerAwareTrait;
    /**
     * @var int
     */
    private $groupBrokerId;
    /**
     * @var mixed[][]
     */
    private $topics = [];
    /**
     * @var string[]
     */
    private $brokers = [];
    /** @var Config */
    private $config;
    /** @var SocketPool */
    private $pool;

    /**
     * Broker constructor.
     * @param Config $config
     */
    public function __construct(SocketPool $pool, Config $config)
    {
        $this->pool = $pool;
        $this->config = $config;
    }

    /**
     * @return Config
     */
    public function getConfig(): Config
    {
        return $this->config;
    }

    public function setConfig(Config $config): void
    {
        $this->config = $config;
    }

    public function getGroupBrokerId(): int
    {
        return $this->groupBrokerId;
    }

    public function setGroupBrokerId(int $brokerId): void
    {
        $this->groupBrokerId = $brokerId;
    }

    /**
     * @return SocketPool
     */
    public function getPool(): SocketPool
    {
        return $this->pool;
    }

    /**
     * @param array $topics
     * @param array $brokersResult
     * @return bool
     */
    public function setData(array $topics, array $brokersResult): bool
    {
        $brokers = [];

        foreach ($brokersResult as $value) {
            $brokers[] = $value['host'] . ':' . $value['port'];
        }

        $changed = false;

        if (serialize($this->brokers) !== serialize($brokers)) {
            $this->pool->getPoolConfig()->setUri($brokers);
            $changed = true;
        }

        $newTopics = [];
        foreach ($topics as $topic) {
            if ((int)$topic['errorCode'] !== Protocol::NO_ERROR) {
                $this->logger->error('Parse metadata for topic is error, error:' . Protocol::getError($topic['errorCode']));
                continue;
            }

            $item = [];

            foreach ($topic['partitions'] as $part) {
                $item[$part['partitionId']] = $part['leader'];
            }

            $newTopics[$topic['topicName']] = $item;
        }

        if (serialize($this->topics) !== serialize($newTopics)) {
            $this->topics = $newTopics;

            $changed = true;
        }

        return $changed;
    }

    /**
     * @return mixed[][]
     */
    public function getTopics(): array
    {
        return $this->topics;
    }

    /**
     * @return string[]
     */
    public function getBrokers(): array
    {
        return $this->pool->getPoolConfig()->getUri();
    }

    /**
     * @return SocketClientInterface
     * @throws Exception
     */
    public function getConnect(): SocketClientInterface
    {
        $connection = $this->pool->getConnection();
        if (($sasl = $this->judgeConnectionConfig()) !== null) {
            $sasl->authenticate($connection);
        }
        return $connection;
    }

    /**
     * @throws Exception
     */
    private function judgeConnectionConfig(): ?SaslMechanism
    {
        if ($this->config === null) {
            return null;
        }

        $plainConnections = [
            Config::SECURITY_PROTOCOL_PLAINTEXT,
            Config::SECURITY_PROTOCOL_SASL_PLAINTEXT,
        ];

        $saslConnections = [
            Config::SECURITY_PROTOCOL_SASL_SSL,
            Config::SECURITY_PROTOCOL_SASL_PLAINTEXT,
        ];

        $securityProtocol = $this->config->getSecurityProtocol();

        $this->config->setSslEnable(!in_array($securityProtocol, $plainConnections, true));

        if (in_array($securityProtocol, $saslConnections, true)) {
            return $this->getSaslMechanismProvider($this->config);
        }

        return null;
    }

    /**
     * @throws Exception
     */
    private function getSaslMechanismProvider(Config $config): SaslMechanism
    {
        $mechanism = $config->getSaslMechanism();
        $username = $config->getSaslUsername();
        $password = $config->getSaslPassword();

        switch ($mechanism) {
            case Config::SASL_MECHANISMS_PLAIN:
                return new Plain($username, $password);
            case Config::SASL_MECHANISMS_GSSAPI:
                return Gssapi::fromKeytab($config->getSaslKeytab(), $config->getSaslPrincipal());
            case Config::SASL_MECHANISMS_SCRAM_SHA_256:
                return new Scram($username, $password, Scram::SCRAM_SHA_256);
            case Config::SASL_MECHANISMS_SCRAM_SHA_512:
                return new Scram($username, $password, Scram::SCRAM_SHA_512);
        }

        throw new Exception(sprintf('"%s" is an invalid SASL mechanism', $mechanism));
    }
}