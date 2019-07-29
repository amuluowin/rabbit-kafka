<?php
declare(strict_types=1);

namespace rabbit\kafka\Producter;

use DI\DependencyException;
use DI\NotFoundException;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;
use rabbit\contract\InitInterface;
use rabbit\kafka\Broker;
use rabbit\kafka\Exception;
use rabbit\kafka\Protocol\Protocol;

class Client implements InitInterface
{
    use LoggerAwareTrait;
    /** @var Broker */
    private $broker;
    /** @var RecordValidator */
    private $recordValidator;

    /**
     * Client constructor.
     * @param Broker $broker
     * @param RecordValidator $recordValidator
     * @param LoggerInterface|null $logger
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function __construct(Broker $broker, RecordValidator $recordValidator, ?LoggerInterface $logger = null)
    {
        $this->broker = $broker;
        $this->logger = $logger;
        $this->recordValidator = $recordValidator;
        \rabbit\kafka\Protocol::init($broker->getConfig()->getBrokerVersion(), $logger);
    }

    public function init()
    {
        $this->syncMeta();
    }

    /**
     * @param array $recordSet
     * @return array
     * @throws Exception
     * @throws \rabbit\kafka\Exception\Protocol
     */
    public function send(array $recordSet): array
    {
        /** @var ProducerConfig $config */
        $config = $this->broker->getConfig();
        $requiredAck = $config->getRequiredAck();
        $timeout = $config->getTimeout();
        $compression = $config->getCompression();
        if (empty($recordSet)) {
            return [];
        }

        $sendData = $this->convertRecordSet($recordSet);
        $result = [];
        foreach ($sendData as $brokerId => $topicList) {
            $connect = $this->broker->getConnect();
            $params = [
                'required_ack' => $requiredAck,
                'timeout' => $timeout,
                'data' => $topicList,
                'compression' => $compression,
            ];

            $this->logger->debug('Send message start, params:' . json_encode($params));
            $requestData = \rabbit\kafka\Protocol::encode(\rabbit\kafka\Protocol::PRODUCE_REQUEST, $params);
            if ($requiredAck !== 0) {
                $group = waitGroup();
                $group->add(null, function () use ($connect, $requestData) {
                    $connect->send($requestData);
                    $dataLen = Protocol::unpack(Protocol::BIT_B32, $connect->recv(4));
                    $recordSet = $connect->recv($dataLen);
                    $correlationId = Protocol::unpack(Protocol::BIT_B32, substr($recordSet, 0, 4));
                    return \rabbit\kafka\Protocol::decode(\rabbit\kafka\Protocol::PRODUCE_REQUEST,
                        substr($recordSet, 4));
                });
                $result = $group->wait($timeout / 1000);
            } else {
                rgo(function () use ($connect, $requestData) {
                    $connect->send($requestData);
                });
            }
        }

        return $result;
    }

    public function syncMeta(): void
    {
        $this->logger->debug('Start sync metadata request');
        $socket = $this->broker->getConnect();
        $params = [];
        $requestData = \rabbit\kafka\Protocol::encode(\rabbit\kafka\Protocol::METADATA_REQUEST, $params);
        $socket->send($requestData);
        $dataLen = Protocol::unpack(Protocol::BIT_B32, $socket->recv(4));
        $data = $socket->recv($dataLen);
        $correlationId = Protocol::unpack(Protocol::BIT_B32, substr($data, 0, 4));
        $result = \rabbit\kafka\Protocol::decode(\rabbit\kafka\Protocol::METADATA_REQUEST, substr($data, 4));

        if (!isset($result['brokers'], $result['topics'])) {
            throw new Exception('Get metadata is fail, brokers or topics is null.');
        }
        $this->broker->setData($result['topics'], $result['brokers']);
    }

    /**
     * @param string[][] $recordSet
     *
     * @return mixed[]
     */
    protected function convertRecordSet(array $recordSet): array
    {
        $sendData = [];
        $topics = $this->broker->getTopics();

        foreach ($recordSet as $record) {
            $this->recordValidator->validate($record, $topics);

            $topicMeta = $topics[$record['topic']];
            $partNums = array_keys($topicMeta);
            shuffle($partNums);

            $partId = isset($record['partId'], $topicMeta[$record['partId']]) ? $record['partId'] : $partNums[0];

            $brokerId = $topicMeta[$partId];
            $topicData = [];
            if (isset($sendData[$brokerId][$record['topic']])) {
                $topicData = $sendData[$brokerId][$record['topic']];
            }

            $partition = [];
            if (isset($topicData['partitions'][$partId])) {
                $partition = $topicData['partitions'][$partId];
            }

            $partition['partition_id'] = $partId;

            if (trim($record['key'] ?? '') !== '') {
                $partition['messages'][] = ['value' => $record['value'], 'key' => $record['key']];
            } else {
                $partition['messages'][] = $record['value'];
            }

            $topicData['partitions'][$partId] = $partition;
            $topicData['topic_name'] = $record['topic'];
            $sendData[$brokerId][$record['topic']] = $topicData;
        }

        return $sendData;
    }
}
