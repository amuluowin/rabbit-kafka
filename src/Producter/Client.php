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
use rabbit\kafka\Protocol as ProtocolTool;
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
        ProtocolTool::init($broker->getConfig()->getBrokerVersion(), $logger);
    }

    public function init()
    {
        $this->syncMeta();
    }

    /**
     * @param array $recordSet
     * @param callable|null $callback
     * @throws Exception
     * @throws Exception\Protocol
     */
    public function send(array $recordSet, ?callable $callback = null): void
    {
        /** @var ProducterConfig $config */
        $config = $this->broker->getConfig();
        $requiredAck = $config->getRequiredAck();
        $timeout = $config->getTimeout();
        $compression = $config->getCompression();
        if (empty($recordSet)) {
            return;
        }

        $sendData = $this->convertRecordSet($recordSet);
        foreach ($sendData as $brokerId => $topicList) {
            $connect = $this->broker->getPoolConnect();
            $params = [
                'required_ack' => $requiredAck,
                'timeout' => $timeout,
                'data' => $topicList,
                'compression' => $compression,
            ];

            $this->logger->debug('Send message start, params:' . json_encode($params));
            $requestData = ProtocolTool::encode(ProtocolTool::PRODUCE_REQUEST, $params);
            rgo(function () use ($connect, $requestData, $requiredAck, $callback) {
                if ($requiredAck !== 0) {
                    $connect->send($requestData);
                    $dataLen = Protocol::unpack(Protocol::BIT_B32, $connect->recv(4));
                    $recordSet = $connect->recv($dataLen);
                    $connect->release();
                    $correlationId = Protocol::unpack(Protocol::BIT_B32, substr($recordSet, 0, 4));
                    $callback && $callback(ProtocolTool::decode(ProtocolTool::PRODUCE_REQUEST,
                        substr($recordSet, 4)));
                } else {
                    $connect->send($requestData);
                    $connect->release();
                }
            });
        }
    }

    public function syncMeta(): void
    {
        $this->logger->debug('Start sync metadata request');
        $socket = $this->broker->getPoolConnect();
        $params = [];
        $requestData = ProtocolTool::encode(ProtocolTool::METADATA_REQUEST, $params);
        $socket->send($requestData);
        $dataLen = Protocol::unpack(Protocol::BIT_B32, $socket->recv(4));
        $data = $socket->recv($dataLen);
        $socket->release();
        $correlationId = Protocol::unpack(Protocol::BIT_B32, substr($data, 0, 4));
        $result = ProtocolTool::decode(ProtocolTool::METADATA_REQUEST, substr($data, 4));

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
