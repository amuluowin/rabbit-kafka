<?php
declare(strict_types=1);

namespace rabbit\kafka\Producter;

use Co\System;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\NullLogger;
use rabbit\contract\InitInterface;
use rabbit\kafka\Broker;
use rabbit\kafka\Exception;
use rabbit\kafka\Protocol as ProtocolTool;
use rabbit\kafka\Protocol\Protocol;

class Producter implements InitInterface
{
    use LoggerAwareTrait;
    /** @var Broker */
    private $broker;
    /** @var RecordValidator */
    private $recordValidator;
    /** @var array */
    private $msgBuffer = [];
    /** @var bool */
    private $isSyncData = false;

    /**
     * Producter constructor.
     * @param Broker $broker
     */
    public function __construct(Broker $broker)
    {
        $this->broker = $broker;
        $this->recordValidator = new RecordValidator();
    }

    public function init()
    {
        $this->logger = $this->logger ?? new NullLogger();
        ProtocolTool::init($this->broker->getConfig()->getBrokerVersion(), $this->logger);
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

        if (!$this->isSyncData) {
            $this->isSyncData = true;
            rgo(function () {
                while (true) {
                    $this->syncMeta();
                    System::sleep(10);
                }
            });
        }

        $recordSet = array_merge($recordSet, array_splice($this->msgBuffer, 0));
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
                    $callback && $callback(ProtocolTool::decode(
                        ProtocolTool::PRODUCE_REQUEST,
                        substr($recordSet, 4)
                    ));
                } else {
                    $connect->send($requestData);
                    $connect->release();
                }
            });
        }
    }

    public function syncMeta(): void
    {
        $socket = $this->broker->getPoolConnect();
        $this->logger->debug('Start sync metadata request');
        $params = [];
        $requestData = ProtocolTool::encode(ProtocolTool::METADATA_REQUEST, $params);
        $socket->send($requestData);
        $dataLen = Protocol::unpack(Protocol::BIT_B32, $socket->recv(4));
        $data = $socket->recv($dataLen);
        $correlationId = Protocol::unpack(Protocol::BIT_B32, substr($data, 0, 4));
        $result = ProtocolTool::decode(ProtocolTool::METADATA_REQUEST, substr($data, 4));
        if (!isset($result['brokers'], $result['topics'])) {
            throw new Exception('Get metadata is fail, brokers or topics is null.');
        }
        $this->broker->setData($result['topics'], $result['brokers']);
        $socket->release();
    }

    /**
     * @param string[][] $recordSet
     *
     * @return mixed[]
     */
    protected function convertRecordSet(array $recordSet): array
    {
        $sendData = [];
        while (empty($topics = $this->broker->getTopics())) {
            \Co::sleep(0.5);
        }

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
