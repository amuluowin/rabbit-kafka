<?php
declare(strict_types=1);

namespace rabbit\kafka\Producter;

use Co\Channel;
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
    /** @var bool */
    private $isSyncData = false;
    /** @var Channel */
    private $channel;

    /**
     * Producter constructor.
     * @param Broker $broker
     */
    public function __construct(Broker $broker)
    {
        $this->broker = $broker;
        $this->recordValidator = new RecordValidator();
        $this->channel = new Channel();
        ProtocolTool::init($this->broker->getConfig()->getBrokerVersion());
    }

    public function init()
    {
        $this->logger = $this->logger ?? new NullLogger();
        $this->loop();
    }

    /**
     * @param array $recordSet
     * @param callable|null $callback
     * @throws Exception
     * @throws Exception\Protocol
     */
    public function send(array $recordSet, ?callable $callback = null): void
    {
        $this->channel->push([$recordSet, $callback]);
    }

    /**
     * @throws \Exception
     */
    private function loop(): void
    {
        rgo(function () {
            while (true) {
                [$recordSet, $callback] = $this->channel->pop();
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
                    $this->syncMeta();
                }
                $retry = $config->getRetry();
                $sendData = $this->convertRecordSet($recordSet);
                foreach ($sendData as $brokerId => $topicList) {
                    $params = [
                        'required_ack' => $requiredAck,
                        'timeout' => $timeout,
                        'data' => $topicList,
                        'compression' => $compression,
                    ];
                    $requestData = ProtocolTool::encode(ProtocolTool::PRODUCE_REQUEST, $params);
                    rgo(function () use ($retry, $requestData, $requiredAck, $callback) {
                        while ($retry--) {
                            try {
                                $connect = $this->broker->getPoolConnect();
                                $connect->send($requestData);
                                if ($requiredAck !== 0) {
                                    $dataLen = Protocol::unpack(Protocol::BIT_B32, $connect->recv(4));
                                    $recordSet = $connect->recv($dataLen);
                                    $correlationId = Protocol::unpack(Protocol::BIT_B32, substr($recordSet, 0, 4));
                                    $msg = ProtocolTool::decode(ProtocolTool::PRODUCE_REQUEST, substr($recordSet, 4));
                                    $connect->release(true);
                                    try {
                                        $callback && $callback($msg);
                                    } catch (\Throwable $exception) {
                                        $this->logger->error($exception->getMessage());
                                    }
                                } else {
                                    $connect->release(true);
                                }
                                break;
                            } catch (\Throwable $exception) {
                                $connect->close();
                                unset($connect);
                                $this->logger->error($exception->getMessage());
                            }
                        }
                    });
                }
            }
        });
    }

    public function syncMeta(): void
    {
        rgo(function () {
            loop:
            $socket = $this->broker->getPoolConnect();
            $pool = $this->broker->getPool();
            $pool->setCurrentCount($pool->getCurrentCount() - 1);
            while ($this->isSyncData) {
                try {
                    $params = [];
                    $requestData = ProtocolTool::encode(ProtocolTool::METADATA_REQUEST, $params);
                    $socket->send($requestData);
                    $dataLen = Protocol::unpack(Protocol::BIT_B32, $socket->recv(4));
                    $data = $socket->recv($dataLen);
                    $correlationId = Protocol::unpack(Protocol::BIT_B32, substr($data, 0, 4));
                    $result = ProtocolTool::decode(ProtocolTool::METADATA_REQUEST, substr($data, 4));
                    if (!isset($result['brokers'], $result['topics'])) {
                        $this->logger->error('Get metadata is fail, brokers or topics is null.');
                        System::sleep(2);
                        continue;
                    }
                    $this->broker->setData($result['topics'], $result['brokers']);
                    System::sleep($this->broker->getConfig()->getMetadataRefreshIntervalMs() / 1000);
                } catch (\Throwable $exception) {
                    $socket->close();
                    unset($socket);
                    $this->logger->error($exception->getMessage());
                    goto loop;
                }
            }
        });
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
