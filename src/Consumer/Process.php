<?php
declare(strict_types=1);

namespace rabbit\kafka\Consumer;

use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;
use rabbit\kafka\Broker;
use rabbit\kafka\Exception\ConnectionException;
use rabbit\kafka\Protocol;
use rabbit\kafka\Protocol\Protocol as ProtocolTool;
use function end;
use function in_array;
use function json_encode;
use function sprintf;
use function substr;
use function trim;

class Process
{
    use LoggerAwareTrait;
    /**
     * @var callable|null
     */
    protected $consumer;
    /**
     * @var string[][][]
     */
    protected $messages = [];
    /** @var Broker */
    private $broker;
    /** @var Assignment */
    private $assignment;
    /** @var State */
    private $state;

    public function __construct(
        Broker $broker,
        Assignment $assignment,
        State $state,
        LoggerInterface $logger
    ) {
        $this->broker = $broker;
        $this->assignment = $assignment;
        $this->state = $state;
        $this->logger = $logger;
    }

    public function init(): void
    {
        $config = $this->broker->getConfig();
        Protocol::init($config->getBrokerVersion(), $this->logger);
        $this->broker->setProcess(function (string $data, int $fd): void {
            $this->processRequest($data, $fd);
        });
        if ($this->logger) {
            $this->state->setLogger($this->logger);
        }
        $this->state->setCallback(
            [
                State::REQUEST_METADATA => function (): void {
                    $this->syncMeta();
                },
                State::REQUEST_GETGROUP => function (): void {
                    $this->getGroupBrokerId();
                },
                State::REQUEST_JOINGROUP => function (): void {
                    $this->joinGroup();
                },
                State::REQUEST_SYNCGROUP => function (): void {
                    $this->syncGroup();
                },
                State::REQUEST_HEARTGROUP => function (): void {
                    $this->heartbeat();
                },
                State::REQUEST_OFFSET => function (): array {
                    return $this->offset();
                },
                State::REQUEST_FETCH_OFFSET => function (): void {
                    $this->fetchOffset();
                },
                State::REQUEST_FETCH => function (): array {
                    return $this->fetch();
                },
                State::REQUEST_COMMIT_OFFSET => function (): void {
                    $this->commit();
                },
            ]
        );
        $this->state->init($this->broker->getConfig()->getMetadataRefreshIntervalMs());
    }

    public function start(callable $consumer): void
    {
        $this->consumer = $consumer;
        $this->init();
        $this->state->start();
    }

    public function stop(): void
    {
        $this->state->stop();
    }

    /**
     * @throws Exception
     */
    protected function processRequest(string $data, int $fd): void
    {
        $correlationId = ProtocolTool::unpack(ProtocolTool::BIT_B32, substr($data, 0, 4));

        switch ($correlationId) {
            case Protocol::METADATA_REQUEST:
                $result = Protocol::decode(Protocol::METADATA_REQUEST, substr($data, 4));

                if (!isset($result['brokers'], $result['topics'])) {
                    $this->logger->error('Get metadata is fail, brokers or topics is null.');
                    $this->state->failRun(State::REQUEST_METADATA);
                    break;
                }

                $isChange = $this->broker->setData($result['topics'], $result['brokers']);
                $this->state->succRun(State::REQUEST_METADATA, $isChange);

                break;
            case Protocol::GROUP_COORDINATOR_REQUEST:
                $result = Protocol::decode(Protocol::GROUP_COORDINATOR_REQUEST, substr($data, 4));

                if (!isset($result['errorCode'], $result['coordinatorId']) || $result['errorCode'] !== Protocol::NO_ERROR) {
                    $this->state->failRun(State::REQUEST_GETGROUP);
                    break;
                }

                $this->broker->setGroupBrokerId($result['coordinatorId']);

                $this->state->succRun(State::REQUEST_GETGROUP);

                break;
            case Protocol::JOIN_GROUP_REQUEST:
                $result = Protocol::decode(Protocol::JOIN_GROUP_REQUEST, substr($data, 4));
                if (isset($result['errorCode']) && $result['errorCode'] === Protocol::NO_ERROR) {
                    $this->succJoinGroup($result);
                    break;
                }

                $this->failJoinGroup($result['errorCode']);
                break;
            case Protocol::SYNC_GROUP_REQUEST:
                $result = Protocol::decode(Protocol::SYNC_GROUP_REQUEST, substr($data, 4));
                if (isset($result['errorCode']) && $result['errorCode'] === Protocol::NO_ERROR) {
                    $this->succSyncGroup($result);
                    break;
                }

                $this->failSyncGroup($result['errorCode']);
                break;
            case Protocol::HEART_BEAT_REQUEST:
                $result = Protocol::decode(Protocol::HEART_BEAT_REQUEST, substr($data, 4));
                if (isset($result['errorCode']) && $result['errorCode'] === Protocol::NO_ERROR) {
                    $this->state->succRun(State::REQUEST_HEARTGROUP);
                    break;
                }

                $this->failHeartbeat($result['errorCode']);
                break;
            case Protocol::OFFSET_REQUEST:
                $result = Protocol::decode(Protocol::OFFSET_REQUEST, substr($data, 4));
                $this->succOffset($result, $fd);
                break;
            case ProtocolTool::OFFSET_FETCH_REQUEST:
                $result = Protocol::decode(Protocol::OFFSET_FETCH_REQUEST, substr($data, 4));
                $this->succFetchOffset($result);
                break;
            case ProtocolTool::FETCH_REQUEST:
                $result = Protocol::decode(Protocol::FETCH_REQUEST, substr($data, 4));
                $this->succFetch($result, $fd);
                break;
            case ProtocolTool::OFFSET_COMMIT_REQUEST:
                $result = Protocol::decode(Protocol::OFFSET_COMMIT_REQUEST, substr($data, 4));
                $this->succCommit($result);
                break;
            default:
                $this->logger->error('Error request, correlationId:' . $correlationId);
        }
    }

    protected function syncMeta(): void
    {
        $this->logger->debug('Start sync metadata request');
        /** @var ConsumerConfig $config */
        $config = $this->broker->getConfig();

        $brokerHost = $config->getMetadataBrokerList();

        if (count($brokerHost) === 0) {
            throw new Exception('No valid broker configured');
        }

        shuffle($brokerHost);

        foreach ($brokerHost as $host) {
            $connect = $this->broker->getMetaConnect($host);

            if ($connect === null) {
                continue;
            }

            $params = $config->getTopics();
            $this->logger->debug('Start sync metadata request params:' . json_encode($params));
            $requestData = Protocol::encode(Protocol::METADATA_REQUEST, $params);
            $connect->write($requestData);
            
            return;
        }

        throw ConnectionException::fromBrokerList($brokerList);
    }

    protected function getGroupBrokerId(): void
    {
        $connect = $this->broker->getRandConnect();

        if ($connect === null) {
            return;
        }

        $config = $this->broker->getConfig();
        $params = ['group_id' => $config->getGroupId()];

        $requestData = Protocol::encode(Protocol::GROUP_COORDINATOR_REQUEST, $params);
        $connect->write($requestData);
        
    }

    protected function joinGroup(): void
    {
        $groupBrokerId = $this->broker->getGroupBrokerId();
        $connect = $this->broker->getMetaConnect((string)$groupBrokerId);

        if ($connect === null) {
            return;
        }

        $config = $this->broker->getConfig();
        $topics = $config->getTopics();
        $memberId = $this->assignment->getMemberId();

        $params = [
            'group_id' => $config->getGroupId(),
            'session_timeout' => $config->getSessionTimeout(),
            'rebalance_timeout' => $config->getRebalanceTimeout(),
            'member_id' => $memberId ?? '',
            'data' => [
                [
                    'protocol_name' => 'range',
                    'version' => 0,
                    'subscription' => $topics,
                    'user_data' => '',
                ],
            ],
        ];

        $requestData = Protocol::encode(Protocol::JOIN_GROUP_REQUEST, $params);
        $connect->write($requestData);
        
        $this->logger->debug('Join group start, params:' . json_encode($params));
    }

    public function failJoinGroup(int $errorCode): void
    {
        $memberId = $this->assignment->getMemberId();

        $this->logger->error(sprintf('Join group fail, need rejoin, errorCode %d, memberId: %s', $errorCode,
            $memberId));
        $this->stateConvert($errorCode);
    }

    /**
     * @param mixed[] $result
     */
    public function succJoinGroup(array $result): void
    {
        $this->state->succRun(State::REQUEST_JOINGROUP);
        $this->assignment->setMemberId($result['memberId']);
        $this->assignment->setGenerationId($result['generationId']);

        if ($result['leaderId'] === $result['memberId']) { // leader assign partition
            $this->assignment->assign($result['members'], $this->broker);
        }

        $this->logger->debug(sprintf('Join group sucess, params: %s', json_encode($result)));
    }

    public function syncGroup(): void
    {
        $groupBrokerId = $this->broker->getGroupBrokerId();
        $connect = $this->broker->getMetaConnect((string)$groupBrokerId);

        if ($connect === null) {
            return;
        }

        $memberId = $this->assignment->getMemberId();
        $generationId = $this->assignment->getGenerationId();

        $params = [
            'group_id' => $this->broker->getConfig()->getGroupId(),
            'generation_id' => $generationId ?? null,
            'member_id' => $memberId,
            'data' => $this->assignment->getAssignments(),
        ];

        $requestData = Protocol::encode(Protocol::SYNC_GROUP_REQUEST, $params);
        $this->logger->debug('Sync group start, params:' . json_encode($params));

        $connect->write($requestData);
        
    }

    public function failSyncGroup(int $errorCode): void
    {
        $this->logger->error(sprintf('Sync group fail, need rejoin, errorCode %d', $errorCode));
        $this->stateConvert($errorCode);
    }

    /**
     * @param mixed[][] $result
     */
    public function succSyncGroup(array $result): void
    {
        $this->logger->debug(sprintf('Sync group sucess, params: %s', json_encode($result)));
        $this->state->succRun(State::REQUEST_SYNCGROUP);

        $topics = $this->broker->getTopics();

        $brokerToTopics = [];

        foreach ($result['partitionAssignments'] as $topic) {
            foreach ($topic['partitions'] as $partId) {
                $brokerId = $topics[$topic['topicName']][$partId];

                $brokerToTopics[$brokerId] = $brokerToTopics[$brokerId] ?? [];

                $topicInfo = $brokerToTopics[$brokerId][$topic['topicName']] ?? [];

                $topicInfo['topic_name'] = $topic['topicName'];

                $topicInfo['partitions'] = $topicInfo['partitions'] ?? [];
                $topicInfo['partitions'][] = $partId;

                $brokerToTopics[$brokerId][$topic['topicName']] = $topicInfo;
            }
        }

        $this->assignment->setTopics($brokerToTopics);
    }

    protected function heartbeat(): void
    {
        $groupBrokerId = $this->broker->getGroupBrokerId();
        $connect = $this->broker->getMetaConnect((string)$groupBrokerId);

        if ($connect === null) {
            return;
        }

        $memberId = $this->assignment->getMemberId();

        if (trim($memberId) === '') {
            return;
        }

        $generationId = $this->assignment->getGenerationId();

        $params = [
            'group_id' => $this->broker->getConfig()->getGroupId(),
            'generation_id' => $generationId,
            'member_id' => $memberId,
        ];

        $requestData = Protocol::encode(Protocol::HEART_BEAT_REQUEST, $params);
        $connect->write($requestData);
        
    }

    public function failHeartbeat(int $errorCode): void
    {
        $this->logger->error('Heartbeat error, errorCode:' . $errorCode);
        $this->stateConvert($errorCode);
    }

    /**
     * @return int[]
     */
    protected function offset()
    {
        $context = [];
        $topics = $this->assignment->getTopics();

        foreach ($topics as $brokerId => $topicList) {
            $connect = $this->broker->getMetaConnect((string)$brokerId);

            if ($connect === null) {
                return [];
            }

            $data = [];
            foreach ($topicList as $topic) {
                $item = [
                    'topic_name' => $topic['topic_name'],
                    'partitions' => [],
                ];

                foreach ($topic['partitions'] as $partId) {
                    $item['partitions'][] = [
                        'partition_id' => $partId,
                        'offset' => 1,
                        'time' => -1,
                    ];
                    $data[] = $item;
                }
            }

            $params = [
                'replica_id' => -1,
                'data' => $data,
            ];

            $requestData = Protocol::encode(Protocol::OFFSET_REQUEST, $params);

            $connect->write($requestData);
            $context[] = (int)$connect->getSocket();
        }

        return $context;
    }

    /**
     * @param mixed[][] $result
     */
    public function succOffset(array $result, int $fd): void
    {
        $offsets = $this->assignment->getOffsets();
        $lastOffsets = $this->assignment->getLastOffsets();

        foreach ($result as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] !== Protocol::NO_ERROR) {
                    $this->stateConvert($part['errorCode']);
                    break 2;
                }

                $offsets[$topic['topicName']][$part['partition']] = end($part['offsets']);
                $lastOffsets[$topic['topicName']][$part['partition']] = $part['offsets'][0];
            }
        }

        $this->assignment->setOffsets($offsets);
        $this->assignment->setLastOffsets($lastOffsets);
        $this->state->succRun(State::REQUEST_OFFSET, $fd);
    }

    protected function fetchOffset(): void
    {
        $groupBrokerId = $this->broker->getGroupBrokerId();
        $connect = $this->broker->getMetaConnect((string)$groupBrokerId);

        if ($connect === null) {
            return;
        }

        $topics = $this->assignment->getTopics();
        $data = [];

        foreach ($topics as $brokerId => $topicList) {
            foreach ($topicList as $topic) {
                $partitions = [];

                if (isset($data[$topic['topic_name']]['partitions'])) {
                    $partitions = $data[$topic['topic_name']]['partitions'];
                }

                foreach ($topic['partitions'] as $partId) {
                    $partitions[] = $partId;
                }

                $data[$topic['topic_name']]['partitions'] = $partitions;
                $data[$topic['topic_name']]['topic_name'] = $topic['topic_name'];
            }
        }

        $params = [
            'group_id' => $this->broker->getConfig()->getGroupId(),
            'data' => $data,
        ];

        $requestData = Protocol::encode(Protocol::OFFSET_FETCH_REQUEST, $params);
        $connect->write($requestData);
        
    }

    /**
     * @param mixed[] $result
     */
    public function succFetchOffset(array $result): void
    {
        $msg = sprintf('Get current fetch offset sucess, result: %s', json_encode($result));
        $this->logger->debug($msg);
        $offsets = $this->assignment->getFetchOffsets();

        foreach ($result as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] !== 0) {
                    $this->stateConvert($part['errorCode']);
                    break 2;
                }

                $offsets[$topic['topicName']][$part['partition']] = $part['offset'];
            }
        }

        $this->assignment->setFetchOffsets($offsets);

        $consumerOffsets = $this->assignment->getConsumerOffsets();
        $lastOffsets = $this->assignment->getLastOffsets();

        if (empty($consumerOffsets)) {
            $consumerOffsets = $this->assignment->getFetchOffsets();

            foreach ($consumerOffsets as $topic => $value) {
                foreach ($value as $partId => $offset) {
                    if (isset($lastOffsets[$topic][$partId]) && $lastOffsets[$topic][$partId] > $offset) {
                        $consumerOffsets[$topic][$partId] = $offset;
                    }
                }
            }

            $this->assignment->setConsumerOffsets($consumerOffsets);
            $this->assignment->setCommitOffsets($this->assignment->getFetchOffsets());
        }
        $this->state->succRun(State::REQUEST_FETCH_OFFSET);
    }

    /**
     * @return int[]
     */
    protected function fetch()
    {
        $this->messages = [];
        $context = [];
        $topics = $this->assignment->getTopics();
        $consumerOffsets = $this->assignment->getConsumerOffsets();

        foreach ($topics as $brokerId => $topicList) {
            $connect = $this->broker->getDataConnect((string)$brokerId);

            if ($connect === null) {
                return [];
            }

            $data = [];

            foreach ($topicList as $topic) {
                $item = [
                    'topic_name' => $topic['topic_name'],
                    'partitions' => [],
                ];

                foreach ($topic['partitions'] as $partId) {
                    $item['partitions'][] = [
                        'partition_id' => $partId,
                        'offset' => isset($consumerOffsets[$topic['topic_name']][$partId]) ? $consumerOffsets[$topic['topic_name']][$partId] : 0,
                        'max_bytes' => $this->broker->getConfig()->getMaxBytes(),
                    ];
                }

                $data[] = $item;
            }

            $params = [
                'max_wait_time' => $this->broker->getConfig()->getMaxWaitTime(),
                'replica_id' => -1,
                'min_bytes' => '1000',
                'data' => $data,
            ];

            $this->logger->debug('Fetch message start, params:' . json_encode($params));
            $requestData = Protocol::encode(Protocol::FETCH_REQUEST, $params);
            $connect->write($requestData);
            $context[] = (int)$connect->getSocket();
        }

        return $context;
    }

    /**
     * @param mixed[][][] $result
     */
    public function succFetch(array $result, int $fd): void
    {
        $this->logger->debug('Fetch success, result:' . json_encode($result));
        foreach ($result['topics'] as $topic) {
            foreach ($topic['partitions'] as $part) {
                $context = [
                    $topic['topicName'],
                    $part['partition'],
                ];

                if ($part['errorCode'] !== 0) {
                    $this->stateConvert($part['errorCode'], $context);
                    continue;
                }

                $offset = $this->assignment->getConsumerOffset($topic['topicName'], $part['partition']);

                if ($offset === null) {
                    return; // current is rejoin....
                }

                foreach ($part['messages'] as $message) {
                    $this->messages[$topic['topicName']][$part['partition']][] = $message;

                    $offset = $message['offset'];
                }

                $consumerOffset = ($part['highwaterMarkOffset'] > $offset) ? ($offset + 1) : $offset;
                $this->assignment->setConsumerOffset($topic['topicName'], $part['partition'], $consumerOffset);
                $this->assignment->setCommitOffset($topic['topicName'], $part['partition'], $offset);
            }
        }
        $this->state->succRun(State::REQUEST_FETCH, $fd);
    }

    protected function consumeMessage(): void
    {
        foreach ($this->messages as $topic => $value) {
            foreach ($value as $partition => $messages) {
                foreach ($messages as $message) {
                    if ($this->consumer !== null) {
                        ($this->consumer)($topic, $partition, $message);
                    }
                }
            }
        }
        unset($this->messages);
        $this->messages = [];
    }

    protected function commit(): void
    {
        $config = $this->broker->getConfig();

        if ($config->getConsumeMode() === ConsumerConfig::CONSUME_BEFORE_COMMIT_OFFSET) {
            $this->consumeMessage();
        }

        $groupBrokerId = $this->broker->getGroupBrokerId();
        $connect = $this->broker->getMetaConnect((string)$groupBrokerId);

        if ($connect === null) {
            return;
        }

        $commitOffsets = $this->assignment->getCommitOffsets();
        $topics = $this->assignment->getTopics();
        $this->assignment->setPreCommitOffsets($commitOffsets);
        $data = [];

        foreach ($topics as $brokerId => $topicList) {
            foreach ($topicList as $topic) {
                $partitions = [];

                if (isset($data[$topic['topic_name']]['partitions'])) {
                    $partitions = $data[$topic['topic_name']]['partitions'];
                }

                foreach ($topic['partitions'] as $partId) {
                    if ($commitOffsets[$topic['topic_name']][$partId] === -1) {
                        continue;
                    }

                    $partitions[$partId]['partition'] = $partId;
                    $partitions[$partId]['offset'] = $commitOffsets[$topic['topic_name']][$partId];
                }

                $data[$topic['topic_name']]['partitions'] = $partitions;
                $data[$topic['topic_name']]['topic_name'] = $topic['topic_name'];
            }
        }

        $params = [
            'group_id' => $config->getGroupId(),
            'generation_id' => $this->assignment->getGenerationId(),
            'member_id' => $this->assignment->getMemberId(),
            'data' => $data,
        ];

        $this->logger->debug('Commit current fetch offset start, params:' . json_encode($params));
        $requestData = Protocol::encode(Protocol::OFFSET_COMMIT_REQUEST, $params);
        $connect->write($requestData);
        
    }

    /**
     * @param mixed[][] $result
     */
    public function succCommit(array $result): void
    {
        $this->logger->debug('Commit success, result:' . json_encode($result));
        $this->state->succRun(State::REQUEST_COMMIT_OFFSET);
        foreach ($result as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] !== 0) {
                    $this->stateConvert($part['errorCode']);
                    return;  // not call user consumer function
                }
            }
        }

        if ($this->broker->getConfig()->getConsumeMode() === ConsumerConfig::CONSUME_AFTER_COMMIT_OFFSET) {
            $this->consumeMessage();
        }
    }

    /**
     * @param string[] $context
     */
    protected function stateConvert(int $errorCode, ?array $context = null): bool
    {
        $this->logger->error(Protocol::getError($errorCode));

        $recoverCodes = [
            Protocol::UNKNOWN_TOPIC_OR_PARTITION,
            Protocol::NOT_LEADER_FOR_PARTITION,
            Protocol::BROKER_NOT_AVAILABLE,
            Protocol::GROUP_LOAD_IN_PROGRESS,
            Protocol::GROUP_COORDINATOR_NOT_AVAILABLE,
            Protocol::NOT_COORDINATOR_FOR_GROUP,
            Protocol::INVALID_TOPIC,
            Protocol::INCONSISTENT_GROUP_PROTOCOL,
            Protocol::INVALID_GROUP_ID,
        ];

        $rejoinCodes = [
            Protocol::ILLEGAL_GENERATION,
            Protocol::INVALID_SESSION_TIMEOUT,
            Protocol::REBALANCE_IN_PROGRESS,
            Protocol::UNKNOWN_MEMBER_ID,
        ];

        if (in_array($errorCode, $recoverCodes, true)) {
            $this->state->recover();
            $this->assignment->clearOffset();
            return false;
        }

        if (in_array($errorCode, $rejoinCodes, true)) {
            if ($errorCode === Protocol::UNKNOWN_MEMBER_ID) {
                $this->assignment->setMemberId('');
            }

            $this->assignment->clearOffset();
            $this->state->rejoin();
            return false;
        }

        if ($errorCode === Protocol::OFFSET_OUT_OF_RANGE) {
            $resetOffset = $this->broker->getConfig()->getOffsetReset();
            $offsets = $resetOffset === 'latest' ? $assign->getLastOffsets() : $assign->getOffsets();

            [$topic, $partId] = $context;

            if (isset($offsets[$topic][$partId])) {
                $this->assignment->setConsumerOffset($topic, (int)$partId, $offsets[$topic][$partId]);
            }
        }

        return true;
    }
}
