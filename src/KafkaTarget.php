<?php

declare(strict_types=1);

namespace Rabbit\Kafka;

use longlang\phpkafka\Producer\Producer;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\StringHelper;
use Rabbit\Log\Targets\AbstractTarget;

class KafkaTarget extends AbstractTarget
{
    protected Producer $client;
    /** @var array */
    protected $template = [
        ['datetime', 'timespan'],
        ['level', 'string'],
        ['request_uri', 'string'],
        ['request_method', 'string'],
        ['clientip', 'string'],
        ['requestid', 'string'],
        ['filename', 'string'],
        ['memoryusage', 'int'],
        ['message', 'string']
    ];

    protected $topic = 'seaslog';

    /**
     * KafkaTarget constructor.
     * @param Client $client
     */
    public function __construct(Producer $client)
    {
        parent::__construct();
        $this->client = $client;
    }

    /**
     * @param array $messages
     * @throws Exception
     * @throws Exception\Protocol
     */
    public function export(array $messages): void
    {
        foreach ($messages as $module => $message) {
            foreach ($message as $msg) {
                if (is_string($msg)) {
                    switch (ini_get('seaslog.appender')) {
                        case '2':
                        case '3':
                            $msg = trim(substr($msg, StringHelper::str_n_pos($msg, ' ', 6)));
                            break;
                        case '1':
                        default:
                            $fileName = basename($module);
                            $module = substr($fileName, 0, strrpos($fileName, '_'));
                    }
                    $msg = explode($this->split, trim($msg));
                } else {
                    ArrayHelper::remove($msg, '%c');
                }
                if (!empty($this->levelList) && !in_array($msg[$this->levelIndex], $this->levelList)) {
                    continue;
                }
                $log = [
                    'appname' => $module,
                ];
                foreach ($msg as $index => $value) {
                    [$name, $type] = $this->template[$index];
                    switch ($type) {
                        case "string":
                            $log[$name] = trim($value);
                            break;
                        case "int":
                            $log[$name] = (int)$value;
                            break;
                        default:
                            $log[$name] = trim($value);
                    }
                }
                $this->channel->push(json_encode($log));
            }
        }
    }

    /**
     * @throws Exception
     * @throws Exception\Protocol
     */
    protected function write(): void
    {
        loop(function () {
            $logs = $this->getLogs();
            !empty($logs) && $this->client->send($this->topic, implode(',', $logs));
        });
    }
}
