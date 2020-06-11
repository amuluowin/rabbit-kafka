<?php


namespace rabbit\kafka;

use rabbit\helper\ArrayHelper;
use rabbit\helper\StringHelper;
use rabbit\kafka\Producter\Producter;
use rabbit\log\targets\AbstractTarget;

/**
 * Class KafkaTarget
 * @package rabbit\kafka
 */
class KafkaTarget extends AbstractTarget
{
    /** @var Client */
    protected $client;
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
    public function __construct(Producter $client)
    {
        parent::__construct();
        $this->client = $client;
    }

    public function init()
    {
        parent::init();
        $this->client->init();
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
        goloop(function () {
            $logs = $this->getLogs();
            !empty($logs) && $this->client->send([
                [
                    'topic' => $this->topic,
                    'value' => implode(',', $logs),
                    'key' => ''
                ]
            ]);
        });
    }
}
