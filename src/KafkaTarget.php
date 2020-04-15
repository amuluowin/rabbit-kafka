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
    private $client;
    /** @var array */
    private $template = [
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

    private $topic = 'seaslog';

    /**
     * KafkaTarget constructor.
     * @param Client $client
     */
    public function __construct(Producter $client)
    {
        $this->client = $client;
    }


    /**
     * @param array $messages
     * @param bool $flush
     */
    public function export(array $messages, bool $flush = true): void
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
                        case "timespan":
                            $log[$name] = strtotime(explode('.', $value)[0]);
                            break;
                        case "int":
                            $log[$name] = (int)$value;
                            break;
                        default:
                            $log[$name] = trim($value);
                    }
                }
                $this->client->send([
                    [
                        'topic' => $this->topic,
                        'value' => json_encode($log),
                        'key' => ''
                    ]
                ]);
            }
        }
    }
}
