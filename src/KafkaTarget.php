<?php


namespace rabbit\kafka;


use rabbit\helper\ArrayHelper;
use rabbit\kafka\Producter\Client;
use rabbit\log\targets\AbstractTarget;

/**
 * Class KafkaTarget
 * @package rabbit\kafka
 */
class KafkaTarget extends AbstractTarget
{
    /** @var Client */
    private $client;

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
    public function __construct(Client $client)
    {
        $this->client = $client;
        $this->client->init();
    }

    /**
     * @param array $messages
     * @param bool $flush
     */
    public function export(array $messages, bool $flush = true): void
    {
        foreach ($messages as $module => $message) {
            foreach ($message as $value) {
                ArrayHelper::remove($value, '%c');
                $log = [
                    'appname' => $module,
                ];
                foreach ($value as $index => $value) {
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