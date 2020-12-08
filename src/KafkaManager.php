<?php

declare(strict_types=1);

namespace Rabbit\Kafka;

use longlang\phpkafka\Consumer\ConsumerConfig;
use longlang\phpkafka\Producer\Producer;
use longlang\phpkafka\Producer\ProducerConfig;
use Rabbit\Base\Helper\ArrayHelper;

class KafkaManager
{
    protected array $items = [];

    public function __construct(array $configs = [])
    {
        $this->add($configs);
    }

    public function add(array $configs): void
    {
        foreach ($configs as $name => $item) {
            if (!isset($this->items[$name])) {
                if (null !== $proConifg = ArrayHelper::getValue($item, 'producer')) {
                    $this->items[$name]['producer'] = new Producer(new ProducerConfig($proConifg));
                } else {
                    $this->items[$name]['producer'] = null;
                }

                if (null !== $conConifg = ArrayHelper::getValue($item, 'consumer')) {
                    $this->items[$name]['consumer'] = new ConsumerConfig($conConifg);
                } else {
                    $this->items[$name]['consumer'] = null;
                }
            }
        }
    }

    public function getProducer(string $name = 'default'): ?Producer
    {
        if (!isset($this->items[$name])) {
            return null;
        }
        return $this->items[$name]['producer'] ?? null;
    }

    public function getConsumerConfig(string $name = 'default'): ?ConsumerConfig
    {
        if (!isset($this->items[$name])) {
            return null;
        }
        return isset($this->items[$name]['consumer']) ? clone $this->items[$name]['consumer'] : null;
    }
}
