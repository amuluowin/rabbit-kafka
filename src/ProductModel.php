<?php
declare(strict_types=1);

namespace rabbit\kafka;

use rabbit\kafka\Producter\Producter;

/**
 * Class ProductModel
 * @package rabbit\kafka
 */
class ProductModel extends Model
{
    /** @var Producter */
    protected $producter;

    /**
     * @throws Exception
     * @throws Exception\Protocol
     */
    public function save(bool $validated = true): void
    {
        $validated && $this->validate();
        $this->producter->send([
            [
                'topic' => $this->topic,
                'value' => $this->toJsonString(),
                'key' => $this->key
            ]
        ]);
    }
}